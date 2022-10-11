import datetime
import json
import os
import traceback
from collections import defaultdict
import time
from pyspark.sql.context import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.accumulators import AccumulatorParam
from auto_profiler import Profiler
from multiprocessing.dummy import Pool as Threadpool


def _get_leaf_files(path):
    import os
    list_of_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            list_of_files.append(os.path.join(root, file))
    return list_of_files


def _parse_dns_logs(files):
    ans_dict = defaultdict(lambda: dict())
    tot_files = len(files)
    index = 0
    for file in files:
        index += 1
        try:
            with open(file) as FileObj:
                for line in FileObj:
                    try:
                        if 'good proper' not in line:
                            continue
                        if 'dnssec60' not in line:
                            continue

                        segments = line.strip().split()
                        resolver_ip, ts, mode, webserver_ip, dns_server_inside_ip, qn, qt, tc_bit, \
                        ednsflag = segments[2], segments[3], segments[4], segments[5], segments[6], \
                                   segments[7], segments[8], segments[10], segments[12]
                        protocol = segments[13] if len(segments) == 14 else 'UDP'
                        uid, exp_id = qn.split('.')[0], qn.split('.')[1].split('_')[2]
                        if not exp_id:
                            continue

                        d = ans_dict[exp_id]
                        if "requests" not in d:
                            d["requests"] = {}

                        datetime_object = datetime.datetime.fromtimestamp(float(ts))
                        meta = {"date": datetime_object, "qn": qn, "resolver_ip": resolver_ip, "qt": qt,
                                "mode": mode, "webserver_ip": webserver_ip, "tc": False if tc_bit == 0 else True,
                                "do": False if ednsflag != "32768" else True, "protocol": protocol,
                                "dns_server_inside_ip": dns_server_inside_ip}
                        if uid not in d["requests"]:
                            d['requests'][uid] = list()
                        d['requests'][uid].append(meta)
                    except Exception as e:
                        traceback.print_exc()
                        print('Exception in parsing ', e)
                        continue
        except Exception as e:
            traceback.print_exc()
            print('Exception in file reading', e)
            continue

        print("*** Done with parsing DNS log file {} {}/{}".format(file, index, tot_files))

    return ans_dict


def _parse_http_logs(files):
    ans_dict = defaultdict(lambda: dict())
    tot_files = len(files)
    index = 0
    for f in files:
        exp_id = f.split('/')[-1][: - len("-out.json")].split('_')[2]
        index += 1
        try:
            with open(f) as fl:
                x = json.load(fl)
                data = x['dict_of_phases']
                for key in data:
                    try:
                        temp = data[key]
                        req_url = temp['req_url'][7:]
                        uid = str(req_url.split(".")[0])
                        phase_1, server_time_1, asn, phase_2, server_time_2, phase_2_status, ip_hash = temp.get(
                            '1-response'), temp.get('1-time'), temp.get('asn'), temp.get('2-response'), temp.get(
                            '2-time'), temp.get('host-phase-2'), temp.get('ip_hash')
                        if not phase_1 or 'phase' not in phase_1:
                            continue
                        if not phase_2 or 'phase' not in phase_2:
                            if not phase_2_status:
                                continue
                            elif phase_2_status == "err":
                                if temp.get("errmsg") == "Proxy Error: No peers with requested IP available":
                                    continue
                                elif temp.get("errmsg") == "Proxy Error: Failed to establish connection with peer":
                                    continue
                                elif temp.get("errmsg") == "unknown":
                                    continue
                                elif temp.get("errmsg") == "Invalid Auth":
                                    continue
                                elif temp.get("errmsg") == "Proxy Error: socket hang up":
                                    continue
                                else:
                                    phase_2 = "ServFail"  # possibly for DNSSEC failure
                            else:
                                continue
                        if uid not in ans_dict[exp_id]:
                            ans_dict[exp_id][uid] = {}
                        ans_dict[exp_id][uid] = {"phase1": phase_1, "phase2": phase_2, "phase1_time": server_time_1,
                                                 "phase2_time": server_time_2, "exit_node_asn": asn, "ip_hash": ip_hash,
                                                 "batch_phase1_start": x['telemetry']['1']['start'] / 1000,
                                                 "batch_phase2_start": x['telemetry']['2']['start'] / 1000,
                                                 "batch_phase1_end": x['telemetry']['1']['end'] / 1000,
                                                 "batch_phase2_end": x['telemetry']['2']['end'] / 1000}
                    except Exception as e:
                        traceback.print_exc()
                        print('Exception in parsing', e)
                        continue
        except Exception as e:
            traceback.print_exc()
            print('Exception in file reading', e)
            continue
        print("*** Done with parsing HTTP log file {} {}/{}".format(f, index, tot_files))
    return ans_dict


def _segment(lst, d1, d2):
    ans = []
    for e in lst:
        # print(d1, d2, e['date'].timestamp())
        if d1 <= e['date'].timestamp() <= d2:
            ans.append(e)
    return ans


def _identify_actual_resolver_ip(phase_resp, dns_queries, which=1):
    resolvers = {}
    actual_webserver_ip = None
    l = [i for i in range(1, 11)] + ["x"]
    for i in l:
        if 'phase' + str(i) in phase_resp:
            actual_webserver_ip = index_to_ip[i]

    for query in dns_queries:
        if resolver_to_asn.get(query['resolver_ip']) in lum_resolvers_asn:
            continue
        if not query['do']:
            continue
        if query['resolver_ip'] not in validating_resolvers:
            continue
        resolvers[query['resolver_ip']] = query['webserver_ip']
    if which == 1:
        actual_resolver_ip = None
        for ip in resolvers:
            if actual_webserver_ip == resolvers[ip]:
                actual_resolver_ip = ip
                break
        return actual_resolver_ip
    else:
        actual_resolver_ips = []
        for ip in resolvers:
            if actual_webserver_ip == resolvers[ip]:
                actual_resolver_ips.append(ip)
        return actual_resolver_ips


def _parse_logs(expt_id):
    global result
    dns_logs = dns_info[expt_id]
    http_logs = http_info[expt_id]
    try:
        # for uid in dns_logs['requests']:
        #     dns_logs['requests'][uid].sort(key=lambda item: item['date'])

        d = defaultdict(list)

        for uid in http_logs:
            dns_info_curated_first_v2 = []
            dns_info_curated_second_v2 = []
            dns_info_curated_first = _segment(dns_logs['requests'][uid], http_logs[uid]["batch_phase1_start"],
                                              http_logs[uid]["batch_phase1_end"])
            dns_info_curated_second = _segment(dns_logs['requests'][uid], http_logs[uid]["batch_phase2_start"],
                                               http_logs[uid]["batch_phase2_end"])
            time1, time2 = http_logs[uid]['phase1_time'], http_logs[uid]['phase2_time']
            if time1:
                time1_range = (time1 / 1000 - 4, time1 / 1000 + 4)
                for query in dns_info_curated_first:
                    if time1_range[0] <= query['date'].timestamp() <= time1_range[1]:
                        dns_info_curated_first_v2.append(query)
            if time2:
                time2_range = (time2 / 1000 - 4, time2 / 1000 + 4)
                for query in dns_info_curated_second:
                    if time2_range[0] <= query['date'].timestamp() <= time2_range[1]:
                        dns_info_curated_second_v2.append(query)
                    # print('dns_info_curated', len(dns_info_curated_second_v2))

            phase1_resp, phase2_resp = http_logs[uid]['phase1'], http_logs[uid]['phase2']

            # actual resolver identification
            actual_resolver_ip_phase1 = _identify_actual_resolver_ip(phase1_resp, dns_info_curated_first_v2)
            actual_resolver_ips_phase2 = _identify_actual_resolver_ip(phase2_resp, dns_info_curated_second_v2, which=2)

            if phase1_resp and actual_resolver_ip_phase1:
                # case 1: serving from cache, resolver from case 1 won't appear and both phase will have same responses
                if phase2_resp is not None and phase1_resp == phase2_resp and \
                        actual_resolver_ip_phase1 not in actual_resolver_ips_phase2:
                    d[actual_resolver_ip_phase1].append({
                        "case": 1,
                        "exit_node_asn": http_logs[uid]["exit_node_asn"],
                        "exit_node_ip_hash": http_logs[uid]["ip_hash"],
                        "uid": uid,
                        "exp_id": expt_id
                    })
                # case 2: new pull, resolver from case 1 would appear again and phasex will be returned by the apache
                # server in phase 2
                elif phase2_resp is not None and phase1_resp != phase2_resp and 'phasex' in phase2_resp and \
                        actual_resolver_ip_phase1 in actual_resolver_ips_phase2:
                    d[actual_resolver_ip_phase1].append({
                        "case": 2,
                        "exit_node_asn": http_logs[uid]["exit_node_asn"],
                        "exit_node_ip_hash": http_logs[uid]["ip_hash"],
                        "uid": uid,
                        "exp_id": expt_id
                    })
                # case 3: servfail in phase 2, resolver from case 1 won't appear and phase 2 will have no response
                elif phase1_resp != phase2_resp and phase2_resp == "ServFail" and \
                        actual_resolver_ip_phase1 not in actual_resolver_ips_phase2:
                    d[actual_resolver_ip_phase1].append({
                        "case": 3,
                        "exit_node_asn": http_logs[uid]["exit_node_asn"],
                        "exit_node_ip_hash": http_logs[uid]["ip_hash"],
                        "uid": uid,
                        "exp_id": expt_id
                    })
        for ip in d:
            result[ip] += d[ip]
        return d
    except Exception as e:
        print(expt_id, dns_logs)
        traceback.print_exc()
        return defaultdict(lambda v: list())


def _write_to_file(fn, l):
    with open('Outer_updates/temp/' + fn, 'w') as f:
        for ip in l:
            if ip:
                f.write(ip + '\n')


def json_dump(d, fn):
    json.dump(d, open(fn, 'w'), default=str, indent=4)


def master():
    print(len(dns_info.keys()))
    print(len(http_info.keys()))

    pool = Threadpool(50)
    results = pool.map(_parse_logs, exp_id_list)
    pool.close()
    pool.join()
    if live:
        json_dump(result, 'Outer_updates/temp/new_ttl_dnssec_expt_result.json')
    else:
        json_dump(result, 'result/new_ttl_dnssec_expt_result.json')

    # exp_id = "1664901002"
    # _parse_logs(expt_id=exp_id, dns_logs=dns_info[exp_id], http_logs=http_info[exp_id])


class DictParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, value1, value2):
        ip = value2[0]
        print("value2", value2, value1[ip])
        value1[ip] += value2[1]
        return value1


def master_with_spark():
    def _add_case(v):
        global result
        for ip in v:
            result += (ip, v[ip])

    sc.parallelize(exp_id_list).map(lambda v: _parse_logs(expt_id=v)).foreach(_add_case)


def quick_analyze():
    result = json.load(open('Outer_updates/temp/new_ttl_dnssec_expt_result.json'))
    summary = defaultdict(dict)
    for ip in result:
        items = result[ip]
        case1, case2, case3 = 0, 0, 0
        for item in items:
            if item['case'] == 1:
                case1 += 1
            elif item['case'] == 2:
                case2 += 1
            elif item['case'] == 3:
                case3 += 1
        total = case1 + case2 + case3
        summary[ip] = {"case1": case1 / total, "case2": case2 / total, "case3": case3 / total, "total": total}
    json_dump(summary, 'Outer_updates/temp/new_ttl_dnssec_summary.json')
    cnt_100 = 0
    cnt_90 = 0
    for i in summary:
        if summary[i]['case1'] == 1.0 or summary[i]['case2'] == 1.0 or summary[i]['case3'] == 1.0:
            cnt_100 += 1
        elif summary[i]['case1'] >= 0.9 or summary[i]['case2'] >= 0.9 or summary[i]['case3'] >= 0.9:
            cnt_90 += 1
    print(cnt_100, cnt_90)
    c1, c2, c3 = 0, 0, 0
    for i in summary:
        if summary[i]['case1'] == 1.0:
            c1 += 1
        elif summary[i]['case2'] == 1.0:
            c2 += 1
        elif summary[i]['case3'] == 1.0:
            c3 += 1
    print(len(summary), c1, c2, c3)


if __name__ == "__main__":
    start = time.time()
    ip_to_index = {
        "3.223.194.233": 7,
        "34.226.99.56": 5,
        "52.44.221.99": 8,
        "52.71.44.50": 6,
        "18.207.47.246": 2,
        "3.208.196.0": 3,
        "44.195.175.12": 4,
        "50.16.6.90": 1,
        "3.220.52.113": "x"
    }
    index_to_ip = dict((v, k) for k, v in ip_to_index.items())

    live = False
    if live:
        dns_logs_dir = '/net/data/dns-ttl/dnssec_ttl_new/bind/'
        http_logs_dir = '/home/protick/node_code/results_new_exp_dnssec_v2/'
    else:
        dns_logs_dir = '/Users/ashiq/PycharmProjects/dnssec_ttl/analysis/dns_logs/'
        http_logs_dir = '/Users/ashiq/PycharmProjects/dnssec_ttl/analysis/http_logs/'

    resolver_to_asn = json.load(open('Outer_updates/temp/resolver-to-asn.json'))
    lum_resolvers_asn = [15169, 20473, 36692, 14061, 30607, 24940, 27725]
    validating_resolvers = json.load(open('/home/protick/ocsp_dns_tools/validating_resolvers.json'))

    dns_files = [dns_logs_dir + f for f in os.listdir(dns_logs_dir)
                 if os.path.isfile(os.path.join(dns_logs_dir, f))]
    dns_info = _parse_dns_logs(files=dns_files)
    # print(dns_info['1664901002']['requests'])
    # print(dns_info['1664901002']['requests']['9d71475e-f682-48f1-bd88-fa19eb2dc7f31664901546270'])

    http_files = _get_leaf_files(http_logs_dir)
    http_info = _parse_http_logs(files=http_files)
    # print(http_info['1664901002'])
    # print(http_info['1664901002']['9d71475e-f682-48f1-bd88-fa19eb2dc7f31664901546270'])

    exp_id_list = set()
    for file in http_files:
        exp_id_list.add(file.split('/')[-1][: - len("-out.json")].split('_')[2])
    print(len(exp_id_list))

    # conf = SparkConf() \
    #     .setAppName("spf-exploit-spark") \
    #     # .setMaster("local[*]")
    #
    # sc = SparkContext(conf=conf)
    #
    # sqlContext = SQLContext(sc)
    # sc.setLogLevel("ERROR")

    # result = sc.accumulator(defaultdict(list), DictParam())
    # master_with_spark()
    # print("result", result.value)
    # json_dump(result.value, 'result/new_ttl_dnssec_expt_result.json')
    result = defaultdict(list)
    master()
    quick_analyze()
    end = time.time()
    print(end - start)
