import subprocess
import requests
import time
from os import listdir
from os.path import isfile, join, getsize
from random import randint
from apscheduler.schedulers.blocking import BlockingScheduler

LIVE = True


def send_msg(text):
    token = "5571325320:AAHoEgORTDe4suB2ZsrlRoaYQAZE0-wR-Bc"
    chat_id = "1560985952"
    url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text
    results = requests.get(url_req)
    print(results.json())


if LIVE:
    bind_dir = "/var/log/bind"
    dest_dir = "/net/data/dns-ttl/dnssec/"
    rsa_loc = "/root/pharah_rsa"
    bash_cmd = "logrotate -f /root/bind"


def concat_str(lst):
    init_str = ""
    for e in lst:
        init_str = init_str + " " + e.split("/")[-1]
    return init_str


def execute_cmd(command):
    print("command, ", command)
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return output, error


def bind_transfer_v1(ind, container_id):
    print(ind, container_id)
    execute_cmd("docker exec -i " + container_id + " apt-get install openssh-client -y")


def bind_transfer(ind, container_id):
    print("Starting bind transfer")

    out, err = execute_cmd("docker exec -i " + container_id + " ls " + bind_dir)
    if not err:
        files_before_log_rotate = out.decode().split('\n')[:-1]
        print("before", files_before_log_rotate)
    else:
        files_before_log_rotate = []
    # files_before_log_rotate = [join(bind_dir, f) for f in listdir(bind_dir) if isfile(join(bind_dir, f))]
    print("Files before:" + concat_str(files_before_log_rotate))

    if LIVE:
        s = "docker exec -i " + container_id + " "
        execute_cmd(s + bash_cmd)

    out, err = execute_cmd("docker exec -i " + container_id + " ls " + bind_dir)
    if not err:
        files_after_log_rotate = out.decode().split('\n')[:-1]
        print("After", files_after_log_rotate)
    else:
        files_after_log_rotate = []
    # files_after_log_rotate = [join(bind_dir, f) for f in listdir(bind_dir) if isfile(join(bind_dir, f))]
    print("Files after logrotate:" + concat_str(files_after_log_rotate))

    files_to_transfer = []
    for file in files_after_log_rotate:
        if not file.endswith("query.log"):
            files_to_transfer.append(file)

    for file in files_to_transfer:
        # file_size_in_mb = getsize(file) / 1000000
        file_name = "query.log.{}{}".format(int(time.time()), randint(100, 999))

        cmd = "docker exec -i " + container_id + " mv {} {}".format(bind_dir + '/' + file, "{}/{}".format(bind_dir, file_name))
        execute_cmd(cmd)
        msg_str = "moved {} to {}, size".format(file.split("/")[-1], file_name)
        print(msg_str)
        cmd = "docker exec -i " + container_id + " scp -i {} -r -P 2222 {} ashiq@pharah.cs.vt.edu:{}".format(rsa_loc,
                                                                                                          "{}/{}".format(
                                                                                                              bind_dir,
                                                                                                              file_name),
                                                                                                          dest_dir + ind + '/')
        ans = execute_cmd(cmd)
        if ans[1] is None:
            cmd = "docker exec -i " + container_id + " rm {}".format("{}/{}".format(bind_dir, file_name))
            execute_cmd(cmd)

    out, err = execute_cmd("docker exec -i " + container_id + " ls " + bind_dir)
    if not err:
        files_at_end = out.decode().split('\n')[:-1]
        print("After sending", files_at_end)
    else:
        files_at_end = []
    # files_at_end = [join(bind_dir, f) for f in listdir(bind_dir) if isfile(join(bind_dir, f))]
    print("Files after sending:" + concat_str(files_at_end))


scheduler = BlockingScheduler()
containers = ["668a22e2de4e", "6f7e04631710", "306c42b372c2", "abcda5d14762", "9cebd7c983d9",
              "1147a7f801fc", "b9f78b9084b4", "0494d7089c3a", "e4e70b62ffed", "5e69afc16b5d"]
for ind, container in enumerate(containers):
    scheduler.add_job(bind_transfer, args=[str(ind + 1), container], trigger='interval', minutes=30)
scheduler.start()
