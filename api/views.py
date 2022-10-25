import time
import datetime
from django.shortcuts import render
import os
from filelock import FileLock
import redis
import pathlib
import subprocess
import logging
import fileinput
import requests
import traceback
import asyncio
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from dnssec_ttl.settings import ALLOWED_HOSTS


redis_pass = "20092010"
r = redis.Redis(host='localhost', port=6379, db=0, password=redis_pass, decode_responses=True)
domain = "cashcash.app"
containers = ["668a22e2de4e", "6f7e04631710", "306c42b372c2", "abcda5d14762", "9cebd7c983d9",
              "1147a7f801fc", "b9f78b9084b4", "0494d7089c3a", "e4e70b62ffed", "5e69afc16b5d"]
n = len(containers) + 1
container2ip_dict = {
    "1": "3.220.52.113",
    "2": "52.4.120.223",
    "3": "50.16.6.90",
    "4": "44.195.175.12",
    "5": "3.208.196.0",
    "6": "18.207.47.246",
    "7": "52.71.44.50",
    "8": "52.44.221.99",
    "9": "34.226.99.56",
    "10": "3.223.194.233"
}
container2local_ip_dict = {
    "1": "172.17.0.2",
    "2": "172.17.0.3",
    "3": "172.17.0.4",
    "4": "172.17.0.5",
    "5": "172.17.0.6",
    "6": "172.17.0.7",
    "7": "172.17.0.8",
    "8": "172.17.0.9",
    "9": "172.17.0.10",
    "10": "172.17.0.11"
}
zone_ip = ALLOWED_HOSTS[0]


async def _execute_bash(cmd):
    print('Command:', cmd)
    return subprocess.run(cmd, shell=True, capture_output=True)


async def _reload_bind(container_id):
    print('start', datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S'))
    cmd = "docker exec -i " + containers[container_id - 1] + " service named reload"
    p = await _execute_bash(cmd)
    stdout = p.stdout.decode().split('\n') + p.stderr.decode().split('\n')
    started, reloaded = False, False
    for j in stdout:
        if 'Reloading domain name service... named' in j:
            started = True
        if started and '...done' in j:
            reloaded = True
    if not reloaded:
        raise Exception("Reloaded: " + "\n".join(stdout))
    print('end', datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S'))
    return True


def _call_init_api():
    url = "http://" + zone_ip + ':8080/api/init'
    header = {
        "Content-Type": "application/json",
    }
    res = requests.get(url, headers=header)
    if res.status_code != 200:
        raise Exception("init api resulted in error: ")
    return Response({'success': True}, status=status.HTTP_200_OK)


def _call_sign_api(validity):
    url = "http://" + zone_ip + ':8080/api/sign/?validity=' + str(validity)
    header = {
        "Content-Type": "application/json",
    }
    res = requests.get(url, headers=header)
    if res.status_code != 200:
        raise Exception("signing api resulted in error: ")
    return Response({'success': True}, status=status.HTTP_200_OK)


@asyncio.coroutine
async def _init_zone_file(container_id):
    # 1. add "*.<exp_id>.<domain>. IN A container2ip_dict[container_id]
    # 2. modify TTL value (I guess it can be done manually)
    try:
        base_path = '/home/ubuntu/shared/'
        path = base_path + 'v' + str(container_id - 1) + '/zones'
        zone_file_name = "db." + domain
        path = os.path.join(path, zone_file_name)
        cmd = "cp " + base_path + zone_file_name + " " + path
        await _execute_bash(cmd)
        cmd = "docker exec -i " + containers[container_id-1] + " sh -c 'cat > /etc/bind/zones/" + zone_file_name \
              + "' < " + path
        print(path, cmd)
        await _execute_bash(cmd)
        return True
    except Exception as e:
        traceback.print_exc()
        return False


def _replace_in_file(file_path, search_text, new_line):
    found = False
    # with FileLock(file_path):
    with fileinput.input(file_path, inplace=True) as file:
        for line in file:
            if search_text in line:
                found = True
                print(new_line, end='\n')
            else:
                print(line, end='')
    if not found:
        with open(file_path, 'a') as file:
            file.write(new_line + '\n')


@asyncio.coroutine
async def _edit_zone_file(container_id, ttl, exp_id):
    # 1. add "*.<exp_id>.<domain>. IN A container2ip_dict[container_id]
    # 2. modify TTL value (I guess it can be done manually)
    try:
        base_path = '/home/ubuntu/shared/'
        path = base_path + 'v' + str(container_id - 1) + '/zones'
        zone_file_name = "db." + domain
        path = os.path.join(path, zone_file_name)
        # with open(path, 'a') as f:
        new_line = '*.' + exp_id + '	IN	A	' + container2ip_dict[str(container_id)]
        _replace_in_file(path, container2ip_dict[str(container_id)], new_line)
        # f.write('*.' + exp_id + '	IN	A	' + container2ip_dict[str(container_id)] + '\n')

        cmd = "docker exec -i " + containers[container_id-1] + " sh -c 'cat > /etc/bind/zones/" + zone_file_name \
              + "' < " + path
        print(path, cmd)
        await _execute_bash(cmd)
        return True
    except Exception as e:
        traceback.print_exc()
        return False


@asyncio.coroutine
async def _sign(container_id, validity):
    try:
        print('start time', datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S'))
        # if containers[container_id-1] == "6f7e04631710":
        #     return True
        # else:
        val = str(int(validity) * 60)
        # execute the following command in each docker container in a parallel fashion
        signing_cmd = "dnssec-signzone -N INCREMENT -o " + domain + " -e now+" + val + \
                      " -k /etc/bind/zones/Kcashcash.app.+008+13816.key -t /etc/bind/zones/db.cashcash.app " \
                      "/etc/bind/zones/Kcashcash.app.+008+45873.private"
        cmd = "docker exec -i " + containers[container_id-1] + " " + signing_cmd
        p = await _execute_bash(cmd)
        stdout = p.stdout.decode().split('\n') + p.stderr.decode().split('\n')
        signed = False
        for j in stdout:
            if 'Zone fully signed:' in j:
                signed = True
        print(stdout)
        if not signed:
            raise Exception("Signing resulted in failure: " + "\n".join(stdout))
        print('end time', datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S'))
        return True
    except Exception as e:
        traceback.print_exc()
        return False


# Create your views here.


class Init(APIView):
    def get(self, request):
        try:
            kwargs = request.GET.dict()

            # initialize zone files without the wildcard entry (cp the skeleton backup zone file), call the sign api
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                if str(e).startswith('There is no current event loop in thread'):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                else:
                    raise
            tasks = [_init_zone_file(each) for each in [1] + [i for i in range(3, n)]]
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            _call_sign_api(30)
            # Since sign is calling restart anyways
            # for each in range(1, n):
            #     _reload_bind(each)

            if all(result):
                return Response({'success': True}, status=status.HTTP_200_OK)
            else:
                return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)


class Edit(APIView):
    def get(self, request):
        try:
            kwargs = request.GET.dict()
            ttl = kwargs['ttl']
            exp_id = kwargs['exp_id']

            # add the wildcard entry to the zone file, edit the ttl. do it for every container in an async way
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                if str(e).startswith('There is no current event loop in thread'):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                else:
                    raise
            tasks = [_edit_zone_file(each, ttl, exp_id) for each in [1] + [i for i in range(3, n)]]
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            tasks = [_reload_bind(each) for each in [1] + [i for i in range(3, n)]]
            result_reload = loop.run_until_complete(asyncio.gather(*tasks))

            if all(result):
                return Response({'success': True}, status=status.HTTP_200_OK)
            else:
                return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
            _call_init_api()
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)


class Sign(APIView):
    def get(self, request):
        try:
            kwargs = request.GET.dict()
            validity = kwargs['validity']

            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                if str(e).startswith('There is no current event loop in thread'):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                else:
                    raise

            tasks = [_sign(each, validity) for each in [1] + [i for i in range(3, n)]]
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            tasks = [_reload_bind(each) for each in [1] + [i for i in range(3, n)]]
            result_reload = loop.run_until_complete(asyncio.gather(*tasks))

            if all(result):
                return Response({'success': True}, status=status.HTTP_200_OK)
            else:
                return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
            # removing this to prevent looping scenario...where signing after init results into failure which would then call init again
            # _call_init_api()
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)


class Edit_Sign(APIView):
    def get(self, request):
        try:
            kwargs = request.GET.dict()
            ttl = kwargs['ttl']
            exp_id = kwargs['exp_id']
            validity = kwargs['validity']

            # add the wildcard entry to the zone file, edit the ttl. do it for every container in an async way
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                if str(e).startswith('There is no current event loop in thread'):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                else:
                    raise
            tasks = [_edit_zone_file(each, ttl, exp_id) for each in [1] + [i for i in range(3, n)]]
            result = loop.run_until_complete(asyncio.gather(*tasks))

            if all(result):
                tasks = [_sign(each, validity) for each in [1] + [i for i in range(3, n)]]
                result = loop.run_until_complete(asyncio.gather(*tasks))
                # loop.close()
                if all(result):
                    tasks = [_reload_bind(each) for each in [1] + [i for i in range(3, n)]]
                    # result_reload = []
                    # for each in [1] + [i for i in range(3, n)]:
                    #     result_reload.append(asyncio.ensure_future(_reload_bind(each)))
                    # loop.run_forever()
                    result_reload = loop.run_until_complete(asyncio.gather(*tasks))
                    if all(result_reload):
                        return Response({'success': True}, status=status.HTTP_200_OK)
                    else:
                        _call_init_api()
                        return Response({'success': False, 'error': str("Failure in signing")},
                                        status=status.HTTP_400_BAD_REQUEST)
                else:
                    _call_init_api()
                    return Response({'success': False, 'error': str("Failure in signing")}, status=status.HTTP_400_BAD_REQUEST)
            else:
                _call_init_api()
                return Response({'success': False, 'error': str("Failure in editing")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
            _call_init_api()
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)


class BindUpdateViewV2(APIView):
    def get(self, request):
        try:

            file_version = request.GET.get('file_version', None)
            exp_id = request.GET.get('exp_id', None)
            redis_key = "mode-" + str(exp_id)
            if exp_id is None:
                raise Exception

            if file_version not in ['first', 'second', 'remove']:
                raise Exception

            file_version_to_int = {
                "first": 1,
                "second": 3,
                "remove": 2
            }

            r.set(redis_key, file_version_to_int[file_version])
            r.expire(redis_key, 2 * 60)
            return Response({'success': True}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'success': False, 'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

