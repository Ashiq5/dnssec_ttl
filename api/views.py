from django.shortcuts import render
import os
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

domain = "cashcash.app"
containers = ["668a22e2de4e", "6f7e04631710"]
container2ip_dict = {
    "1": "10.0.0.1",
    "2": "10.0.0.2",
    "3": "",
    "4": "",
    "5": "",
    "6": "",
    "7": "",
    "8": ""
}
zone_ip = ALLOWED_HOSTS[0]


def _execute_bash(cmd):
    print('Command:', cmd)
    return subprocess.run(cmd, shell=True, capture_output=True)


def _reload_bind(container_id):
    cmd = "docker exec -i " + containers[container_id - 1] + " service named restart"
    p = _execute_bash(cmd)
    stdout = p.stdout.decode().split('\n') + p.stderr.decode().split('\n')
    started, reloaded = False, False
    for j in stdout:
        if 'Starting domain name service... named' in j:
            started = True
        if started and '...done' in j:
            reloaded = True
    if not reloaded:
        raise Exception("Reloaded: " + "\n".join(stdout))
    return True


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
def _init_zone_file(container_id):
    # 1. add "*.<exp_id>.<domain>. IN A container2ip_dict[container_id]
    # 2. modify TTL value (I guess it can be done manually)
    try:
        base_path = '/home/ubuntu/shared/'
        path = base_path + 'v' + str(container_id)
        zone_file_name = "db." + domain
        path = os.path.join(path, zone_file_name)
        cmd = "cp " + base_path + zone_file_name + " " + path
        _execute_bash(cmd)
        cmd = "docker exec -i " + containers[container_id-1] + " sh -c 'cat > /etc/bind/zones/" + zone_file_name \
              + "' < " + path
        print(path, cmd)
        _execute_bash(cmd)
        return True
    except Exception as e:
        traceback.print_exc()
        return False


@asyncio.coroutine
def _edit_zone_file(container_id, ttl, exp_id):
    # 1. add "*.<exp_id>.<domain>. IN A container2ip_dict[container_id]
    # 2. modify TTL value (I guess it can be done manually)
    try:
        base_path = '/home/ubuntu/shared/'
        path = base_path + 'v' + str(container_id)
        zone_file_name = "db." + domain
        path = os.path.join(path, zone_file_name)
        with open(path, 'a') as f:
            f.write('*.' + exp_id + '	IN	A	' + container2ip_dict[str(container_id)] + '\n')

        cmd = "docker exec -i " + containers[container_id-1] + " sh -c 'cat > /etc/bind/zones/" + zone_file_name \
              + "' < " + path
        print(path, cmd)
        _execute_bash(cmd)
        return True
    except Exception as e:
        traceback.print_exc()
        return False


@asyncio.coroutine
async def _sign(container_id, validity):
    try:
        # execute the following command in each docker container in a parallel fashion
        signing_cmd = "dnssec-signzone -N INCREMENT -o " + domain + " -e now+" + str(int(validity) * 60) + \
                      " -k /etc/bind/zones/Kcashcash.app.+008+13816.key -t /etc/bind/zones/db.cashcash.app " \
                      "/etc/bind/zones/Kcashcash.app.+008+45873.private"
        cmd = "docker exec -i " + containers[container_id-1] + " " + signing_cmd
        p = _execute_bash(cmd)
        stdout = p.stdout.decode().split('\n') + p.stderr.decode().split('\n')
        signed = False
        for j in stdout:
            if 'Zone fully signed:' in j:
                signed = True
        print(stdout)
        if not signed:
            raise Exception("Signing resulted in failure: " + "\n".join(stdout))

        for each in range(1, 3):
            _reload_bind(each)

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
            tasks = [_init_zone_file(each) for each in range(1, 3)]  # TODO: change it to 9
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            _call_sign_api(30)
            for each in range(1, 3):
                _reload_bind(each)

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
            tasks = [_edit_zone_file(each, ttl, exp_id) for each in range(1, 3)]  # TODO: change it to 9
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            for each in range(1, 3):
                _reload_bind(each)

            if all(result):
                return Response({'success': True}, status=status.HTTP_200_OK)
            else:
                return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
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

            tasks = [_sign(each, validity) for each in range(1, 3)]  # TODO: change it to 9
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # loop.close()

            for each in range(1, 3):
                _reload_bind(each)

            if all(result):
                return Response({'success': True}, status=status.HTTP_200_OK)
            else:
                return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            traceback.print_exc()
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)

