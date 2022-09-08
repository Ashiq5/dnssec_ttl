from django.shortcuts import render
import os
import pathlib
import subprocess
import logging
import fileinput
import asyncio
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

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


def _execute_bash(cmd):
    print('Command:', cmd)
    return subprocess.run(cmd, shell=True, capture_output=True)


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
            f.write('*.' + exp_id + '.' + domain + '.' + '	IN	A	' + container2ip_dict[container_id])

        cmd = "docker exec -i " + containers[container_id-1] + " sh -c 'cat > /etc/bind/zones/" + zone_file_name + "' < " + \
              path
        print(path, cmd)
        _execute_bash(cmd)
        return True
    except Exception as e:
        print(e)
        return False


async def _sign(container_id, validity):
    try:
        # execute the following command in each docker container in a parallel fashion
        signing_cmd = "dnssec-signzone -N INCREMENT -o " + domain + " -e now+" + (validity * 60) + \
                      " -k /etc/bind/zones/Kcashcash.app.+008+13816.key -t /etc/bind/zones/db.cashcash.app " \
                      "/etc/bind/zones/Kcashcash.app.+008+45873.private"
        cmd = "docker exec -i " + containers[container_id-1] + " " + signing_cmd
        _execute_bash(cmd)
        return True
    except Exception as e:
        print(e)
        return False


# Create your views here.


class Init(APIView):
    def get(self, request):
        kwargs = request.GET.dict()

        # initialize zone files without the wildcard entry (cp the skeleton backup zone file), call the sign api

        return Response({'success': True}, status=status.HTTP_200_OK)


class Edit(APIView):
    def get(self, request):
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
        loop.close()

        if all(result):
            return Response({'success': True}, status=status.HTTP_200_OK)
        else:
            return Response({'success': False, 'error': str("Failure")}, status=status.HTTP_400_BAD_REQUEST)


class Sign(APIView):
    def get(self, request):
        kwargs = request.GET.dict()
        validity = kwargs['validity']

        async def worker():
            tasks = [_sign(each, validity) for each in range(1, 9)]
            await asyncio.wait(tasks)

        loop = asyncio.get_event_loop()
        coroutine = worker()
        loop.run_until_complete(coroutine)

        return Response({'success': True}, status=status.HTTP_200_OK)
