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


async def _edit_zone_file(container_id, ttl, exp_id):
    # 1. add "*.<exp_id>.<domain>. IN A container2ip_dict[container_id]
    # 2. modify TTL value (I guess it can be done manually)
    # 3.
    pass


async def _sign(container_id, validity):
    # execute the following command in each docker container in a parallel fashion
    # dnssec-signzone -N INCREMENT -o cashcash.app -e now+(validity*60)
    # -k /etc/bind/zones/Kcashcash.app.+008+13816.key -t /etc/bind/zones/db.cashcash.app
    # /etc/bind/zones/Kcashcash.app.+008+45873.private
    pass


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

        return Response({'success': True}, status=status.HTTP_200_OK)


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
