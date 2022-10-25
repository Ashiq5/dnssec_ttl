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
