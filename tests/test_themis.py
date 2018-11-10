import asyncio
import unittest
import aiohttp
import json
from configs import THEMIS_SYSTEM_OPEN_URL, permissionAppKey, THEMIS_SYSTEM_ADMIN_URL



class TestThemis(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_app_detail(self):
        async def go():
            async with aiohttp.ClientSession() as client:
                async with client.get(THEMIS_SYSTEM_OPEN_URL + "/application/getOne?appKey=%s" % permissionAppKey, data={}) as res:
                    print (res.status)
                    # print (json.dumps(res.text, indent=4))

        self.loop.run_until_complete(go())

    def test_userrole_create(self):
        async def go():
            async with aiohttp.ClientSession() as client:
                cookie = {
                    "UBUS": "7vBHpCpzU6wh_MN0qVuV85iFpGvVDWN4_9mqMt6D8AO2aGAL17Y2-UGTnffxf2oj"
                }
                data = {
                    "appKey": permissionAppKey,
                    "userId": 5300305,
                    "roleId": 37
                }
                print ()
                async with client.post(THEMIS_SYSTEM_ADMIN_URL + "/userRole/create" ,
                                       data=data,
                                       headers={"Cookie": "UBUS=7vBHpCpzU6wh_MN0qVuV85iFpGvVDWN4_9mqMt6D8AO2aGAL17Y2-UGTnffxf2oj"}) as res:
                    print (res.status)
                    # print (json.dumps(res.text, indent=4))

        self.loop.run_until_complete(go())