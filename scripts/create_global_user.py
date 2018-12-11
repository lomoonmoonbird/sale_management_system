import asyncio
import unittest
import aiohttp
import json
import ujson


UC_SYSTEM_API_URL = 'http://uc.hexinedu.com/api/admin'
UC_SYSTEM_API_ADMIN_URL = 'http://uc.ali.hexin.im/api/admin'
THEMIS_SYSTEM_OPEN_URL = 'http://themis.hexinedu.com/api/open'
THEMIS_SYSTEM_ADMIN_URL = 'http://themis.ali.hexin.im/api/admin'
permissionAppKey = 'themise8aacb6b63'
permissionAppSecret = '1f08aee118e0167277b1f21fb6aa577c'
ucAppKey = '0d8cf2c3b25588ca'
ucAppSecret = '956bc2120d8cf2c3b25588ca8b3e116b'

class TestThemis(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    # def test_app_detail(self):
    #     async def go():
    #         async with aiohttp.ClientSession() as client:
    #             async with client.get(THEMIS_SYSTEM_OPEN_URL + "/application/getOne?appKey=%s" % permissionAppKey, data={}) as res:
    #                 print (res.status)
    #                 # print (json.dumps(res.text, indent=4))
    #
    #     self.loop.run_until_complete(go())

    def test_userrole_create(self):
        async def go():
            async with aiohttp.ClientSession() as client:


                themis_role_user = {
                    "appKey": permissionAppKey,
                    "appSecret": permissionAppSecret,
                    "userId": [3003039],
                    "roleId": [32]
                }

                # 绑定用户和权限角色
                print(ujson.dumps(themis_role_user))
                print(THEMIS_SYSTEM_OPEN_URL + "/userRole/bulkCreate")
                async with client.post(THEMIS_SYSTEM_OPEN_URL + "/userRole/bulkCreate" ,
                                       data=ujson.dumps(themis_role_user),
                                       headers={"Content-Type":"application/json",
                                                "Cookie": "UBUS=OEKewmjlycudDQFBZehzvFdOZzJxqGnzdjUY9UbEoGtPAqZxCY1TSZRPj1q05-pE"}) as res:
                    print (res.status)
                    print(await res.text())

        self.loop.run_until_complete(go())





