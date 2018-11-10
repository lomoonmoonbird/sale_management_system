#! python3.6
# --*-- coding: utf-8 --*--

"""
注册 登录
"""

from datetime import datetime, timedelta
import asyncio
import json
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL, ucAppKey, ucAppSecret, permissionAppKey
import aiohttp
import ujson
from utils import get_json, get_params
from demo.utils import validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError
from menu.menu import Menu
from auth.utils import insert_area
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId

class Roles(Enum):
    """
    销管对应的实例角色
    """
    SUPER = 0 #超级管理员
    GLOBAL = 1 #总部
    AREA = 2  #大区
    CHANNEL = 3 #渠道
    MARKET = 4 #市场
    SCHOOL = 5 #学校

class PermissionRole(Enum):
    """
    对应themis角色的id
    """
    SUPER = 37 #超级管理员id
    GLOBAL= 35 #总部id
    AREA = 33 #大区id
    CHANNEL = 34 #渠道id
    MARKET = 36 #市场id


class User(BaseHandler):

    def __init__(self):
        self.db = "sales"
        self.user_coll = "sale_user"
        self.instance_coll = "instance"

    @validate_permission()
    async def create_account(self, request: Request):
        """
        批量创建用户，可以单个，可多个
        {
            ""
            "users": [{"username": "","password": ""}, {"username": "", "password": ""}]
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        print(request['user_info'])
        req_param = {
            "appKey": ucAppKey,
            "appSecret": ucAppSecret,
            "data": ujson.dumps(request_data['users'])

        }

        resp = await self.json_post_request(request.app['http_session'],
                                            UC_SYSTEM_API_ADMIN_URL + '/user/bulkCreate',
                                            data=ujson.dumps(req_param))

        print (resp)
        data = {}
        if resp['status'] == 0:
            data = resp['data']
        elif resp['status'] == 1001:
            raise UserExistError("User Already Exist")

        return self.reply_ok(data)

    @validate_permission()
    async def profile(self, request: Request):
        """
        当前登录用户资料
        :param request:
        :return:
        """
        user_info = await request.app['mongodb'][self.db][self.user_coll].find_one({"user_id": str(request['user_info']['user_id'])})
        user_data = {}
        if user_info:
            user_data = {
                "user_id": user_info['user_id'],
                "username": user_info['username'],
                "nickname": user_info['nickname'],
                "phone": int(user_info['phone']),
                "role": int(user_info.get('instance_role_id', -1))
                # "menu": Menu().show()
            }

        return self.reply_ok(user_data)

    @validate_permission()
    async def add_area(self, request: Request):
        """
        添加大区
        {
            "area_name": ""
        }
        :param request:
        :return:
        """
        req_data = await get_json(request)
        area_data = {
            "name": req_data['area_name'],
            "status": 1,
            "create_at": time.time(),
            "modify_at": time.time()
        }

        await self._create_area(request.app['mongodb'].sales.instance, area_data)
        area = await request.app['mongodb'].sales.instance.find_one({"name": req_data['area_name']})
        area_data.update({"area_id": str(area['_id']), "parent_id": str(area['parent_id'])})

        return self.reply_ok(area_data)

    @validate_permission()
    async def add_area_user(self, request: Request):
        """
        创建大区用户
        {
            "area_id": "",
            "nickname": "",
            "username": "",
            "password": "",
            "phone": ""
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        if not request_data.get("area_id"):
            raise RequestError("Parameter error: area_id is empty")
        uc_create_user = {
            "appKey": ucAppKey,
            "appSecret": ucAppSecret,
            "data": ujson.dumps(
                [{
                    "username": request_data['username'],
                    "password": request_data['password'] or 123456
                }]
            )

        }

        #uc创建用户
        create_resp = await self.json_post_request(request.app['http_session'],
                                            UC_SYSTEM_API_ADMIN_URL + '/user/bulkCreate',
                                            data=ujson.dumps(uc_create_user))
        if create_resp['status'] == 0:

            themis_role_user = {
                "appKey": permissionAppKey,
                "userId": create_resp['data'][0]["userId"],
                "roleId": PermissionRole.AREA.value
            }

            #绑定用户和权限角色
            bindg_resp = await self.json_post_request(request.app['http_session'],
                                                THEMIS_SYSTEM_ADMIN_URL + "/userRole/create",
                                                data=ujson.dumps(themis_role_user), cookie=request.headers.get('Cookie'))
            if bindg_resp['status'] == 0:
                global_id = (await request.app['mongodb'][self.db][self.instance_coll].find_one({'role': Roles.GLOBAL.value}))['_id']
                user_data = {
                    "username": create_resp['data'][0]['username'],
                    "user_id": create_resp['data'][0]['userId'],
                    "nickname": request_data['nickname'],
                    "password": request_data['password'] or 123456,
                    "phone": request_data.get('phone', ''),
                    "global_id": str(global_id),
                    "area_id": request_data.get('area_id', ''),
                    "status": 1,
                    "create_at": time.time(),
                    "modify_at": time.time(),
                    "role_id": PermissionRole.AREA.value,
                    "instance_role_id": Roles.AREA.value
                }
                await self._create_user(request.app['mongodb'][self.db][self.user_coll], user_data)
                return self.reply_ok(user_data)

        raise CreateUserError("AREA adding user failed")

    @validate_permission()
    async def add_area_channel(self, request: Request):
        """
        {
            "area_id": "",
            "old_channel_ids": []

        }
        添加大区的渠道
        :return:
        """
        request_data = await get_json(request)
        bulk_update = []
        for old_id in request_data['old_channel_ids']:
            bulk_update.append(UpdateOne({"old_id": old_id},
                                         {"$set": {"parent_id": request_data['area_id'],
                                                   "old_id": int(old_id),
                                                   "role": Roles.CHANNEL.value,
                                                   "status": 1,
                                                   "create_at": time.time(),
                                                   "modify_at": time.time()
                                                   }
                                          },upsert=True))
        if bulk_update:
            ret = await request.app['mongodb'][self.db][self.instance_coll].bulk_write(bulk_update)
            print (ret.bulk_api_result)
        return self.reply_ok({})

    @validate_permission()
    async def get_one_area_channels(self, request: Request):
        """
        获取某一个大区的渠道
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_oids = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": request_param.get("area_id"),
                                                                                 "role": Roles.CHANNEL.value,
                                                                                 "status": 1})
        res = []
        channel_ids = await channel_oids.to_list(10000)
        if channel_ids:
            sql = "select id, name from sigma_account_us_user where available = 1 and id IN (%s) " % (','.join([str(id['old_id']) for id in channel_ids]))
            print(sql)
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    res = await cur.fetchall()

        return self.reply_ok(res)

    @validate_permission()
    async def get_areas(self, request: Request):
        """
        分页获取大区列表
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param['page'])
        per_page = 100
        areas = request.app['mongodb'][self.db][self.instance_coll].find({"role": Roles.AREA.value, "status": 1}).skip(page*per_page).limit(per_page)
        users = request.app['mongodb'][self.db][self.user_coll].find({"instance_role_id": Roles.AREA.value})
        areas = await areas.to_list(10000)
        users = await users.to_list(10000)
        data = []
        for area in areas:
            one_area = {
                "area_id": str(area['_id']),
                "area_name": area['name'],
                "create_at": area['create_at'],
                "parent_id": str(area['parent_id']),
                "users": []
            }

            for user in users:
                if str(area['_id']) == user['area_id']:
                    one_user = {
                        "user_id": user['user_id'],
                        "username": user['username'],
                        "nickname": user['nickname'],
                        "phone": user['phone'],

                    }
                    one_area['users'].append(one_user)
            data.append(one_area)
        return self.reply_ok(data)

    @validate_permission()
    async def get_channels(self, request: Request):
        """
        分页获取渠道列表
        :param request:
        :return:
        """
        request_param = await get_params(request)
        # page = int(request_param['page'])
        sql = "select * from sigma_account_us_user where available = 1 and role_id=6"
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()

        return self.reply_ok(res)

    @validate_permission()
    async def del_area(self, request: Request):
        """
        删除大区
        {
            "area_id": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)

        channels_counts = await request.app['mongodb'][self.db][self.instance_coll].count_documents({"parent_id": ObjectId(request_data['area_id']),
                                                                                        "role": Roles.CHANNEL.value})
        if channels_counts > 0:
            raise DELETEERROR("Area has channels")
        await request.app['mongodb'][self.db][self.user_coll].update_many({"area_id": request_data['area_id']},
                                                                          {"$set": {"status": 0}})


        return self.reply_ok({})

    @validate_permission()
    async def del_area_channels(self, request: Request):
        """
        删除大区渠道
        {
            "area_id": "",
            "channel_ids": []
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)

        bulk_update = []
        for channelid in request_data['channel_ids']:
            bulk_update.append(UpdateOne({"_id": ObjectId(channelid)}, {"$set": {"status": 0}}))

        if bulk_update:
            ret = await request.app['mongodb'][self.db][self.instance_coll].bulk_write(bulk_update)
            print(ret.bulk_api_result)
        return self.reply_ok({})

    @validate_permission()
    async def add_channel_user(self, request: Request):
        """
        创建渠道用户
        {
            "area_id: "",
            "channel_id": "",
            "nickname": "",
            "username": "",
            "password": "",
            "phone": ""
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        if not request_data.get("area_id") or not request_data.get("channel_id"):
            raise RequestError("Parameter error: area_id or channel_id is empty")
        uc_create_user = {
            "appKey": ucAppKey,
            "appSecret": ucAppSecret,
            "data": ujson.dumps(
                [{
                    "username": request_data['username'],
                    "password": request_data['password'] or 123456
                }]
            )

        }

        # uc创建用户
        create_resp = await self.json_post_request(request.app['http_session'],
                                                   UC_SYSTEM_API_ADMIN_URL + '/user/bulkCreate',
                                                   data=ujson.dumps(uc_create_user))
        if create_resp['status'] == 0:

            themis_role_user = {
                "appKey": permissionAppKey,
                "userId": create_resp['data'][0]["userId"],
                "roleId": PermissionRole.CHANNEL.value
            }

            # 绑定用户和权限角色
            bindg_resp = await self.json_post_request(request.app['http_session'],
                                                      THEMIS_SYSTEM_ADMIN_URL + "/userRole/create",
                                                      data=ujson.dumps(themis_role_user),
                                                      cookie=request.headers.get('Cookie'))
            if bindg_resp['status'] == 0:

                user_data = {
                    "username": create_resp['data'][0]['username'],
                    "user_id": create_resp['data'][0]['userId'],
                    "nickname": request_data['nickname'],
                    "phone": request_data.get('phone', ''),
                    "status": 1,
                    "create_at": time.time(),
                    "modify_at": time.time(),
                    "global_id": request['user_info']['global_id'],
                    "area_id": request_data['area_id'],
                    "channel_id": request_data.get('channel_id', ''),
                    "role_id": PermissionRole.CHANNEL.value,
                    "instance_role_id": Roles.CHANNEL.value
                }
                await self._create_user(request.app['mongodb'][self.db][self.user_coll], user_data)
                return self.reply_ok(user_data)

        raise CreateUserError("Channel adding user failed")

    @validate_permission()
    async def get_area_user_channels(self, request: Request):
        """
        分页获取带有标注大区的渠道
        :param request:
        :return:
        """
        try:
            request_param = await get_params(request)
            page = int(request_param['page'])
            per_page = 100

            query_cond = {
                "role": Roles.CHANNEL.value
            }
            area_infos = []
            res = []
            if not request['user_info']['area_id']:
                return self.reply_ok({"areas": [], "channels": []})
            if request["user_info"]["instance_role_id"] != Roles.GLOBAL.value:
                area_id = request['user_info']['area_id']
                query_cond.update({"parent_id": ObjectId(area_id)})

                area_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(request['user_info']['area_id'])})

                channels = request.app['mongodb'][self.db][self.instance_coll].find(query_cond).skip(page*per_page).limit(per_page)
                channels = await channels.to_list(100000)
                old_ids = [str(item['old_id']) for item in channels]
                sql = ''
                if old_ids:
                    sql = "select * from sigma_account_us_user where available = 1 and role_id = 6 and id in (%s)" % ','.join(old_ids)

                    async with request.app['mysql'].acquire() as conn:
                        async with conn.cursor(DictCursor) as cur:
                            await cur.execute(sql)
                            res = await cur.fetchall()

                # area_info["id"] = str(area_info['_id'])
                # area_info["parent_id"] = str(area_info['parent_id'])


                area_info = {
                    "area_id": str(area_info['_id']),
                    "area_name": area_info['name']
                }
                area_infos.append(area_info)
                res = res
            else:
                sql = "select * from sigma_account_us_user where available = 1 and role_id = 6 limit %s, %s " % (page*per_page, per_page)

                async with request.app['mysql'].acquire() as conn:
                    async with conn.cursor(DictCursor) as cur:
                        await   cur.execute(sql)
                        res = await cur.fetchall()

                old_ids = [item['id'] for item in res]

                channels = request.app['mongodb'][self.db][self.instance_coll].find({"old_id": {"$in": old_ids}})
                channels = await channels.to_list(100000)
                for channel in res:
                    for c in channels:
                        if channel['id'] == c['old_id']:
                            channel['area_id'] = str(c['parent_id'])
                            break

                parent_ids = [item['parent_id'] for item in channels]
                area_info = request.app['mongodb'][self.db][self.instance_coll].find(
                    {"parent_id": {"$in": parent_ids}, "status": 1})
                area_info = await area_info.to_list(10000)
                for a_i in area_info:
                    area_infos.append({
                        "area_id": str(a_i['_id']),
                        "area_name": a_i['name'],

                    })
        except:
            import traceback
            traceback.print_exc()

        return self.reply_ok({"areas": area_infos, "channels": res})

    @validate_permission()
    async def add_market_user(self, request: Request):
        """
        创建市场用户
        {
            "area_id": "",
            "channel_id": "",
            "nickname": "",
            "username": "",
            "password": "",
            "phone": ""
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        if not request_data.get("area_id") or not request_data.get("channel_id"):
            raise RequestError("Parameter error: area_id or channel_id is empty")
        uc_create_user = {
            "appKey": ucAppKey,
            "appSecret": ucAppSecret,
            "data": ujson.dumps(
                [{
                    "username": request_data['username'],
                    "password": request_data['password'] or 123456
                }]
            )

        }

        # uc创建用户
        create_resp = await self.json_post_request(request.app['http_session'],
                                                   UC_SYSTEM_API_ADMIN_URL + '/user/bulkCreate',
                                                   data=ujson.dumps(uc_create_user))
        if create_resp['status'] == 0:

            themis_role_user = {
                "appKey": permissionAppKey,
                "userId": create_resp['data'][0]["userId"],
                "roleId": PermissionRole.MARKET.value
            }

            # 绑定用户和权限角色
            bindg_resp = await self.json_post_request(request.app['http_session'],
                                                      THEMIS_SYSTEM_ADMIN_URL + "/userRole/create",
                                                      data=ujson.dumps(themis_role_user),
                                                      cookie=request.headers.get('Cookie'))
            if bindg_resp['status'] == 0:
                user_data = {
                    "username": create_resp['data'][0]['username'],
                    "user_id": create_resp['data'][0]['userId'],
                    "nickname": request_data['nickname'],
                    "password": request_data['password'] or 123456,
                    "phone": request_data.get('phone', ''),
                    "global_id": request['user_info']['global_id'],
                    "area_id": request['user_info']['area_id'],
                    "channel_id": request_data.get('channel_id', ''),
                    "status": 1,
                    "create_at": time.time(),
                    "modify_at": time.time(),
                    "role_id": PermissionRole.MARKET.value,
                    "instance_role_id": Roles.MARKET.value
                }
                await self._create_user(request.app['mongodb'][self.db][self.user_coll], user_data)
                return self.reply_ok(user_data)

        raise CreateUserError("Market adding user failed")

    @validate_permission()
    async def get_market_user(self, request: Request):
        """
        获取市场用户
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param['page'])
        per_page = 100

        #todo
        if request['user_info']['instance_role_id'] != Roles.GLOBAL.value:
            channel_id = request['user_info']['channel_id']
            users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": channel_id, "status": 1}).skip(page*per_page).limit(per_page)
            channel_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id), "status": 1})



    async def _create_user(self, col: Collection, user_data: dict):
        return await col.update_one({"user_id": user_data['user_id']},
                                    {"$set": user_data},
                                    upsert=True)

    async def _create_area(self, col: Collection, area_data: dict):
        """
        创建大区 并关联其所属总部，及当前大区角色2
        :param col:
        :param area_data:
        :return:
        """
        global_id = (await col.find_one({'role': Roles.GLOBAL.value}))['_id']
        area_data.update({"parent_id": global_id, "role": Roles.AREA.value})
        return await col.update_one({"name": area_data['name']},
                                                                {"$set": area_data},
                                                                upsert=True)


