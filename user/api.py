#! python3.6
# --*-- coding: utf-8 --*--

"""
注册 登录
"""

from datetime import datetime
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL,THEMIS_SYSTEM_OPEN_URL, ucAppKey, \
    ucAppSecret, permissionAppKey, permissionAppSecret
import ujson
from utils import get_json, get_params, validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError, InstanceExistError
from motor.core import Collection
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne
from bson import ObjectId
from enumconstant import Roles, PermissionRole
from mixins import DataExcludeMixin
from tasks.celery_base import BaseTask


class User(BaseHandler, DataExcludeMixin):
    """
    账号 用户 实例 管理
    """
    def __init__(self):
        self.db = "sales"
        self.user_coll = "sale_user"
        self.instance_coll = "instance"
        self.start_time = BaseTask().start_time

    @validate_permission()
    async def profile(self, request: Request):
        """
        当前登录用户资料
        :param request:
        :return:
        """
        user_coll = request.app['mongodb'][self.db][self.user_coll]
        user_info = await user_coll.find_one({"user_id": str(request['user_info']['user_id'])})
        user_data = {}
        if user_info:
            user_data = {
                "user_id": user_info['user_id'],
                "username": user_info['username'],
                "nickname": user_info['nickname'],
                "phone": user_info['phone'],
                "role": int(user_info.get('instance_role_id', -1))
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
        area_coll = request.app['mongodb'][self.db][self.instance_coll]
        area = await area_coll.find_one({"name": req_data['area_name'], "status": 1})
        if area:
            raise InstanceExistError("area exist")
        await self._create_area(area_coll, area_data)
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
        try:
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
            if create_resp['status'] == 1001:
                raise UserExistError("user exist")
            if create_resp['status'] == 0:

                themis_role_user = {
                    "appKey": permissionAppKey,
                    "appSecret": permissionAppSecret,
                    "userId": [create_resp['data'][0]["userId"]],
                    "roleId": [PermissionRole.AREA.value]
                }

                #绑定用户和权限角色
                bindg_resp = await self.json_post_request(request.app['http_session'],
                                                          THEMIS_SYSTEM_OPEN_URL + "/userRole/bulkCreate",
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
        except:
            import traceback
            traceback.print_exc()
        raise CreateUserError("AREA adding user failed")

    @validate_permission(operate_validation=True)
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
        old_channel_ids = [int(id) for id in request_data['old_channel_ids']]
        channels = request.app['mongodb'][self.db][self.instance_coll].find({
            # "parent_id": str(request_data['area_id']),
            # "operator": request['user_info']['user_id'],
            "old_id": {"$in": old_channel_ids},
            "role": Roles.CHANNEL.value,
            "status": 1
        }, {"old_id": 1, "operator": 1, "_id": 0})
        channels = await channels.to_list(None)
        can_operate_channel_ids = []
        if not channels:
            can_operate_channel_ids = old_channel_ids
        else:
            channels_map = {}
            for c in channels:
                channels_map[c['old_id']] = c
            for old_id in old_channel_ids:
                operator = channels_map.get(old_id, {}).get("operator")
                if operator and operator != request['user_info']['user_id']:
                    continue
                else:
                    can_operate_channel_ids.append(old_id)


        if not can_operate_channel_ids:
            result = await request.app['mongodb'][self.db][self.instance_coll]\
                .update_many({"parent_id": str(request_data['area_id']),
                              "operator": request['user_info']['user_id'],
                              "role": Roles.CHANNEL.value,
                              "status": 1}, {"$set": {"status":0,
                                                      "operator": ""}})
        else:
            result = await request.app['mongodb'][self.db][self.instance_coll]\
                .update_many({
                              "parent_id": str(request_data['area_id']),
                              "operator": request['user_info']['user_id'],
                              "role": Roles.CHANNEL.value,
                              "status": 1}, {"$set": {"status": 0, "operator": ""}})
            bulk_update = []
            for old_id in can_operate_channel_ids:
                bulk_update.append(UpdateOne({"old_id": old_id, "role": Roles.CHANNEL.value, "status": 1},
                                             {"$set": {"parent_id": str(request_data['area_id']),
                                                       "operator": request['user_info']['user_id'],
                                                       "role": Roles.CHANNEL.value,
                                                       "create_at": time.time(),
                                                       "modify_at": time.time()
                                                       }
                                              }, upsert=True))
            if bulk_update:
                ret = await request.app['mongodb'][self.db][self.instance_coll].bulk_write(bulk_update)

        return self.reply_ok({})




        # else:
        #     bulk_update = []
        #     prepare_channel_info = request.app['mongodb'][self.db][self.instance_coll]\
        #         .find({
        #                "old_id": {"$in": old_channel_ids},
        #                "role": Roles.CHANNEL.value,
        #                "status": 1}, {"old_id": 1, "operator": 1, "_id": 0})
        #
        #
        #     prepare_channel_info = await prepare_channel_info.to_list(None)
        #     prepare_channel_info_map = {}
        #     for p_c_i in prepare_channel_info:
        #         prepare_channel_info_map[p_c_i['old_id']] = p_c_i
        #
        #     prepare_channels_map = {}
        #     for id in old_channel_ids:
        #         prepare_channels_map[id] = prepare_channel_info_map.get(id, {})
        #     can_operate_channel_ids = []
        #
        #     for id in old_channel_ids:
        #         if prepare_channels_map.get(id):
        #             if prepare_channels_map.get(id).get("operator", "") == int(request['user_info']['user_id']) \
        #                     or  prepare_channels_map.get(id).get("operator", "") is "":
        #                 can_operate_channel_ids.append(id)
        #         else:
        #             can_operate_channel_ids.append(id)
        #     result = await request.app['mongodb'][self.db][self.instance_coll]\
        #         .update_many({
        #                       "parent_id": str(request_data['area_id']),
        #                       "role": Roles.CHANNEL.value,
        #                       "status": 1}, {"$set": {"status": 0, "operator": ""}})
        #     for old_id in can_operate_channel_ids:
        #
        #         bulk_update.append(UpdateOne({"old_id": old_id, "role": Roles.CHANNEL.value, "status": 1},
        #                                      {"$set": {"parent_id": str(request_data['area_id']),
        #                                                "operator": request['user_info']['user_id'],
        #                                                "role": Roles.CHANNEL.value,
        #                                                "create_at": time.time(),
        #                                                "modify_at": time.time()
        #                                                }
        #                                       }, upsert=True))
        #     if bulk_update:
        #         ret = await request.app['mongodb'][self.db][self.instance_coll].bulk_write(bulk_update)
        # return self.reply_ok({})

    @validate_permission(data_validation=True)
    async def get_one_area_channels(self, request: Request):
        """
        获取某一个大区的渠道
        {
            "area_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_oids = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": request_param.get("area_id"),
                                                                                 "role": Roles.CHANNEL.value,
                                                                                 "status": 1})
        res = []
        channel_ids = await channel_oids.to_list(10000)

        channel_ids = ','.join([str(id['old_id']) for id in channel_ids])

        if channel_ids:
            sql = "select id, name from " \
                  "sigma_account_us_user " \
                  "where available = 1 " \
                  "and id IN (%s) " % (channel_ids)
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    res = await cur.fetchall()

        return self.reply_ok(res)

    @validate_permission(data_validation=True)
    async def get_areas(self, request: Request):
        """
        分页获取大区列表
        {
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get('page', 1)) - 1
        per_page = 10
        total_count = 0
        exclude_area = [ObjectId(id) for id in request['data_permission']['exclude_area']]
        include_area = [ObjectId(id) for id in request['data_permission']['include_area']]

        query = {}
        if not include_area:
            query = {"_id": {"$nin": exclude_area},
                              "role": Roles.AREA.value, "status": 1}
        else:
            query = {"_id": {"$in": include_area},
             "role": Roles.AREA.value, "status": 1}
        total_count = await request.app['mongodb'][self.db][self.instance_coll]\
            .count_documents(query)
        areas = request.app['mongodb'][self.db][self.instance_coll]\
            .find(query).sort('create_at', -1).skip(page*per_page).limit(per_page)
        users = request.app['mongodb'][self.db][self.user_coll].find({"instance_role_id": Roles.AREA.value, "status": 1})
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
        return self.reply_ok({"area_list": data, "extra": {"total": total_count, "number_per_page": per_page, "curr_page": page + 1}})

    @validate_permission(data_validation=True)
    async def get_channels(self, request: Request):
        """
        分页获取渠道列表
        :param request:
        :return:
        """
        request_param = await get_params(request)
        # page = int(request_param['page'])
        exclude_channels_u = request['data_permission']['exclude_channel']
        exclude_channels = await self.exclude_channel(request.app['mysql']) + exclude_channels_u
        exclude_channel_str = ','.join(['"' + str(id) + '"' for id in exclude_channels]) if exclude_channels else "''"
        include_channel = request['data_permission']['include_channel']
        include_channel_str = ','.join(['"' + str(id) + '"' for id in include_channel]) if include_channel else "''"
        sql = "select id,username,name from sigma_account_us_user where available = 1 and role_id=6 and id not in (%s)" % (exclude_channel_str)
        if include_channel:
            sql = "select id,username,name from sigma_account_us_user where available = 1 and role_id=6 and id in (%s)" % (
                include_channel_str)
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        channel_that_has_belong_to_area = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": {"$nin": exclude_channels},
                                                                                                    "role":Roles.CHANNEL.value,
                                                                                                    "status": 1}, {"old_id": 1,
                                                                                                                   "operator": 1,
                                                                                                                   "_id": 0})
        channel_that_has_belong_to_area = await channel_that_has_belong_to_area.to_list(None)
        channel_that_has_belong_to_area_map = {}
        for c_a in channel_that_has_belong_to_area:
            channel_that_has_belong_to_area_map[c_a['old_id']] = c_a
        data = []
        for r in res:
            if r['id'] in exclude_channels:
                continue
            r['operator'] = channel_that_has_belong_to_area_map.get(r['id'], {}).get("operator", "")
            data.append(r)
        return self.reply_ok(data)

    @validate_permission(operate_validation=True)
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

        channels_counts = await request.app['mongodb'][self.db][self.instance_coll].count_documents({"parent_id": request_data['area_id'],
                                                                                        "role": Roles.CHANNEL.value, "status": 1})

        if channels_counts > 0:
            raise DELETEERROR("Area has channels")
        await request.app['mongodb'][self.db][self.instance_coll].update_many({"_id": ObjectId(request_data['area_id'])},
                                                                          {"$set": {"status": 0}})
        await request.app['mongodb'][self.db][self.user_coll].update_many({"area_id": request_data['area_id']},
                                                                          {"$set": {"status": 0}})


        return self.reply_ok({})

    @validate_permission()
    async def del_area_user(self, request: Request):
        """
        删除大区用户
        {
            "area_id": "",
            "user_id": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)
        user_id = str(request_data['user_id'])
        await request.app['mongodb'][self.db][self.user_coll].update_one({"user_id": user_id,
                                                                        "status": 1}, {"$set": {"status": 0}})

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
        if create_resp['status'] == 1001:
            raise UserExistError("user exist")
        if create_resp['status'] == 0:
            themis_role_user = {
                "appKey": permissionAppKey,
                "appSecret": permissionAppSecret,
                "userId": [create_resp['data'][0]["userId"]],
                "roleId": [PermissionRole.CHANNEL.value]
            }

            # 绑定用户和权限角色
            bindg_resp = await self.json_post_request(request.app['http_session'],
                                                      THEMIS_SYSTEM_OPEN_URL + "/userRole/bulkCreate",
                                                      data=ujson.dumps(themis_role_user),
                                                      cookie=request.headers.get('Cookie'))

            if bindg_resp['status'] == 0:

                user_data = {
                    "username": create_resp['data'][0]['username'],
                    "user_id": create_resp['data'][0]['userId'],
                    "nickname": request_data['nickname'],
                    "password": request_data['password'] or 123456,
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
    async def del_channel_user(self, request: Request):
        """
        删除渠道账号
        {
            "user_id" :""
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        user_id = str(request_data['user_id'])

        channel_user = await request.app['mongodb'][self.db][self.user_coll].find_one({"user_id": user_id, "status": 1})
        channel_id = channel_user.get("channel_id", "")
        market_user = request.app['mongodb'][self.db][self.user_coll].find({"instance_role_id": Roles.MARKET.value, "channel_id": channel_id, "status": 1})
        market_user = await market_user.to_list(10000)

        if market_user:
            raise DELETEERROR("channel has markets")
        await request.app['mongodb'][self.db][self.user_coll].update_one({"user_id": user_id,
                                                                          "status": 1}, {"$set": {"status": 0}})

        return self.reply_ok({})

    @validate_permission(data_validation=True)
    async def get_area_user_channels(self, request: Request):
        """
        分页获取大区用户的渠道
        {
            "page": ""
        }
        :param request:
        :return:
        """
        try:
            request_param = await get_params(request)
            page = int(request_param.get('page', 1)) - 1
            per_page = 10
            total_count = 0
            query_cond = {
                "role": Roles.CHANNEL.value,
                "status": 1
            }
            area_infos = []
            res = []
            # if not request['user_info']['area_id']:
            #     return self.reply_ok({"channels": [], "extra":{"total": total_count,"number_per_page": per_page, "curr_page": page}})

            if int(request["user_info"]["instance_role_id"]) == Roles.AREA.value:
                area_id = request['user_info']['area_id']
                query_cond.update({"parent_id": area_id})

                area_info = await request.app['mongodb'][self.db][self.instance_coll].\
                    find_one({"_id": ObjectId(request['user_info']['area_id']), "status": 1})
                total_count = await request.app['mongodb'][self.db][self.instance_coll].count_documents(query_cond)
                channels = request.app['mongodb'][self.db][self.instance_coll].find(query_cond).skip(page*per_page).limit(per_page)
                channels = await channels.to_list(100000)
                channels_map = {}
                for channel in channels:
                    channels_map[channel['old_id']] = {"channel_id": str(channel['_id'])}

                old_ids = [str(item['old_id']) for item in channels]
                channels_ids = [str(item['_id']) for item in channels]
                users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": {"$in": channels_ids},
                                                                              "instance_role_id": Roles.CHANNEL.value,
                                                                              "status": 1})

                users = await users.to_list(10000)

                users_map = {}
                for u in users:
                    one_user = {"user_id": u.get('user_id', -1), "nickname": u.get('nickname', ''),
                                "username": u.get("username", ""), "phone": u.get("phone", "")}
                    if users_map.get(u['channel_id'], []):
                        users_map[u['channel_id']].append(one_user)
                    else:
                        users_map[u['channel_id']] = [one_user]

                sql = ''
                if old_ids:
                    sql = "select * from sigma_account_us_user " \
                          "where available = 1 " \
                          "and role_id = 6 " \
                          "and time_create >= '%s' " \
                          "and time_create <= '%s' " \
                          "and id in (%s)" % (
                                              self.start_time.strftime("%Y-%m-%d"),
                                              datetime.now().strftime("%Y-%m-%d"),
                                              ','.join(old_ids))

                    async with request.app['mysql'].acquire() as conn:
                        async with conn.cursor(DictCursor) as cur:
                            await cur.execute(sql)
                            res = await cur.fetchall()

                # area_info["id"] = str(area_info['_id'])
                # area_info["parent_id"] = str(area_info['parent_id'])
                for channel in res:
                    channel['channel_id'] = channels_map.get(channel['id'], {}).get("channel_id", "")
                    channel['area_info'] = {"area_id": str(area_info['_id']), "area_name": area_info['name']}
                    channel['channel_info'] = channels_map.get(channel['id'], {})
                    channel['user_info'] = users_map.get(channel['channel_id'], [])


            elif int(request["user_info"]["instance_role_id"]) == Roles.GLOBAL.value:
                exclude_channel = request['data_permission']['exclude_channel']
                exclude_channel_str = ','.join(['"'+str(id)+'"' for id in exclude_channel]) if exclude_channel else "''"
                include_channel = request['data_permission']['include_channel']
                include_channel_str = ','.join(
                    ['"' + str(id) + '"' for id in include_channel]) if include_channel else "''"
                sql = ''
                count_sql = ''
                if not include_channel:
                    sql = "select * from sigma_account_us_user " \
                          "where available = 1 " \
                          "and role_id = 6 " \
                          "and id not in (%s)" \
                          "and time_create >= '%s' " \
                          "and time_create <= '%s' " \
                          " limit %s, %s " % (exclude_channel_str,
                                              self.start_time.strftime("%Y-%m-%d"),
                                                  datetime.now().strftime("%Y-%m-%d"),
                                                  page*per_page,
                                                  per_page)

                    count_sql = "select count(id) as total_count " \
                                "from sigma_account_us_user " \
                                "where available = 1 " \
                                "and id not in (%s)" \
                                "and role_id = 6 " \
                                "and time_create>='%s'" \
                                " and time_create<='%s'" %(exclude_channel_str,
                                                           self.start_time.strftime("%Y-%m-%d"),
                                                         datetime.now().strftime("%Y-%m-%d"))
                else:
                    sql = "select * from sigma_account_us_user " \
                          "where available = 1 " \
                          "and role_id = 6 " \
                          "and id in (%s)" \
                          "and time_create >= '%s' " \
                          "and time_create <= '%s' " \
                          " limit %s, %s " % (include_channel_str,
                                              self.start_time.strftime("%Y-%m-%d"),
                                              datetime.now().strftime("%Y-%m-%d"),
                                              page * per_page,
                                              per_page)

                    count_sql = "select count(id) as total_count " \
                                "from sigma_account_us_user " \
                                "where available = 1 " \
                                "and id in (%s)" \
                                "and role_id = 6 " \
                                "and time_create>='%s'" \
                                " and time_create<='%s'" % (include_channel_str,
                                                            self.start_time.strftime("%Y-%m-%d"),
                                                            datetime.now().strftime("%Y-%m-%d"))
                async with request.app['mysql'].acquire() as conn:
                    async with conn.cursor(DictCursor) as cur:
                        await   cur.execute(sql)
                        res = await cur.fetchall()
                        await cur.execute(count_sql)
                        total_count = await cur.fetchall()
                        total_count = total_count[0]['total_count']
                        # print(total_count)
                old_ids = [item['id'] for item in res]
                channels = request.app['mongodb'][self.db][self.instance_coll].find({"old_id": {"$in": old_ids}, "role": Roles.CHANNEL.value, "status": 1})
                channels = await channels.to_list(10000)
                parent_ids = list(set([ObjectId(item['parent_id']) for item in channels]))
                area_info = request.app['mongodb'][self.db][self.instance_coll].find(
                    {"_id": {"$in": parent_ids}, "status": 1})
                area_info = await area_info.to_list(10000)
                users = request.app['mongodb'][self.db][self.user_coll].find({"instance_role_id": Roles.CHANNEL.value, "status": 1})
                users = await users.to_list(10000)
                from collections import defaultdict
                users_map = defaultdict(list)
                for user in users:
                    users_map[user['channel_id']].append({"user_id": user['user_id'], "nickname": user['nickname'],
                                                          "username": user['username'], 'phone': user['phone']})
                channels_oid_map = {}
                channels_id_map = {}
                for channel in channels:
                    channels_oid_map[channel['old_id']] = channel
                    channels_id_map[str(channel['_id'])] = channel

                area_info_map = {}
                for a_i in area_info:
                    area_info_map[str(a_i['_id'])] = a_i
                for channel in res:
                    area_id = ''
                    channel['channel_info'] = {
                        "channel_id": str(channels_oid_map.get(channel['id'], {}).get("_id", ""))
                    }
                    channel['user_info'] = users_map.get(str(channels_oid_map.get(channel['id'], {}).get("_id", "")), [])

                    area_id = channels_oid_map.get(channel['id'], {}).get("parent_id", "")
                    # channel['area_info'] = area_info_map.get(area_id,  {"area_id": "", "area_name": ""})
                    channel['area_info'] = {
                        "area_id": area_info_map.get(area_id, {}).get("_id", ""),
                        "area_name": area_info_map.get(area_id, {}).get("name", "")
                    }
                    # for area in area_info:
                    #     if area_id == str(area['_id']):
                    #         print('1')
                    #         channel['area_info'] = {"area_id": area_id, "area_name": area['name']}
                    #     else:
                    #         print(2)
                    #         channel['area_info'] = {"area_id": "", "area_name": ""}


        except:
            import traceback
            traceback.print_exc()

        return self.reply_ok({"channels": res, "extra":{"total": total_count,"number_per_page": per_page, "curr_page": page + 1}})

    @validate_permission()
    async def add_market_user(self, request: Request):
        """
        创建市场用户
        {
            "nickname": "",
            "username": "",
            "password": "",
            "phone": ""
        }
        :param request:
        :return:
        """
        try:
            request_data = await get_json(request)

            # if not request_data.get("area_id") or not request_data.get("channel_id"):
            #     raise RequestError("Parameter error: area_id or channel_id is empty")
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
            if create_resp['status'] == 1001:
                raise UserExistError("user exist")
            if create_resp['status'] == 0:

                themis_role_user = {
                    "appKey": permissionAppKey,
                    "appSecret": permissionAppSecret,
                    "userId": [create_resp['data'][0]["userId"]],
                    "roleId": [PermissionRole.MARKET.value]
                }

                # 绑定用户和权限角色
                bindg_resp = await self.json_post_request(request.app['http_session'],
                                                          THEMIS_SYSTEM_OPEN_URL + "/userRole/bulkCreate",
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
                        "channel_id": request['user_info']['channel_id'],
                        "status": 1,
                        "create_at": time.time(),
                        "modify_at": time.time(),
                        "role_id": PermissionRole.MARKET.value,
                        "instance_role_id": Roles.MARKET.value
                    }
                    await self._create_user(request.app['mongodb'][self.db][self.user_coll], user_data)
                    await request.app['mongodb'][self.db][self.instance_coll].\
                        update_one({"parent_id": request['user_info']['channel_id'],
                                    "user_id": user_data['user_id'],
                                    "status": 1}, {"$set": {"parent_id": request['user_info']['channel_id'],
                                                            "user_id": user_data['user_id'],
                                                            "role": Roles.MARKET.value,
                                                            "status": 1,
                                                            "create_at": time.time(),
                                                            "modify_at": time.time()}}, upsert=True)
                    return self.reply_ok(user_data)
        except:
            import  traceback
            traceback.print_exc()

        raise CreateUserError("Market adding user failed")

    @validate_permission()
    async def update_marker_user(self, request: Request):
        """
        编辑市场用户信息
        {
            "user_id": "",
            "nickname": "",
            "phone": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)
        user_id = request_data['user_id']
        nickname = request_data['nickname']
        phone = request_data['phone']
        user = await request.app['mongodb'][self.db][self.user_coll].find_one({"user_id": user_id})
        if not user:
            return self.reply_ok({})
        await request.app['mongodb'][self.db][self.user_coll].update_one({"user_id": user_id},
                                                                         {"$set": {"nickname": nickname,
                                                                                   "phone": phone,
                                                                                   "modify_at": time.time()}})

        return self.reply_ok({})

    @validate_permission()
    async def del_market_user(self, request: Request):
        """
        删除市场
        {
            "user_id": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)
        channel_id = request['user_info']['channel_id']
        await request.app['mongodb'][self.db][self.user_coll].update_many({"channel_id": channel_id,
                                                                          "user_id": request_data['user_id']},
                                                                         {"$set": {"status": 0}})

        return self.reply_ok({})

    @validate_permission(data_validation=True)
    async def get_market_user(self, request: Request):
        """
        获取市场用户
        {
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get('page')) - 1
        per_page = 10


        channels = []
        users_info = []
        total_count = 0
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:

            include_channel = request['data_permission']['include_channel']
            query = {}
            if not include_channel:
                query = {"instance_role_id": Roles.MARKET.value, "status": 1}
            else:
                query = {"instance_role_id": Roles.MARKET.value, "parent_id": {"$in": include_channel},  "status": 1}
            users = request.app['mongodb'][self.db][self.user_coll].find(query).skip(page*per_page).limit(per_page)
            users = await users.to_list(10000)

            total_count = await request.app['mongodb'][self.db][self.user_coll].count_documents(query)
            area_ids = list(set([item['area_id'] for item in users]))
            channel_ids = list(set([item['channel_id'] for item in users]))
            area_info = request.app['mongodb'][self.db][self.instance_coll].find({"_id": area_ids, "status": 1})
            area_info = await area_info.to_list(10000)
            channel_info = request.app['mongodb'][self.db][self.instance_coll].find({"_id": channel_ids, "status": 1})
            channel_info = await channel_info.to_list(10000)

            for user in users:
                for area in area_info:
                    if user['area_id'] == str(area['_id']):
                        user["area_info"] = {"area_id": user['area_id'], "area_name": area['name']}
                        break
                for channel in channel_info:
                    if user['channel_id'] == (channel['_id']):
                        user['channel_info'] = {"channel_id": user['channel_id']}
                        break
                users_info.append(user)

        elif request['user_info']['instance_role_id'] == Roles.AREA.value:
            channel_of_area =  request.app['mongodb'][self.db][self.instance_coll].\
                find({"parent_id": request['user_info']['area_id'],
                      "status": 1})

            channel_of_area = await channel_of_area.to_list(100000)
            channel_ids = [str(item['_id']) for item in channel_of_area]
            area_info = await request.app['mongodb'][self.db][self.instance_coll].\
                find_one({"_id": ObjectId(request['user_info']['area_id']),
                          "status": 1})
            # if channel_old_ids:
            #     sql = "select * from sigma_account_us_user where available = 1 and" \
            #           " role_id = 6 and id in %s " % (','.join(channel_old_ids))
            #
            #     async with request.app['mysql'].acquire() as conn:
            #         async with conn.cursor(DictCursor) as cur:
            #             await cur.execute(sql)
            #             channel_info = await cur.fetchall()
            users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": {"$in": channel_ids},"instance_role_id": Roles.MARKET.value,"status": 1}).skip(page*per_page).limit(per_page)
            total_count = await request.app['mongodb'][self.db][self.user_coll].count_documents({"channel_id": {"$in": channel_ids},"instance_role_id": Roles.MARKET.value, "status": 1})
            users = await users.to_list(10000)
            for user in users:
                for channel in channel_of_area:
                    user['id'] = str(user['_id'])

                    if user['channel_id'] == str(channel['_id']):
                        user['channel_info'] = {"channel_id": user['channel_id']}
                        user['area_info'] = {"area_id": str(area_info['_id']), "area_name": area_info['name']}
                del user['_id']
                users_info.append(user)

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            channel_id = request['user_info']['channel_id']
            total_count = await request.app['mongodb'][self.db][self.user_coll].count_documents({"channel_id": channel_id,
                                                                              "instance_role_id": Roles.MARKET.value,
                                                                              "status": 1})
            users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": channel_id,
                                                                              "instance_role_id": Roles.MARKET.value,
                                                                              "status": 1})\
                .skip(page*per_page).limit(per_page)


            area_info = await request.app['mongodb'][self.db][self.instance_coll]. \
                find_one({"_id": ObjectId(request['user_info']['area_id']),
                          "status": 1})
            channel_info = await request.app['mongodb'][self.db][self.instance_coll]. \
                find_one({"_id": ObjectId(request['user_info']['channel_id']),
                          "status": 1})
            users = await users.to_list(100000)
            for user in users:
                user['channel_info'] = {"channel_id": str(channel_info['_id'])} if channel_info else {"channel_id": ""}
                user['area_info'] = {"area_id": str(area_info['_id']), "area_name": area_info['name']}
                users_info.append(user)



        return self.reply_ok({"users": users_info, "extra": {"total": total_count, "number_per_page": per_page, "curr_page": page + 1}})

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
        area_data.update({"parent_id": str(global_id), "role": Roles.AREA.value})
        return await col.update_one({"name": area_data['name']},
                                                                {"$set": area_data},
                                                                upsert=True)

