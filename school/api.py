#! python3.6
# --*-- coding: utf-8 --*--

"""
学校管理
"""

from datetime import datetime, timedelta
import asyncio
import json
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL, ucAppKey, ucAppSecret, permissionAppKey
import aiohttp
import ujson
from utils import get_json, get_params, validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError
from menu.menu import Menu
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId
from enumconstant import Roles, PermissionRole
from tasks.celery_base import BaseTask
class School(BaseHandler):
    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.start_time = BaseTask().start_time

    @validate_permission()
    async def add_school_market(self, request: Request):
        """
        绑定学校和市场，市场即为人
        {
            "user_id": "",
            "school_id": ""
        }
        :param request:
        :return:
        """

        request_data = await get_json(request)
        global_id = request['user_info']['global_id']
        area_id = request['user_info']['area_id']
        channel_id = request['user_info']['channel_id']
        school_id = int(request_data.get("school_id", 0))
        user_id = int(request_data.get("user_id", 0))
        if not channel_id:
            raise RequestError("No channel id")

        market_schema = {
            "parent_id": channel_id,
            "school_id": school_id,
            "user_id": user_id,
            "role": Roles.SCHOOL.value,
            "status": 1,
            "create_at": time.time(),
            "modify_at": time.time()
        }

        await request.app['mongodb'][self.db][self.instance_coll].update_one(
            {"parent_id": channel_id,
             "school_id": school_id,
             "user_id": user_id,
             },
            {"$set": market_schema}
        , upsert=True)

        school = await request.app['mongodb'][self.db][self.instance_coll].find_one(
            {"parent_id": channel_id,
             "school_id": school_id,
             "user_id": user_id,
             "status": 1
             })
        if school:
            school['id'] = str(school['_id'])
            del school['_id']

        return self.reply_ok({"market_school": school})

    @validate_permission()
    async def del_market_school(self, request: Request):
        """
        解绑市场和学校
        {
            "school_id": "",
            "user_id": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)
        channel_id = request['user_info']['channel_id']
        user_id = int(request_data.get("user_id", 0))
        school_id = int((request_data.get("school_id", 0)))
        await request.app['mongodb'][self.db][self.instance_coll].update_one({"parent_id": channel_id,
                                                                              "role": Roles.SCHOOL.value,
                                                                              "user_id": user_id,
                                                                              "school_id": school_id
                                                                              },
                                                                         {"$set": {"status": 0}})

        return self.reply_ok({})


    @validate_permission()
    async def get_market_school(self, request: Request):
        """
        获取市场学校列表
        {
            "page": "",

        }
        :param request:
        :return:
        """
        request_param = await get_params(request)

        page = int(request_param['page'])
        per_page = 10
        total_count = 0
        schools= []
        if request['user_info']['instance_role_id'] == Roles.CHANNEL.value: #渠道
            channel_id = request['user_info']['channel_id']
            print( channel_id)
            market_school = []
            if not channel_id:
                return self.reply_ok({"market_school": market_school})
            channel_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id), "status": 1})
            old_id = channel_info.get('old_id', None) if channel_info else None

            if old_id:
                sql = "select id, full_name, time_create from sigma_account_ob_school where available = 1 and owner_id = %s and time_create>='%s' and time_create <='%s' limit %s,%s" %  (old_id, self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"), per_page*page, per_page)
                total_sql = "select count(id) as total from sigma_account_ob_school where available = 1 and owner_id = %s and time_create>='%s' and time_create <='%s'" % (old_id, self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"))
                async with request.app['mysql'].acquire() as conn:
                    async with conn.cursor(DictCursor) as cur:
                        await cur.execute(sql)
                        schools = await cur.fetchall()
                        await cur.execute(total_sql)
                        total = await cur.fetchall()
                total_count = total[0]['total']
                school_ids = [item['id'] for item in schools]

                distributed_school = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id,
                                                                                               "role": Roles.SCHOOL.value,
                                                                                               "status": 1})

                distributed_school = await distributed_school.to_list(10000)
                print(distributed_school)
                distributed_school_map = {}
                for d_s_m in distributed_school:
                    if distributed_school_map.get("school_id", []):
                        distributed_school_map[d_s_m['school_id']].append(d_s_m['user_id'])
                    else:
                        distributed_school_map[d_s_m['school_id']] = [d_s_m['user_id']]

                distributed_user_ids = [str(item['user_id']) for item in distributed_school]
                print("distributed_user_ids", distributed_user_ids)
                distributed_user = request.app['mongodb'][self.db][self.user_coll].find({"user_id": {"$in": distributed_user_ids},
                                                                                               "status": 1})
                distributed_user = await distributed_user.to_list(10000)

                distributed_user_map = {}
                for d_u in distributed_user:
                    distributed_user_map[int(d_u['user_id'])] = {"user_name": d_u['nickname'], "user_id": d_u['user_id']}

                for school in schools:
                    user_ids = distributed_school_map.get(school['id'], [])
                    users_info = [distributed_user_map.get(int(user_id), {}) for user_id in user_ids]
                    school['market_info'] = users_info


        elif request['user_info']['instance_role_id'] == Roles.GLOBAL.value: #总部
            sql = "select id, full_name, time_create from sigma_account_ob_school where available = 1 and time_create>='%s' and time_create <='%s' limit %s, %s" %(self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"), per_page*page, per_page)
            total_sql = "select count(id) as total from sigma_account_ob_school where available = 1 and time_create>='%s' and time_create <='%s'" % (self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"))
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    schools = await cur.fetchall()
                    await cur.execute(total_sql)
                    total = await cur.fetchall()
            total_count = total[0]['total']
            distributed_school = request.app['mongodb'][self.db][self.instance_coll].find({
                                                                                           "role": Roles.SCHOOL.value,
                                                                                           "status": 1})
            distributed_school = await distributed_school.to_list(10000)
            distributed_user_ids = [str(item['user_id']) for item in distributed_school]

            distributed_user = request.app['mongodb'][self.db][self.user_coll].find(
                {"user_id": {"$in": distributed_user_ids},
                 "status": 1})

            distributed_user = await distributed_user.to_list(10000)

            distributed_school_map = {}
            for d_s_m in distributed_school:
                if distributed_school_map.get("school_id", []):
                    distributed_school_map[d_s_m['school_id']].append(d_s_m['user_id'])
                else:
                    distributed_school_map[d_s_m['school_id']] = [d_s_m['user_id']]

            distributed_user_map = {}
            for d_u in distributed_user:
                distributed_user_map[int(d_u['user_id'])] = {"user_name": d_u['nickname'], "user_id": d_u['user_id']}
            for school in schools:
                user_ids = distributed_school_map.get(school['id'], [])
                users_info = [distributed_user_map.get(int(user_id), {}) for user_id in user_ids]
                school['market_info'] = users_info


        elif request['user_info']['instance_role_id'] == Roles.AREA.value: #大区
            area_id = request['user_info']['area_id']
            channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                                 "role": Roles.CHANNEL.value,
                                                                                 'status': 1})
            channels = await channels.to_list(10000)
            old_ids = [item['old_id'] for item in channels]
            channels_ids = [str(item['_id']) for item in channels]
            if old_ids:
                sql = "select id, full_name,time_create from sigma_account_ob_school " \
                      "where available = 1 and owner_id in (%s) " \
                      "and time_create>='%s' " \
                      "and time_create <='%s' limit %s,%s " % (",".join(str(id) for id in old_ids),
                                                               self.start_time.strftime("%Y-%m-%d"),
                                                               datetime.now().strftime("%Y-%m-%d"),
                                                               per_page*page, per_page)
                total_sql = "select count(id) as total " \
                            "from sigma_account_ob_school " \
                            "where available = 1 and owner_id in (%s) " \
                            "and time_create>='%s' and time_create <='%s'"  % (",".join(str(id) for id in old_ids),
                                                                               self.start_time.strftime("%Y-%m-%d"),
                                                                               datetime.now().strftime("%Y-%m-%d"))
                async with request.app['mysql'].acquire() as conn:
                    async with conn.cursor(DictCursor) as cur:
                        await cur.execute(sql)
                        schools = await cur.fetchall()
                        await cur.execute(total_sql)
                        total = await cur.fetchall()
                total_count = total[0]['total']
                distributed_school = request.app['mongodb'][self.db][self.instance_coll].find({
                    "parent_id": {"$in": channels_ids},
                    "role": Roles.SCHOOL.value,
                    "status": 1})
                distributed_school = await distributed_school.to_list(10000)
                distributed_user_ids = [str(item['user_id']) for item in distributed_school]

                distributed_user = request.app['mongodb'][self.db][self.user_coll].find(
                    {"user_id": {"$in": distributed_user_ids},
                     "status": 1})

                distributed_user = await distributed_user.to_list(10000)

                distributed_school_map = {}
                for d_s_m in distributed_school:
                    if distributed_school_map.get("school_id", []):
                        distributed_school_map[d_s_m['school_id']].append(d_s_m['user_id'])
                    else:
                        distributed_school_map[d_s_m['school_id']] = [d_s_m['user_id']]

                distributed_user_map = {}
                for d_u in distributed_user:
                    distributed_user_map[int(d_u['user_id'])] = {"user_name": d_u['nickname'], "user_id": d_u['user_id']}
                for school in schools:
                    user_ids = distributed_school_map.get(school['id'], [])
                    users_info = [distributed_user_map.get(int(user_id), {}) for user_id in user_ids]
                    school['market_info'] = users_info

        return self.reply_ok({"market_school": schools, "extra": {"total": total_count, "number_per_page": per_page, "curr_page": page}})



    @validate_permission()
    async def get_spare_market_user_for_school(self, request: Request):
        """
        获取某个学校可分配的市场
        {
            "choool_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_id = request['user_info']['channel_id']
        schools = request.app['mongodb'][self.db][self.instance_coll].find({"school_id": int(request_param['school_id']),
                                                                                     "status": 1})

        schools = await schools.to_list(10000)
        print(schools)
        users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": channel_id,
                                                                      "instance_role_id": Roles.MARKET.value,
                                                                      "status": 1})
        users = await users.to_list(10000)
        distributed_user_ids = [str(item['user_id']) for item in schools]
        undistributed_user = []
        for user in users:
            if user["user_id"] not in distributed_user_ids:
                undistributed_user.append({"user_id": user['user_id'], "nickname": user['nickname']})

        return self.reply_ok({"spare_market_users": undistributed_user})