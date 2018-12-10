#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 市场列表
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
from mixins import DataExcludeMixin
from utils import CustomEncoder


class MarketList(BaseHandler, DataExcludeMixin):
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'
        self.school_per_day_coll = "school_per_day"
        self.sale_user_coll = "sale_user"

    @validate_permission()
    async def market_list(self, request: Request):
        """
        市场列表
        {
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get('page', 0))
        per_page = 10
        channel_id = request['user_info']['channel_id']
        # channel_id = '5be57636be16fcb0232c5969'
        total_market_count = await request.app['mongodb'][self.db][self.sale_user_coll].count_documents({"channel_id": channel_id,
                                                                         "instance_role_id": Roles.MARKET.value,
                                                                         "status": 1})
        markets = request.app['mongodb'][self.db][self.sale_user_coll].find({"channel_id": channel_id,
                                                                         "instance_role_id": Roles.MARKET.value,
                                                                         "status": 1}).skip(page*per_page).limit(per_page)
        # total_market_count = request.app['mongodb'][self.db][self.sale_user_coll].find({"channel_id": channel_id,
        #                                                                      "instance_role_id": Roles.MARKET.value,
        #                                                                      "status": 1}).skip(page * per_page).limit(
        #     per_page)
        markets = await markets.to_list(10000)
        # print(markets)
        market_user_ids = [int(item['user_id']) for item in markets]
        schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id,
                                                                            "role": Roles.SCHOOL.value,
                                                                            "user_id": {"$in": market_user_ids},
                                                                            "status": 1})
        schools = await schools.to_list(10000)
        # school_ids = [item['school_id'] for item in schools]

        channel_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id), "status": 1})
        if channel_info:
            old_ids = [channel_info.get("old_id", -1)]
            if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
                exclude_channels = await self.exclude_channel(request.app['mysql'])
                old_ids = list(set(old_ids).difference(set(exclude_channels)))
            items = await self._list(request, old_ids)
            # print(json.dumps(items,indent=4))
            item_map = {}
            for item in items:
                item_map[item['_id']] = item
            # print(json.dumps(items, indent=4, cls=CustomEncoder))
            from collections import defaultdict
            market_school_map = defaultdict(lambda: defaultdict(dict))
            # print("schools", schools)
            for school in schools:
                default = {
                    "total_school_number": 0,
                    "total_teacher_number": 0,
                    "total_student_number": 0,
                    "total_guardian_number": 0,
                    "total_pay_number": 0,
                    "total_pay_amount": 0,
                    "total_valid_exercise_number": 0,
                    "total_valid_word_number": 0,
                    "total_exercise_image_number": 0,
                    "total_word_image_number": 0,
                    "total_valid_reading_number": 0,
                    "pay_ratio": 0,
                    "bind_ratio": 0,
                    "contest_coverage_ratio": 0,
                    "contest_average_per_person": 0,
                }
                school.update(item_map.get(school['school_id'], default))
                if market_school_map[school['user_id']]['n']:
                    market_school_map[school['user_id']]['n'].append(1)
                else:
                    market_school_map[school['user_id']]['n'] = [1]
                if market_school_map[school['user_id']]['schools']:
                    market_school_map[school['user_id']]['schools'].append(school)
                else:
                    market_school_map[school['user_id']]['schools'] = [school]



            # print(json.dumps(market_school_map, indent=4, cls=CustomEncoder))
            #
            # print(json.dumps(items, indent=4, cls=CustomEncoder))
            data = []
            # print("market",len(markets), markets)
            # print("market_school_map", market_school_map)
            for market in markets:
                tmp = {
                    "market_name": market['nickname'],
                    "total_school_number": 0,
                    "total_teacher_number": 0,
                    "total_student_number": 0,
                    "total_guardian_number": 0,
                    "total_pay_number": 0,
                    "total_pay_amount": 0,
                    "total_valid_exercise_number": 0,
                    "total_valid_word_number": 0,
                    "total_exercise_image_number": 0,
                    "total_word_image_number": 0,
                    "total_valid_reading_number": 0,
                    "pay_ratio": 0,
                    "bind_ratio": 0,
                    "contest_coverage_ratio": 0,
                    "contest_average_per_person": 0,
                }
                one_market = market_school_map.get(int(market['user_id']), tmp)
                # print("market", one_market)
                if one_market:

                    tmp["total_school_number"] = sum(one_market.get('n')) if one_market.get('n') else 0
                    if one_market.get('schools'):
                        for school in one_market['schools']:
                            tmp["total_teacher_number"] += school['total_teacher_number']
                            tmp['total_student_number'] += school['total_student_number']
                            tmp['total_guardian_number'] += school['total_guardian_number']
                            tmp['total_pay_number'] += school['total_pay_number']
                            tmp['total_pay_amount'] += school['total_pay_amount']
                            tmp['total_valid_exercise_number'] += school['total_valid_exercise_number']
                            tmp['total_valid_word_number'] += school['total_valid_word_number']
                            tmp['total_exercise_image_number'] += school['total_exercise_image_number']
                            tmp['total_word_image_number'] += school['total_word_image_number']
                            tmp['total_valid_reading_number'] += school['total_valid_reading_number']
                            tmp['contest_coverage_ratio'] += school.get('contest_coverage_ratio',0)
                            tmp['contest_average_per_person'] += school.get("contest_average_per_person", 0)

                    tmp['pay_ratio'] = tmp['total_pay_number'] / tmp['total_student_number'] if tmp['total_student_number']>0 else 0
                    tmp['bind_ratio'] = tmp['total_guardian_number'] / tmp['total_student_number'] if tmp[
                                                                                                'total_student_number'] > 0 else 0

                    data.append(tmp)


            return self.reply_ok({"market_stat": data, "extra": {"total": total_market_count, "number_per_page": per_page, "curr_page": page}})
        return self.reply_ok({"extra": {"total": 0, "number_per_page": per_page, "curr_page": page}})


    async def _list(self, request: Request, channel_ids: list):
        """
        学校统计
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.school_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count":1 ,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            "total_exercise_image_number": {"$sum": "$e_image_c"},
                            "total_word_image_number": {"$sum": "$w_image_c"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_school_number": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items
