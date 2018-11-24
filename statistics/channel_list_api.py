#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 数据总览
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


class ChannelList(BaseHandler):
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'

    @validate_permission()
    async def channel_list(self, request: Request):
        """
        大区列表
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get('page', 0))
        per_page = 100


        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": request['user_info']['area_id'],
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1
                                                                             }).skip(page*per_page).limit(per_page)

        channels = await channels.to_list(10000)
        items = []
        old_ids = [item['old_id'] for item in channels]
        if old_ids:
            sql = "select id, name from sigma_account_us_user where available = 1 and id in (%s) " % \
                  ','.join([str(id) for id in old_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    real_channels = await cur.fetchall()

            channels_map = {}
            for channel in real_channels:
                channels_map[channel["id"]] = channel
            items = await self._list(request, old_ids)
            for item in items:
                item['contest_coverage_ratio'] = 0
                item['contest_average_per_person'] = 0
                item["channel_info"] = channels_map.get(item["_id"], 0)
            items = items
        return self.reply_ok(items)



    async def _list(self, request: Request, channel_ids: list):
        """
        学校数
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
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
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_reading_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$channel",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
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
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_number", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items


