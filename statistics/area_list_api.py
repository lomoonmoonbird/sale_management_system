#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 大区列表
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
from utils import CustomEncoder
from mixins import DataExcludeMixin


class AreaList(BaseHandler, DataExcludeMixin):
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'

    @validate_permission(data_validation=True)
    async def area_list(self, request: Request):
        """
        大区列表
        {
            "page": ""
        }
        :param request:
        :return:
        """

        request_param = await get_params(request)
        page = int(request_param.get('page', 0))
        per_page = 10
        total_count = 0
        exclude_area_objectid = [ObjectId(id) for id in request['data_permission']['exclude_area']]
        areas = request.app['mongodb'][self.db][self.instance_coll].find({"_id": {"$nin": exclude_area_objectid},
                                                                          "parent_id": request['user_info']['global_id'],
                                                                          "role": Roles.AREA.value,
                                                                          "status": 1}).skip(page*per_page).limit(per_page)
        areas = await areas.to_list(100000)
        total_count = await request.app['mongodb'][self.db][self.instance_coll].count_documents({"_id": {"$nin": request['data_permission']['exclude_area']},
                                                                                                 "parent_id": request['user_info']['global_id'],
                                                                          "role": Roles.AREA.value,
                                                                          "status": 1})
        areas_ids = [str(item['_id']) for item in areas]
        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": {"$in": areas_ids},
                                                                             "role": Roles.CHANNEL.value, "status": 1})
        channels = await channels.to_list(100000)

        old_ids = list(set([item['old_id'] for item in channels]))
        areas_map = {}
        for area in areas:
            areas_map[str(area['_id'])] = area
        channels_map = {}
        for channel in channels:
            channels_map[channel['old_id']] = channel['parent_id']

        exclude_channels = await self.exclude_channel(request.app['mysql'])
        old_ids = list(set(old_ids).difference(set(exclude_channels)))

        items = await self._list(request, old_ids)

        items = await self.compute_area_list(request, areas, areas_map, channels_map, items)

        return self.reply_ok({"area_list": items, "extra": {'total': total_count, "number_per_page": per_page, "curr_page": page}})


    async def compute_area_list(self, request, areas, areas_map, channels_map, items):
        """
        计算大区列表总和
        :param request:
        :return:
        """


        from collections import defaultdict
        area_compact_data = defaultdict(dict)
        for item in items:
            item['contest_coverage_ratio'] = 0
            item['contest_average_per_person'] = 0
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_school_number', []).append(item.get('total_school_number',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_school_number', []).append(item.get('range_school_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_teacher_number', []).append(item.get('total_teacher_number',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_student_number', []).append(item.get('total_student_number',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_guardian_number', []).append(item.get('total_guardian_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_guardian_number', []).append(item.get('range_guardian_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_guardian_unique_number', []).append(item.get('total_guardian_unique_number', 0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_guardian_unique_number', []).append(item.get('range_guardian_unique_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_guardian_unique_count', []).append(item.get('total_guardian_unique_count',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_guardian_unique_count', []).append(item.get('range_guardian_unique_count', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_pay_number', []).append(item.get('total_pay_number',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_pay_amount', []).append(item.get('total_pay_amount',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_pay_amount', []).append(item.get('range_pay_amount', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_valid_reading_number', []).append(item.get('total_valid_reading_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_valid_reading_number', []).append(item.get('range_valid_reading_number', 0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_valid_exercise_number', []).append(item.get('total_valid_exercise_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_valid_exercise_number', []).append(item.get('range_valid_exercise_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_valid_word_number', []).append(item.get('total_valid_word_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_valid_word_number', []).append(item.get('range_valid_word_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_exercise_image_number', []).append(item.get('total_exercise_image_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_exercise_image_number', []).append(item.get('range_exercise_image_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'total_word_image_number', []).append(item.get('total_word_image_number',0))
            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'range_word_image_number', []).append(item.get('range_word_image_number', 0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'pay_ratio', []).append(item.get('pay_ratio',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'bind_ratio', []).append(item.get('bind_ratio',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'contest_coverage_ratio', []).append(item.get('contest_coverage_ratio',0))

            area_compact_data.setdefault(channels_map.get(item['_id'], 0), {}).setdefault(
                'contest_average_per_person', []).append(item.get('contest_average_per_person',0))

            item["area_info"] = areas_map.get(channels_map.get(item['_id'], 0), {})

        items = []
        for area_id, item in area_compact_data.items():
            items.append(
                {
                    "total_school_number": sum(item['total_school_number']),
                    "range_school_number": sum(item['range_school_number']),
                    "total_teacher_number": sum(item['total_teacher_number']),
                    "total_student_number": sum(item['total_student_number']),
                    "total_guardian_number": sum(item['total_guardian_number']),
                    "range_guardian_number": sum(item['range_guardian_number']),
                    "total_guardian_unique_count": sum(item['total_guardian_unique_count']),
                    "range_guardian_unique_count": sum(item['range_guardian_unique_count']),
                    "total_pay_number": sum(item['total_pay_number']),
                    "total_pay_amount": sum(item['total_pay_amount']),
                    "range_pay_amount": sum(item['total_pay_amount']),
                    "total_valid_exercise_number": sum(item['total_valid_exercise_number']),
                    "range_valid_exercise_number": sum(item['range_valid_exercise_number']),
                    "total_valid_word_number": sum(item['total_valid_word_number']),
                    "range_valid_word_number": sum(item['range_valid_word_number']),
                    "total_exercise_image_number": sum(item['total_exercise_image_number']),
                    "range_exercise_image_number": sum(item['range_exercise_image_number']),
                    "total_word_image_number": sum(item['total_word_image_number']),
                    "range_word_image_number": sum(item['range_word_image_number']),
                    "total_valid_reading_number": sum(item['total_valid_reading_number']),
                    "range_valid_reading_number": sum(item['range_valid_reading_number']),
                    "pay_ratio": self.rounding(
                        sum(item['total_pay_number']) / sum(item['total_student_number'])) if sum(
                        item['total_student_number']) else 0,
                    "bind_ratio": self.rounding(
                        sum(item['total_guardian_unique_count']) / sum(item['total_student_number'])) if sum(
                        item['total_student_number']) else 0,
                    "contest_coverage_ratio": sum(item['contest_coverage_ratio']),
                    "contest_average_per_person": sum(item['contest_average_per_person']),
                    "area_info": areas_map.get(area_id, {})
                }
            )
        channel_compact_data = {}
        for item in items:
            channel_compact_data[str(item['area_info'].get('_id', ""))] = item
        items = []
        for area in areas:
            if channel_compact_data.get(str(area['_id'])):
                items.append(channel_compact_data.get(str(area['_id'])))
            else:
                items.append({
                    "total_school_number": 0,
                    "range_school_number": 0,
                    "total_teacher_number": 0,
                    "total_student_number": 0,
                    "total_guardian_number": 0,
                    "range_guardian_number": 0,
                    "range_guardian_unique_count": 0,
                    "total_pay_number": 0,
                    "total_pay_amount": 0,
                    "range_pay_amount": 0,
                    "total_valid_exercise_number": 0,
                    "range_valid_exercise_number": 0,
                    "total_valid_word_number": 0,
                    "range_valid_word_number": 0,
                    "total_exercise_image_number": 0,
                    "range_exercise_image_number": 0,
                    "total_word_image_number": 0,
                    "range_word_image_number": 0,
                    "total_valid_reading_number": 0,
                    "range_valid_reading_number": 0,
                    "pay_ratio": 0,
                    "bind_ratio": 0,
                    "contest_coverage_ratio": 0,
                    "contest_average_per_person": 0,
                    "area_info": areas_map.get(str(area['_id']), {})
                })
        return items

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
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},

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

                {"$group": {"_id": "$channel",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
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


