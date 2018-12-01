#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 导出全局 大区 渠道月报表格
"""

from datetime import datetime, timedelta, date
import asyncio
import mimetypes
import os
from tempfile import NamedTemporaryFile
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
from collections import defaultdict
from enumconstant import Roles, PermissionRole
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor
from statistics.export.export_base import ExportBase
from mixins import DataExcludeMixin

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime)):
            return str(obj)
        if isinstance(obj, (ObjectId)):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

class ChannelExportReport(BaseHandler, ExportBase, DataExcludeMixin):
    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'
        self.thread_pool = ThreadPoolExecutor(20)


    @validate_permission()
    async def month(self, request: Request):
        """
        渠道导出月报
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_id = request_param.get("channel_id", "")
        if not channel_id:
            return self.reply_ok({})

        schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id, "role": Roles.SCHOOL.value,"status": 1})
        schools = await schools.to_list(10000)
        schools_ids = [item['school_id'] for item in schools]

        if schools_ids:
            sql = "select id,full_name from sigma_account_ob_school where available = 1 and id in (%s) " % \
                  ','.join([str(id) for id in schools_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    school_info = await cur.fetchall()

            school_map = {}
            for school in school_info:
                school_map[school['id']] = school
            exclude_schools = await self.exclude_schools(request.app['mysql'])
            schools_ids = list(set(schools_ids).difference(set(exclude_schools)))
            items = await self._list_month(request, schools_ids)
            template_path = os.path.dirname(__file__) + "/templates/channel_month_template.xlsx"
            sheet = await request.app.loop.run_in_executor(self.thread_pool,
                                                           self.sheet,
                                                           template_path,
                                                           items,
                                                           school_map,
                                                           "month")

            return await self.replay_stream(sheet, "渠道月报-" + datetime.now().strftime("%Y-%m-%d"), request)
        return self.reply_ok({})

    @validate_permission()
    async def week(self, request: Request):
        """
        渠道导出周报
        :param request:
        :return:
        """

        request_param = await get_params(request)
        channel_id = request_param.get("channel_id", "")
        if not channel_id:
            return self.reply_ok({})

        schools = request.app['mongodb'][self.db][self.instance_coll].find(
            {"parent_id": channel_id, "role": Roles.SCHOOL.value, "status": 1})
        schools = await schools.to_list(10000)
        schools_ids = [item['school_id'] for item in schools]

        if schools_ids:
            sql = "select id,full_name from sigma_account_ob_school where available = 1 and id in (%s) " % \
                  ','.join([str(id) for id in schools_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    school_info = await cur.fetchall()

            school_map = {}
            for school in school_info:
                school_map[school['id']] = school
            exclude_schools = await self.exclude_schools(request.app['mysql'])
            schools_ids = list(set(schools_ids).difference(set(exclude_schools)))
            items = await self._list_week(request, schools_ids)
            template_path = os.path.dirname(__file__) + "/templates/channel_week_template.xlsx"
            sheet = await request.app.loop.run_in_executor(self.thread_pool,
                                                           self.sheet,
                                                           template_path,
                                                           items,
                                                           school_map,
                                                           "week")

            return await self.replay_stream(sheet, "渠道周报-" + datetime.now().strftime("%Y-%m-%d"), request)
        return self.reply_ok({})

    async def _list_month(self, request: Request, school_ids: list):
        """
        月报
        :param request:
        :param channel_ids:
        :return:
        """

        coll = request.app['mongodb'][self.db][self.grade_per_day_coll]
        items = []

        last_last_month_first_day, last_last_month_last_day, last_month_first_day, last_month_last_day,\
        curr_month_first_day, curr_month_last_day = self._curr_and_last_and_last_last_month()

        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_ids}
                    }
                },
                {
                    "$project": {
                        "school_id": 1,
                        "channel": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count":1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "total_images": {"$sum": ["$e_image_c", "$w_image_c"]},


                        "teacher_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$teacher_number", 0]},
                        "teacher_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$teacher_number", 0]},

                        "student_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$student_number", 0]},
                        "student_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$student_number", 0]},

                        "guardian_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$guardian_count", 0]},
                        "guardian_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$guardian_count", 0]},

                        "guardian_unique_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$guardian_unique_count", 0]},
                        "guardian_unique_number_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$guardian_unique_count", 0]},

                        "pay_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$pay_number", 0]},
                        "pay_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$pay_number", 0]},

                        "pay_amount_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$pay_amount", 0]},
                        "pay_amount_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$pay_amount", 0]},

                        "valid_reading_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$valid_reading_count", 0]},
                        "valid_reading_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$valid_reading_count", 0]},

                        "valid_exercise_count_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$valid_exercise_count", 0]},
                        "valid_exercise_count_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$valid_exercise_count", 0]},

                        "valid_word_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$valid_word_count", 0]},
                        "valid_word_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$valid_word_count", 0]},

                        "e_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$e_image_c", 0]},
                        "e_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$e_image_c", 0]},

                        "w_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$w_image_c", 0]},
                        "w_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "valid_reading_count": {"$sum": "$valid_reading_count"},
                            "valid_exercise_count": {"$sum": "$valid_exercise_count"},
                            "valid_word_count": {"$sum": "$valid_word_count"},
                            "e_image_c": {"$sum": "$e_image_c"},
                            "w_image_c": {"$sum": "$w_image_c"},
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_unique_guardian_number": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
                            "teacher_number_curr_month": {"$sum": "$teacher_number_curr_month"},
                            "teacher_number_last_month": {"$sum": "$teacher_number_last_month"},
                            "student_number_curr_month": {"$sum": "$student_number_curr_month"},
                            "student_number_last_month": {"$sum": "$student_number_last_month"},
                            "guardian_number_curr_month": {"$sum": "$guardian_number_curr_month"},
                            "guardian_number_last_month": {"$sum": "$guardian_number_last_month"},
                            "guardian_unique_number_curr_month": {"$sum": "$guardian_unique_number_curr_month"},
                            "guardian_unique_number_last_month": {"$sum": "$guardian_unique_number_last_month"},
                            "pay_number_curr_month": {"$sum": "$pay_number_curr_month"},
                            "pay_number_last_month": {"$sum": "$pay_number_last_month"},
                            "pay_amount_curr_month": {"$sum": "$pay_amount_curr_month"},
                            "pay_amount_last_month": {"$sum": "$pay_amount_last_month"},
                            "valid_reading_count_curr_month": {"$sum": "$valid_reading_count_curr_month"},
                            "valid_reading_count_last_month": {"$sum": "$valid_reading_count_last_month"},
                            "valid_exercise_count_curr_month": {"$sum": "$valid_exercise_count_curr_month"},
                            "valid_exercise_count_last_month": {"$sum": "$valid_exercise_count_last_month"},
                            "valid_word_count_curr_month": {"$sum": "$valid_word_count_curr_month"},
                            "valid_word_count_last_month": {"$sum": "$valid_word_count_last_month"},
                            "e_image_c_curr_month": {"$sum": "$e_image_c_curr_month"},
                            "e_image_c_last_month": {"$sum": "$e_image_c_last_month"},
                            "w_image_c_curr_month": {"$sum": "$w_image_c_curr_month"},
                            "w_image_c_last_month": {"$sum": "$w_image_c_last_month"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_unique_guardian_number": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count": 1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "teacher_number_curr_month": 1,
                        "teacher_number_last_month": 1,
                        "student_number_curr_month": 1,
                        "student_number_last_month": 1,
                        "guardian_number_curr_month": 1,
                        "guardian_number_last_month": 1,
                        "guardian_unique_number_curr_month": 1,
                        "guardian_unique_number_last_month": 1,
                        "pay_number_curr_month": 1,
                        "pay_number_last_month": 1,
                        "pay_amount_curr_month": 1,
                        "pay_amount_last_month": 1,
                        "valid_reading_count_curr_month": 1,
                        "valid_reading_count_last_month": 1,
                        "valid_exercise_count_curr_month": 1,
                        "valid_exercise_count_last_month": 1,
                        "valid_word_count_curr_month": 1,
                        "valid_word_count_last_month": 1,
                        "e_image_c_curr_month": 1,
                        "e_image_c_last_month": 1,
                        "w_image_c_curr_month": 1,
                        "w_image_c_last_month": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                 {"$divide": ["$total_guardian_number", "$total_student_number"]}]},
                        # "pay_ratio": {"$divide": ["$total_pay_number", "$total_student_number"]},
                        # "bind_ratio": {"$divide": ["$total_guardian_number", "$total_student_number"]},
                        # "school_MoM":
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items
    async def _list_week(self, request: Request, school_ids: list):
        """
        学校数
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.grade_per_day_coll]
        items = []
        current_week = self.current_week()
        last_week = self.last_week()
        last_last_week = self.last_last_week()
        last_week_first_day, last_week_last_day, \
        last_last_week_first_day, last_last_week_last_day =  last_week[0], last_week[6],\
                                                             last_last_week[0], last_last_week[6]

        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_ids}
                    }
                },
                {
                    "$project": {
                        "school_id": 1,
                        "channel": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count":1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "total_images": {"$sum": ["$e_image_c", "$w_image_c"]},

                        "teacher_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$teacher_number", 0]},
                        "teacher_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$teacher_number", 0]},

                        "student_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$student_number", 0]},
                        "student_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$student_number", 0]},

                        "guardian_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$guardian_count", 0]},
                        "guardian_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$guardian_count", 0]},

                        "guardian_unique_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$guardian_unique_count", 0]},
                        "guardian_unique_number_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                                "$gte": ["$day", last_last_week_first_day]}]}, "$guardian_unique_count", 0]},

                        "pay_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$pay_number", 0]},
                        "pay_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$pay_number", 0]},

                        "pay_amount_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$pay_amount", 0]},
                        "pay_amount_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$pay_amount", 0]},

                        "valid_reading_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                                "$gte": ["$day", last_week_first_day]}]}, "$valid_reading_count", 0]},
                        "valid_reading_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                                "$gte": ["$day", last_last_week_first_day]}]}, "$valid_reading_count", 0]},

                        "valid_exercise_count_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                            "$gte": ["$day", last_week_first_day]}]}, "$valid_exercise_count", 0]},
                        "valid_exercise_count_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                            "$gte": ["$day", last_last_week_first_day]}]}, "$valid_exercise_count", 0]},

                        "valid_word_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                                "$gte": ["$day", last_week_first_day]}]}, "$valid_word_count", 0]},
                        "valid_word_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                                "$gte": ["$day", last_last_week_first_day]}]}, "$valid_word_count", 0]},

                        "e_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                                "$gte": ["$day", last_week_first_day]}]}, "$e_image_c", 0]},
                        "e_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                                "$gte": ["$day", last_last_week_first_day]}]}, "$e_image_c", 0]},

                        "w_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_week_last_day]}, {
                                "$gte": ["$day", last_week_first_day]}]}, "$w_image_c", 0]},
                        "w_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_week_last_day]}, {
                                "$gte": ["$day", last_last_week_first_day]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "valid_reading_count": {"$sum": "$valid_reading_count"},
                            "valid_exercise_count": {"$sum": "$valid_exercise_count"},
                            "valid_word_count": {"$sum": "$valid_word_count"},
                            "e_image_c": {"$sum": "$e_image_c"},
                            "w_image_c": {"$sum": "$w_image_c"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_unique_guardian_number": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
                            "teacher_number_curr_month": {"$sum": "$teacher_number_curr_month"},
                            "teacher_number_last_month": {"$sum": "$teacher_number_last_month"},
                            "student_number_curr_month": {"$sum": "$student_number_curr_month"},
                            "student_number_last_month": {"$sum": "$student_number_last_month"},
                            "guardian_number_curr_month": {"$sum": "$guardian_number_curr_month"},
                            "guardian_number_last_month": {"$sum": "$guardian_number_last_month"},
                            "guardian_unique_number_curr_month": {"$sum": "$guardian_unique_number_curr_month"},
                            "guardian_unique_number_last_month": {"$sum": "$guardian_unique_number_last_month"},
                            "pay_number_curr_month": {"$sum": "$pay_number_curr_month"},
                            "pay_number_last_month": {"$sum": "$pay_number_last_month"},
                            "pay_amount_curr_month": {"$sum": "$pay_amount_curr_month"},
                            "pay_amount_last_month": {"$sum": "$pay_amount_last_month"},
                            "valid_reading_count_curr_month": {"$sum": "$valid_reading_count_curr_month"},
                            "valid_reading_count_last_month": {"$sum": "$valid_reading_count_last_month"},
                            "valid_exercise_count_curr_month": {"$sum": "$valid_exercise_count_curr_month"},
                            "valid_exercise_count_last_month": {"$sum": "$valid_exercise_count_last_month"},
                            "valid_word_count_curr_month": {"$sum": "$valid_word_count_curr_month"},
                            "valid_word_count_last_month": {"$sum": "$valid_word_count_last_month"},
                            "e_image_c_curr_month": {"$sum": "$e_image_c_curr_month"},
                            "e_image_c_last_month": {"$sum": "$e_image_c_last_month"},
                            "w_image_c_curr_month": {"$sum": "$w_image_c_curr_month"},
                            "w_image_c_last_month": {"$sum": "$w_image_c_last_month"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_unique_guardian_number": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count": 1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "teacher_number_curr_month": 1,
                        "teacher_number_last_month": 1,
                        "student_number_curr_month": 1,
                        "student_number_last_month": 1,
                        "guardian_number_curr_month": 1,
                        "guardian_number_last_month": 1,
                        "guardian_unique_number_curr_month": 1,
                        "guardian_unique_number_last_month": 1,
                        "pay_number_curr_month": 1,
                        "pay_number_last_month": 1,
                        "pay_amount_curr_month": 1,
                        "pay_amount_last_month": 1,
                        "valid_reading_count_curr_month": 1,
                        "valid_reading_count_last_month": 1,
                        "valid_exercise_count_curr_month": 1,
                        "valid_exercise_count_last_month": 1,
                        "valid_word_count_curr_month": 1,
                        "valid_word_count_last_month": 1,
                        "e_image_c_curr_month": 1,
                        "e_image_c_last_month": 1,
                        "w_image_c_curr_month": 1,
                        "w_image_c_last_month": 1,

                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    def sheet(self, template, items,  school_map, report_type):
        file = load_workbook(template)
        sheet_names = file.sheetnames
        sheet = file[sheet_names[0]]

        area_dimesion_items = {}


        row1 = sheet[1]
        if report_type == 'week':
            last_week = self.last_week()
            row1[0].value = "渠道_" + last_week[0] + "-" + last_week[6] + "周报数据"
        elif report_type == 'month':
            _, _, last_month, _, _, _ = self._curr_and_last_and_last_last_month()
            month = datetime.strptime(last_month, "%Y-%m-%d").timetuple()[1]
            row1[0].value = "渠道" + str(month) + "月报数据"

        summary_map = defaultdict(list)
        for index, school_data in enumerate(items):
            row = sheet[index + 4]
            index += 1
            # 大区名字
            row[0].value = index
            row[1].value = school_map.get(school_data.get("_id", ""), {}).get("full_name", "")

            # 新增教师数量
            total_teacher_number = school_data.get("total_teacher_number", 0)
            teacher_number_curr_month = school_data.get("teacher_number_curr_month", 0)
            teacher_number_last_month = school_data.get("teacher_number_last_month", 0)
            mom = (teacher_number_curr_month - teacher_number_last_month) / teacher_number_last_month \
                if teacher_number_last_month else 0
            row[2].value = total_teacher_number
            row[3].value = teacher_number_last_month
            row[4].value = teacher_number_curr_month
            row[5].value = self.percentage(mom)
            summary_map[2].append(row[2].value)
            summary_map[3].append(row[3].value)
            summary_map[4].append(row[4].value)
            summary_map[5].append(mom)

            # 新增学生数量
            total_student_number = school_data.get("total_student_number", 0)
            student_number_curr_month = school_data.get("student_number_curr_month", 0)
            student_number_last_month = school_data.get("student_number_last_month", 0)
            mom = (student_number_curr_month - student_number_last_month) / student_number_last_month \
                if student_number_last_month else 0
            row[6].value = total_student_number
            row[7].value = student_number_last_month
            row[8].value = student_number_curr_month
            row[9].value = self.percentage(mom)
            summary_map[6].append(row[6].value)
            summary_map[7].append(row[7].value)
            summary_map[8].append(row[8].value)
            summary_map[9].append(mom)
            # 新增考试数量
            valid_exercise_count = school_data.get("valid_exercise_count", 0)
            valid_exercise_count_curr_month = school_data.get("valid_exercise_count_curr_month", 0)
            valid_exercise_count_last_month = school_data.get("valid_exercise_count_last_month", 0)
            mom = (valid_exercise_count_curr_month - valid_exercise_count_last_month) / valid_exercise_count_last_month \
                if valid_exercise_count_last_month else 0
            row[10].value = valid_exercise_count
            row[11].value = valid_exercise_count_last_month
            row[12].value = valid_exercise_count_curr_month
            row[13].value = self.percentage(mom)
            summary_map[10].append(row[10].value)
            summary_map[11].append(row[11].value)
            summary_map[12].append(row[12].value)
            summary_map[13].append(mom)
            # 新增考试图片数量
            e_image_c_curr_month = school_data.get("e_image_c_curr_month", 0)
            e_image_c_last_month = school_data.get("e_image_c_last_month", 0)

            mom = (e_image_c_curr_month - e_image_c_last_month) / e_image_c_last_month \
                if e_image_c_last_month else 0
            row[14].value = e_image_c_last_month
            row[15].value = e_image_c_curr_month
            row[16].value = self.percentage(mom)
            summary_map[14].append(row[14].value)
            summary_map[15].append(row[15].value)
            summary_map[16].append(mom)
            # 新增单词数量
            valid_word_count = school_data.get("valid_word_count", 0)
            valid_word_count_curr_month = school_data.get("valid_word_count_curr_month", 0)
            valid_word_count_last_month = school_data.get("valid_word_count_last_month", 0)
            mom = (valid_word_count_curr_month - valid_word_count_last_month) / valid_word_count_last_month \
                if valid_word_count_last_month else 0
            row[17].value = valid_word_count
            row[18].value = valid_word_count_last_month
            row[19].value = valid_word_count_curr_month
            row[20].value = self.percentage(mom)
            summary_map[17].append(row[17].value)
            summary_map[18].append(row[18].value)
            summary_map[19].append(row[19].value)
            summary_map[20].append(mom)
            # 新增单词图像数量
            w_image_c = school_data.get("w_image_c", 0)
            w_image_c_curr_month = school_data.get("w_image_c_curr_month", 0)
            w_image_c_last_month = school_data.get("w_image_c_last_month", 0)
            mom = (w_image_c_curr_month - w_image_c_last_month) / w_image_c_last_month \
                if w_image_c_last_month else 0
            row[21].value = w_image_c_last_month
            row[22].value = w_image_c_curr_month
            row[23].value = self.percentage(mom)
            summary_map[21].append(row[21].value)
            summary_map[22].append(row[22].value)
            summary_map[23].append(mom)
            # 新增阅读数量
            valid_reading_count = school_data.get("valid_reading_count", 0)
            valid_reading_count_curr_month = school_data.get("valid_reading_count_curr_month", 0)
            valid_reading_count_last_month = school_data.get("valid_reading_count_last_month", 0)
            mom = (valid_reading_count_curr_month - valid_reading_count_last_month) / valid_reading_count_last_month \
                if valid_reading_count_last_month else 0
            row[24].value = valid_reading_count
            row[25].value = valid_reading_count_last_month
            row[26].value = valid_reading_count_curr_month
            row[27].value = self.percentage(mom)
            summary_map[24].append(row[24].value)
            summary_map[25].append(row[25].value)
            summary_map[26].append(row[26].value)
            summary_map[27].append(mom)
            # 新增家长数量
            total_guardian_number = school_data.get("total_guardian_number", 0)
            total_unique_guardian_number = school_data.get("total_unique_guardian_number", 0)
            guardian_number_curr_month = school_data.get("guardian_number_curr_month", 0)
            guardian_unique_number_curr_month = school_data.get("guardian_unique_number_curr_month", 0)
            guardian_number_last_month = school_data.get("guardian_number_last_month", 0)
            guardian_unique_number_last_month = school_data.get("guardian_unique_number_last_month", 0)
            mom = (guardian_unique_number_curr_month - guardian_unique_number_last_month) / guardian_unique_number_last_month if guardian_unique_number_last_month else 0
            avg = total_unique_guardian_number / total_student_number if total_student_number > 0 else 0
            row[28].value = self.percentage(avg)
            row[29].value = guardian_number_last_month
            row[30].value = guardian_number_curr_month
            row[31].value = self.percentage(mom)
            summary_map[28].append(avg)
            summary_map[29].append(row[29].value)
            summary_map[30].append(row[30].value)
            summary_map[31].append(mom)
            # 新增付费
            total_pay_amount = school_data.get("total_pay_amount", 0)
            pay_amount_curr_month = school_data.get("pay_amount_curr_month", 0)
            pay_amount_last_month = school_data.get("pay_amount_last_month", 0)
            mom = (pay_amount_curr_month - pay_amount_last_month) / pay_amount_last_month \
                if pay_amount_last_month else 0
            row[32].value = total_pay_amount
            row[33].value = pay_amount_last_month
            row[34].value = pay_amount_curr_month
            row[35].value = self.percentage(mom)
            summary_map[32].append(row[32].value)
            summary_map[33].append(row[33].value)
            summary_map[34].append(row[34].value)
            summary_map[35].append(mom)
            for one in row:
                if isinstance(one.value, (int, float)):
                    if one.value == 0:
                        one.font = self._red_font()

        total_offset = len(items) + 4
        divider = len(items)
        for index, cell in enumerate(sheet[total_offset]):
            if index == 0:
                cell.value = "总计"
                continue
            if index in (4, 8, 12, 16, 20, 23, 27, 30, 34, 35, 38, 42): #平均值
                cell.value = self.percentage(sum((summary_map.get(index, [0]))) / divider if divider > 0 else 0)
            else:
                cell.value = self.rounding(sum(summary_map.get(index,[0])))
        row = sheet[total_offset +3]
        # row[0].value = "分析"
        # notifications = self._analyze(area_dimesion_items, area_name_id_map, users, report_type)
        # for index, notify in enumerate(notifications):
        #
        #     row = sheet[total_offset + 4]
        #     row[0].value = notify['user'].get("area_info", {}).get("name")
        #     row[1].value = notify['info']


        with NamedTemporaryFile() as tmp:
            file.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()

        return stream