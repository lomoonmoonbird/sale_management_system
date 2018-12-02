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
from utils import CustomEncoder
from models.mysql.centauri import StageEnum
from collections import defaultdict

class SchoolManage(BaseHandler):
    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.school_coll = 'school'
        self.grade_coll = 'grade'
        self.school_per_day_coll = 'school_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.start_time = BaseTask().start_time

    @validate_permission()
    async def get_school_list(self, request: Request):
        """
        学校列表
        {
            "page":"",
            "school_name":"",
            "stage": "",
            "open_time_range":
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 0))
        per_page = 30

        school_page_sql = ''
        total_sql = ''
        total_school_count = 0
        if not request_param.get('school_name') and not request_param.get('stage') and not request_param.get('open_time_range'): #全部
            school_page_sql = "select id,full_name, time_create  from sigma_account_ob_school" \
                  " where available = 1 and time_create >= '%s' " \
                  "and time_create <= '%s' limit %s,%s" % (self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"), per_page*page, per_page)
            total_sql = "select count(id) as total_school_count from sigma_account_ob_school" \
                        " where available = 1 and time_create >= '%s' " \
                        "and time_create <= '%s' " % (
                            self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"))

        elif request_param.get('school_name'): #单个学校
            school_page_sql = "select id,full_name, time_create  from sigma_account_ob_school" \
                              " where available = 1 and full_name like %s" % ("'%"+request_param['school_name'] +"%'")

        elif not request_param.get('school_name'):
            stage = [StageEnum.Register.value, StageEnum.Using.value, StageEnum.Binding.value, StageEnum.Pay.value]
            request_stage = request_param.get('stage', -1)
            query = {

            }
            if not request_stage:
                request_stage = -1
            if request_stage != -1 and int(request_stage) not in stage:
                query["stage"] = {"$in": stage}
            elif request_stage != -1 and int(request_stage) in stage:
                query["stage"] = int(request_stage)
            else:
                pass
            date_range = request_param.get('open_time_range', '')

            if date_range:
                date_range = request_param.get('open_time_range').split(',')
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"), "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            else:
                date_range = [self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")]
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"), "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            condition_schools = request.app['mongodb'][self.db][self.school_coll].find(query).skip(per_page*page).limit(per_page)
            condition_schools = await condition_schools.to_list(10000)
            if not condition_schools:
                return self.reply_ok({})
            condition_school_ids = [item['school_id'] for item in condition_schools]
            school_page_sql = "select id,full_name, time_create  from sigma_account_ob_school" \
                              " where available = 1 and id in (%s) limit %s,%s" % (','.join(['"'+str(id)+'"' for id in condition_school_ids]), per_page*page, per_page)
            total_sql = "select count(id) as total_school_count from sigma_account_ob_school" \
                        " where available = 1 and time_create >= '%s' " \
                        "and time_create <= '%s' and id in (%s) " % (
                date_range[0], date_range[1],','.join(['"'+str(id)+'"' for id in condition_school_ids]))
        else:
            pass
        total_school_count = 1
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                if total_sql:
                    await cur.execute(total_sql)
                    total_school = await cur.fetchall()
                    total_school_count = total_school[0]['total_school_count']
                await cur.execute(school_page_sql)
                schools = await cur.fetchall()
        school_ids = [item['id'] for item in schools]
        grades = []
        if school_ids:
            grade_sql = "select grade, school_id, time_create from sigma_account_ob_group where available = 1 and school_id in (%s) group by school_id, grade" % ",".join([str(id) for id in school_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(grade_sql)
                    grades = await cur.fetchall()

        stage_school = request.app['mongodb'][self.db][self.school_coll].find({"school_id": {"$in": school_ids}})
        stage_school = await stage_school.to_list(10000)


        stage_grade = request.app['mongodb'][self.db][self.grade_coll].find({"school_id": {"$in": school_ids}})
        stage_grade = await stage_grade.to_list(10000)

        stage_grade_union_map = {}
        for s_g in stage_grade:
            stage_grade_union_map[str(s_g['school_id']) + "@" + s_g['grade']] = s_g

        stage_grade_union_map2 = {}
        school_grade_union_defaultdict = defaultdict(list)
        for grade in grades:
            union_id = str(grade['school_id']) + "@" + grade['grade']
            default = {
                "stage": StageEnum.Register.value,
                "using_time": 0
            }
            grade['school_grade'] = union_id
            grade.update(stage_grade_union_map.get(union_id, default))
            stage_grade_union_map2[union_id] = grade
            school_grade_union_defaultdict[grade['school_id']].append(union_id)

        school_item = await self._stat_school(request, school_ids)
        school_item_map = {}
        for s_i in school_item:
            school_item_map[s_i['_id']] = s_i
        grade_item = await self._stat_grade(request, school_ids)
        grade_item_map = {}
        for g_i in grade_item:
            grade_item_map[str(g_i['_id']['school_id']) + "@" + g_i['_id']['grade']] = g_i

        for school in schools:
            default = {
            "total_teacher_number": 0,
            "total_student_number": 0,
            "total_guardian_number": 0,
            "total_pay_number": 0,
            "total_pay_amount": 0,
            "total_valid_reading_number": 0,
            "total_valid_exercise_number": 0,
            "total_valid_word_number": 0,
            "total_exercise_image_number": 0,
            "total_word_image_number": 0,
            "pay_ratio": 0.0,
            "bind_ratio": 0.0
             }
            school['stat_info'] = school_item_map.get(school['id'], default)
            school['grade_info'] = []
            stage = []
            for school_grade in school_grade_union_defaultdict.get(school['id'], []):
                g_info = grade_item_map.get(school_grade, {})
                if g_info:
                    g_info.update(stage_grade_union_map2.get(school_grade, {}))
                    school['grade_info'].append(g_info)
                    stage.append(stage_grade_union_map2.get(school_grade, {}).get("stage", StageEnum.Register.value))

            school['stage'] = StageEnum.Register.value if not stage else min(stage)
        return self.reply_ok({"school_list": schools, "extra": {"total":total_school_count, "number_per_page": per_page, "curr_page": page}})

    @validate_permission()
    async def update_grade_stage(self, request: Request):
        """
        更新年级状态
        {
            "school_id": "",
            "grade": "",
            "stage":"",
            "begin_time": ""
        }
        :param request:
        :return:
        """
        request_data = await get_json(request)
        school_id = int(request_data.get("school_id"))
        grade = request_data.get("grade")
        stage = int(request_data.get("stage"))
        begin_time = request_data.get("begin_time")
        if not school_id or not grade or not stage:
            raise RequestError("paramter should not be empty")
        if stage in [StageEnum.Binding.value, StageEnum.Pay.value]:
            grade_of_school_sql = "select grade, school_id, time_create from sigma_account_ob_group where available = 1 and school_id = %s group by school_id, grade" % school_id
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(grade_of_school_sql)
                    school_grade = await cur.fetchall()

            school_grade_mongo = request.app['mongodb'][self.db][self.grade_coll].find({"school_id": school_id})
            school_grade_mongo = await school_grade_mongo.to_list(10000)
            school_grade_mongo_map = {}
            for s_g_m in school_grade_mongo:
                school_grade_mongo_map[s_g_m['grade']] = s_g_m['stage']

            for s_g in school_grade:
                s_g['stage'] = school_grade_mongo_map.get(s_g['grade'], StageEnum.Register.value)
            school_stage = min([item['stage'] for item in school_grade]) if school_grade else StageEnum.Register.value

            if stage == StageEnum.Pay.value:

                await request.app['mongodb'][self.db][self.school_coll].update_one({"school_id": school_id,
                                                                                    "stage": {"$in": [StageEnum.Binding.value,
                                                                                                      StageEnum.Pay.value,
                                                                                                      StageEnum.Using.value]}},
                                                                                   {"$set": {"stage": stage, "pay_time": begin_time}})
                await request.app['mongodb'][self.db][self.grade_coll].update_one(
                    {"school_id": school_id, "grade": grade},
                    {"$set": {"stage": stage,
                              "pay_time": begin_time}})
            elif stage == StageEnum.Binding.value:
                await request.app['mongodb'][self.db][self.school_coll].update_one({"school_id": school_id,
                                                                                    "stage": {
                                                                                        "$in": [StageEnum.Binding.value,
                                                                                                StageEnum.Pay.value,
                                                                                                StageEnum.Using.value]}},
                                                                                   {"$set": {"stage": stage,
                                                                                             "binding_time": begin_time}})

                await request.app['mongodb'][self.db][self.grade_coll].update_one({"school_id": school_id, "grade": grade},
                                                                           {"$set": {"stage": stage, "binding_time": begin_time}})
        else:
            raise RequestError("not support binding type")
        return self.reply_ok({})

    async def _stat_grade(self, request: Request, school_id):
        """
        年级统计
        """
        coll = request.app['mongodb'][self.db][self.grade_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_id}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
                        "grade": 1,
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

                {"$group": {"_id": {"school_id": "$school_id", "grade": "$grade"},
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

    async def _stat_school(self, request: Request, school_id):
        """
        学校统计
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
                        "school_id": {"$in": school_id}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
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

                {"$group": {"_id": "$school_id",
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
