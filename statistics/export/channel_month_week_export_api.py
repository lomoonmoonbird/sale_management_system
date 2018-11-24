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


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime)):
            return str(obj)
        if isinstance(obj, (ObjectId)):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

class ChannelExportReport(BaseHandler):
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
        print(request_param)
        if not channel_id:
            return self.reply_ok({})

        schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id, "role": Roles.SCHOOL.value,"status": 1})
        schools = await schools.to_list(10000)
        schools_ids = [item['school_id'] for item in schools]
        print(schools_ids)

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
            print(school_map)
            items = await self._list_month(request, schools_ids)
            print(json.dumps(items, indent=4, cls=CustomEncoder))
            template_path = os.path.dirname(__file__) + "/templates/channel_month_template.xlsx"
            sheet = await request.app.loop.run_in_executor(self.thread_pool,
                                                           self.sheet,
                                                           template_path,
                                                           items,
                                                           school_map,
                                                           "month")

            return await self.replay_stream(sheet, "渠道月报-" + datetime.now().strftime("%Y-%m-%d"), request)

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
                        "pay_number": 1,
                        "pay_amount": 1,
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
                            "$gte": ["$day", last_month_first_day]}]}, "$guardian_number", 0]},
                        "guardian_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$guardian_number", 0]},

                        "pay_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$pay_number", 0]},
                        "pay_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$pay_number", 0]},

                        "pay_amount_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$pay_amount", 0]},
                        "pay_amount_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$pay_amount", 0]},

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
                            "valid_exercise_count": {"$sum": "$valid_exercise_count"},
                            "valid_word_count": {"$sum": "$valid_word_count"},
                            "e_image_c": {"$sum": "$e_image_c"},
                            "w_image_c": {"$sum": "$w_image_c"},
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
                            "teacher_number_curr_month": {"$sum": "$teacher_number_curr_month"},
                            "teacher_number_last_month": {"$sum": "$teacher_number_last_month"},
                            "student_number_curr_month": {"$sum": "$student_number_curr_month"},
                            "student_number_last_month": {"$sum": "$student_number_last_month"},
                            "guardian_number_curr_month": {"$sum": "$guardian_number_curr_month"},
                            "guardian_number_last_month": {"$sum": "$guardian_number_last_month"},
                            "pay_number_curr_month": {"$sum": "$pay_number_curr_month"},
                            "pay_number_last_month": {"$sum": "$pay_number_last_month"},
                            "pay_amount_curr_month": {"$sum": "$pay_amount_curr_month"},
                            "pay_amount_last_month": {"$sum": "$pay_amount_last_month"},
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
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
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
                        "pay_number_curr_month": 1,
                        "pay_number_last_month": 1,
                        "pay_amount_curr_month": 1,
                        "pay_amount_last_month": 1,
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


        for index, school_data in enumerate(items):
            row = sheet[index + 4]
            index += 1
            # 大区名字
            row[0].value = school_map.get(school_data.get("_id", ""), {}).get("full_name", "")
            area_data = area_dimesion_items.get(area_name)
            #新增地市
            mom = (sum(area_data['city_number_curr_month']) - sum(area_data['city_number_last_month'])) / sum(
                area_data['city_number_last_month']) \
                if sum(area_data['city_number_last_month']) else 0
            row[1].value = sum(area_data['total_city_number'])
            row[2].value = sum(area_data['city_number_last_month'])
            row[3].value = sum(area_data['city_number_curr_month'])
            row[4].value = self.percentage(mom)
            summary_map[1].append(row[1].value)
            summary_map[2].append(row[2].value)
            summary_map[3].append(row[3].value)
            summary_map[4].append(mom)
            # 新增学校
            mom = (sum(area_data['school_number_curr_month']) - sum(area_data['school_number_last_month']))/sum(area_data['school_number_last_month']) \
                if sum(area_data['school_number_last_month']) else 0
            row[5].value = sum(area_data['total_school_number'])
            row[6].value = sum(area_data['school_number_last_month'])
            row[7].value = sum(area_data['school_number_curr_month'])
            row[8].value = self.percentage(mom)
            summary_map[5].append(row[5].value)
            summary_map[6].append(row[6].value)
            summary_map[7].append(row[7].value)
            summary_map[8].append(mom)

            # 新增教师数量
            mom = (sum(area_data['teacher_number_curr_month']) - sum(area_data['teacher_number_last_month'])) / sum(area_data[
                'teacher_number_last_month']) \
                if sum(area_data['teacher_number_last_month']) else 0
            row[9].value = sum(area_data['total_teacher_number'])
            row[10].value = sum(area_data['teacher_number_last_month'])
            row[11].value = sum(area_data['teacher_number_curr_month'])
            row[12].value = self.percentage(mom)
            summary_map[9].append(row[9].value)
            summary_map[10].append(row[10].value)
            summary_map[11].append(row[11].value)
            summary_map[12].append(mom)

            # 新增学生数量
            mom = (sum(area_data['student_number_curr_month']) - sum(area_data['student_number_last_month'])) / sum(area_data[
                'student_number_last_month']) \
                if sum(area_data['student_number_last_month']) else 0
            row[13].value = sum(area_data['total_student_number'])
            row[14].value = sum(area_data['student_number_last_month'])
            row[15].value = sum(area_data['student_number_curr_month'])
            row[16].value = self.percentage(mom)
            summary_map[13].append(row[13].value)
            summary_map[14].append(row[14].value)
            summary_map[15].append(row[15].value)
            summary_map[16].append(mom)
            # 新增考试数量
            mom = (sum(area_data['valid_exercise_count_curr_month']) - sum(area_data['valid_exercise_count_last_month'])) / sum(area_data[
                'valid_exercise_count_last_month']) \
                if sum(area_data['valid_exercise_count_last_month']) else 0
            row[17].value = sum(area_data['valid_exercise_count'])
            row[18].value = sum(area_data['valid_exercise_count_last_month'])
            row[19].value = sum(area_data['valid_exercise_count_curr_month'])
            row[20].value = self.percentage(mom)
            summary_map[17].append(row[17].value)
            summary_map[18].append(row[18].value)
            summary_map[19].append(row[19].value)
            summary_map[20].append(mom)
            # 新增考试图片数量
            mom = (sum(area_data['e_image_c_curr_month']) - sum(area_data['e_image_c_last_month'])) / sum(area_data[
                'e_image_c_last_month']) \
                if sum(area_data['e_image_c_last_month']) else 0
            row[21].value = sum(area_data['e_image_c_last_month'])
            row[22].value = sum(area_data['e_image_c_curr_month'])
            row[23].value = self.percentage(mom)
            summary_map[21].append(row[21].value)
            summary_map[22].append(row[22].value)
            summary_map[23].append(mom)
            # 新增单词数量
            mom = (sum(area_data['valid_word_count_curr_month']) - sum(area_data['valid_word_count_last_month'])) / sum(area_data[
                'valid_word_count_last_month']) \
                if sum(area_data['valid_word_count_last_month']) else 0
            row[24].value = sum(area_data['valid_word_count'])
            row[25].value = sum(area_data['valid_word_count_last_month'])
            row[26].value = sum(area_data['valid_word_count_curr_month'])
            row[27].value = self.percentage(mom)
            summary_map[24].append(row[24].value)
            summary_map[25].append(row[25].value)
            summary_map[26].append(row[26].value)
            summary_map[27].append(mom)
            # 新增单词图像数量
            mom = (sum(area_data['w_image_c_curr_month']) - sum(area_data['w_image_c_last_month'])) / sum(area_data[
                'w_image_c_last_month']) \
                if sum(area_data['w_image_c_last_month']) else 0
            row[27].value = sum(area_data['w_image_c'])
            row[28].value = sum(area_data['w_image_c_last_month'])
            row[29].value = sum(area_data['w_image_c_curr_month'])
            row[30].value = self.percentage(mom)
            summary_map[27].append(row[27].value)
            summary_map[28].append(row[28].value)
            summary_map[29].append(row[29].value)
            summary_map[30].append(mom)
            # 新增阅读数量
            row[31].value = "暂无功能"
            row[32].value = "暂无功能"
            row[33].value = "暂无功能"
            row[34].value = "暂无功能"
            summary_map[31].append(0)
            summary_map[32].append(0)
            summary_map[33].append(0)
            summary_map[34].append(0)
            # 新增家长数量
            mom = (sum(area_data['guardian_number_curr_month']) - sum(area_data['guardian_number_last_month'])) / sum(area_data[
                'guardian_number_last_month']) if sum(area_data['guardian_number_last_month']) else 0
            avg = sum(area_data['total_guardian_number']) / sum(area_data['total_student_number']) if sum(area_data['total_student_number']) > 0 else 0
            row[35].value = self.percentage(avg)
            row[36].value = sum(area_data['guardian_number_last_month'])
            row[37].value = sum(area_data['guardian_number_curr_month'])
            row[38].value = self.percentage(mom)
            summary_map[35].append(avg)
            summary_map[36].append(row[36].value)
            summary_map[37].append(row[37].value)
            summary_map[38].append(mom)
            # 新增付费
            mom = (sum(area_data['pay_amount_curr_month']) - sum(area_data['pay_amount_last_month'])) / sum(area_data[
                                                                                                              'pay_amount_last_month']) \
                if sum(area_data['pay_amount_last_month']) else 0
            row[39].value = sum(area_data['total_pay_amount'])
            row[40].value = sum(area_data['pay_amount_last_month'])
            row[41].value = sum(area_data['pay_amount_curr_month'])
            row[42].value = self.percentage(mom)
            summary_map[39].append(row[39].value)
            summary_map[40].append(row[40].value)
            summary_map[41].append(row[41].value)
            summary_map[42].append(mom)
            for one in row:
                if isinstance(one.value, (int, float)):
                    if one.value == 0:
                        one.font = self._red_font()

        total_offset = len(area_name_id_map) + 4
        divider = len(area_name_id_map)
        for index, cell in enumerate(sheet[total_offset]):
            if index == 0:
                cell.value = "总计"
                continue
            if index in (4, 8, 12, 16, 20, 23, 27, 30, 34, 35, 38, 42): #平均值
                cell.value = self.percentage(sum((summary_map.get(index, [0]))) / divider if divider > 0 else 0)
            else:
                cell.value = self.rounding(sum(summary_map.get(index,[0])))
        row = sheet[total_offset +3]
        row[0].value = "分析"
        notifications = self._analyze(area_dimesion_items, area_name_id_map, users, report_type)
        for index, notify in enumerate(notifications):

            row = sheet[total_offset + 4]
            row[0].value = notify['user'].get("area_info", {}).get("name")
            row[1].value = notify['info']


        with NamedTemporaryFile() as tmp:
            file.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()

        return stream