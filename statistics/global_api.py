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
from enumconstant import Roles, PermissionRole


class Overview(BaseHandler):
    def __init__(self):
        pass

    @validate_permission()
    async def overview(self, request: Request):
        """
        总部数据总览
        :param request:
        :return:
        """
        pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number(request)

        total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number(request)

        teacher_total, student_total, teacher_curr_week_new_number, \
        teacher_last_week_new_number, student_curr_week_new_number, student_last_week_new_number = await self._teacher_student_number(request)

        image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number(request)

        guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number(request)

        return self.reply_ok({"pay_total": pay_total,
                              "pay_curr_week_new_number": pay_curr_week_new_number,
                              "pay_last_week_new_number": pay_last_week_new_number,
                              "total_school_number": total_school_number,
                              "curr_week_new_school_number": curr_week_new_school_number,
                              "last_week_new_school_number": last_week_new_school_number,
                              "total_teacher_number": teacher_total,
                              "teacher_curr_week_new_number": teacher_curr_week_new_number,
                              "teacher_last_week_new_number": teacher_last_week_new_number,
                              "student_total": student_total,
                              "student_curr_week_new_number": student_curr_week_new_number,
                              "student_last_week_new_number": student_last_week_new_number,
                              "image_total": image_total,
                              "image_curr_week_new_number": image_curr_week_new_number,
                              "image_last_week_new_number": image_last_week_new_number,
                              "guardian_total": guardian_total,
                              "guardian_curr_week_new_number": guardian_curr_week_new_number,
                              "guardian_last_week_new_number": guardian_last_week_new_number
                              })



    async def _guardian_number(self, request:Request):
        """
        家长数
        :param request:
        :return:
        """
        current_week = self.current_week()
        last_week = self.last_week()
        sql_guardian_total = "select count(id) as total from sigma_account_re_userwechat where available = 1 and relationship > 1"
        sql_guardian_curr_week_new_number = "select count(id) as total from sigma_account_re_userwechat where available = 1  and relationship > 1 and time_create >= '%s' and time_create < '%s'" % (
            current_week[0], current_week[1])
        sql_guardian_last_week_new_number = "select count(id) as total from sigma_account_re_userwechat where available = 1 and relationship > 1 and time_create >= '%s' and time_create < '%s'" % (
            last_week[0], last_week[1])

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql_guardian_total)
                guardian_total = await cur.fetchone()

                await cur.execute(sql_guardian_curr_week_new_number)
                guardian_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_guardian_last_week_new_number)
                guardian_last_week_new_number = await cur.fetchone()

        guardian_total = guardian_total['total']
        guardian_curr_week_new_number = guardian_curr_week_new_number['total']
        guardian_last_week_new_number = guardian_last_week_new_number['total']

        return guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number


    async def _images_number(self, request: Request):
        """
        图片数
        :param request:
        :return:
        """
        current_week = self.current_week()
        last_week = self.last_week()
        sql_image_total = "select count(id) as total from sigma_pool_as_hermes where available = 1"
        sql_image_curr_week_new_number = "select count(id) as total from sigma_pool_as_hermes where available = 1 and time_create >= '%s' and time_create < '%s'" % (
        current_week[0], current_week[1])
        sql_image_last_week_new_number = "select count(id) as total from sigma_pool_as_hermes where available = 1 and time_create >= '%s' and time_create < '%s'" % (
        last_week[0], last_week[1])

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql_image_total)
                image_total = await cur.fetchone()

                await cur.execute(sql_image_curr_week_new_number)
                image_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_image_last_week_new_number)
                image_last_week_new_number = await cur.fetchone()

        image_total = image_total['total']
        image_curr_week_new_number = image_curr_week_new_number['total']
        image_last_week_new_number = image_last_week_new_number['total']

        return image_total, image_curr_week_new_number, image_last_week_new_number



    async def _teacher_student_number(self, request: Request):
        """
        教师  学生数
        :param coll:
        :param request:
        :return:
        """
        current_week = self.current_week()
        last_week = self.last_week()
        sql_teacher_total = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 1"
        sql_student_total = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 2"
        sql_teacher_curr_week_new_number = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 1 and time_create >= '%s' and time_create < '%s'" % (current_week[0], current_week[6])
        sql_teacher_last_week_new_number = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 1 and time_create >= '%s' and time_create < '%s'" % (last_week[0], last_week[6])
        sql_student_curr_week_new_number = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 2 and time_create >= '%s' and time_create < '%s'" % (
        current_week[0], current_week[6])
        sql_student_last_week_new_number = "select count(id) as total from sigma_account_us_user where available = 1 and role_id = 2 and time_create >= '%s' and time_create < '%s'" % (
        last_week[0], last_week[6])

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql_teacher_total)
                teacher_total = await cur.fetchone()
                await cur.execute(sql_student_total)
                student_total = await cur.fetchone()

                await cur.execute(sql_teacher_curr_week_new_number)
                teacher_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_teacher_last_week_new_number)
                teacher_last_week_new_number = await cur.fetchone()

                await cur.execute(sql_student_curr_week_new_number)
                student_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_student_last_week_new_number)
                student_last_week_new_number = await cur.fetchone()


        teacher_total = teacher_total['total']
        student_total = student_total['total']
        teacher_curr_week_new_number = teacher_curr_week_new_number['total']
        teacher_last_week_new_number = teacher_last_week_new_number['total']
        student_curr_week_new_number = student_curr_week_new_number['total']
        student_last_week_new_number = student_last_week_new_number['total']



        return teacher_total, student_total, teacher_curr_week_new_number, \
               teacher_last_week_new_number, student_curr_week_new_number, student_last_week_new_number



    async def _school_number(self, request: Request):
        """
        学校数
        :param coll:
        :param request:
        :return:
        """
        current_week = self.current_week()
        last_week = self.last_week()
        sql_school_total = "select count(id) as total from sigma_account_ob_school where available = 1"
        sql_school_curr_week_new_number = "select count(id) as total from sigma_account_ob_school where available = 1 and time_create >= '%s' and time_create < '%s'" % (current_week[0], current_week[1])
        sql_school_last_week_new_number = "select count(id) as total from sigma_account_ob_school where available = 1 and time_create >= '%s' and time_create < '%s'" % (last_week[0], last_week[1])


        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql_school_total)
                school_total = await cur.fetchone()

                await cur.execute(sql_school_curr_week_new_number)
                school_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_school_last_week_new_number)
                school_last_week_new_number = await cur.fetchone()


        total_number = school_total['total']
        school_curr_week_new_number = school_curr_week_new_number['total']
        school_last_week_new_number = school_last_week_new_number['total']

        return total_number, school_curr_week_new_number, school_last_week_new_number


    async def _pay_number(self, request: Request):
        """
        付费数
        :param request:
        :return:
        """
        current_week = self.current_week()
        last_week = self.last_week()
        sql_pay_total = "select count(id) as total from sigma_pay_ob_order where available = 1 and status = 3"
        sql_pay_curr_week_new_number = "select count(id) as total from sigma_pay_ob_order where available = 1 and status = 3 and time_create >= '%s' and time_create < '%s'" % (
            current_week[0], current_week[1])
        sql_pay_last_week_new_number = "select count(id) as total from sigma_pay_ob_order where available = 1 and status = 3 and time_create >= '%s' and time_create < '%s'" % (
            last_week[0], last_week[1])

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql_pay_total)
                pay_total = await cur.fetchone()

                await cur.execute(sql_pay_curr_week_new_number)
                pay_curr_week_new_number = await cur.fetchone()

                await cur.execute(sql_pay_last_week_new_number)
                pay_last_week_new_number = await cur.fetchone()

        pay_total = pay_total['total']
        pay_curr_week_new_number = pay_curr_week_new_number['total']
        pay_last_week_new_number = pay_last_week_new_number['total']

        return pay_total, pay_curr_week_new_number, pay_last_week_new_number

    # async def _pay_amount(self, coll: Collection, request: Request):
    #     """
    #     付费数
    #     :param coll:
    #     :return:
    #     """
    #     current_week = self.current_week()
    #     last_week = self.last_week()
    #     print (current_week)
    #     sql = "select sum(coupon_amount) as total from sigma_pay_ob_order where available = 1 and `status` = 3"
    #     print(sql)
    #     async with request.app['mysql'].acquire() as conn:
    #         async with conn.cursor(DictCursor) as cur:
    #             await cur.execute(sql)
    #             res = await cur.fetchone()
    #     total_amount = res["total"]
    #     print (res, 'toital_amount')
    #     current_week_new_pay_amount =  coll.aggregate(
    #             [
    #                 {
    #                     "$match": {
    #                             "day":  {"$gte": current_week[0],
    #                                       "$lte": current_week[6]}
    #                 }
    #                 },
    #                 {
    #                     "$project": {
    #                         "class_pay_amount": 1,
    #                         "day": 1
    #                     }
    #                 },
    #
    #                 {"$group": {"_id": "$day",
    #                             "total": {"$sum": "$class_pay_amount"},
    #                             # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
    #                             }
    #                  },
    #
    #
    #             ])
    #
    #     last_week_new_pay_amount = coll.aggregate(
    #         [
    #             {
    #                 "$match": {
    #                     "day": {"$gte": last_week[0],
    #                             "$lte": last_week[6]}
    #                 }
    #             },
    #             {
    #                 "$project": {
    #                     "class_pay_amount": 1,
    #                     "day": 1
    #                 }
    #             },
    #
    #             {"$group": {"_id": "$day",
    #                         "total": {"$sum": "$class_pay_amount"},
    #                         # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
    #                         }
    #              },
    #
    #         ])
    #
    #     current_week_new_pay_amount_list = []
    #     last_week_new_pay_amount_list = []
    #     async for amount in current_week_new_pay_amount:
    #         current_week_new_pay_amount_list.append(amount)
    #
    #     async for amount in last_week_new_pay_amount:
    #         last_week_new_pay_amount_list.append(amount)
    #
    #     return total_amount, \
    #            sum(item['total'] for item in current_week_new_pay_amount_list),\
    #            sum(item['total'] for item in last_week_new_pay_amount_list)






    def current_week(self):

        #list(self._get_week(datetime.now().date()))
        return [d.isoformat() for d in self._get_week(datetime.now().date())]

    def last_week(self):
        return [d.isoformat() for d in self._get_week((datetime.now()-timedelta(7)).date())]

    def _get_week(self, date):
        one_day = timedelta(days=1)
        day_idx = (date.weekday()) % 7
        sunday = date - timedelta(days=day_idx)
        date = sunday
        for n in range(7):
            yield date
            date += one_day
