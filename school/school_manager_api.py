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

class SchoolManage(BaseHandler):
    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.school_coll = 'school'
        self.start_time = BaseTask().start_time

    async def get_school_list(self, request: Request):
        """
        学校列表
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 0))
        per_page = 30
        sql = "select count(id) as total_school_count from sigma_account_ob_school" \
              " where available = 1 and time_create >= '%s' " \
              "and time_create <= '%s' " % (self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"))
        school_page_sql = "select id,full_name  from sigma_account_ob_school" \
              " where available = 1 and time_create >= '%s' " \
              "and time_create <= '%s' limit %s,%s" % (self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"), per_page*page, per_page)
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                total_school = await cur.fetchall()
                await cur.execute(school_page_sql)
                schools = await cur.fetchall()
        total_school_count = total_school[0]['total_school_count']
        print(schools)




        return self.reply_ok({})
