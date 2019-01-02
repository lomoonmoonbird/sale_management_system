#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 自定义导出报表
"""

from datetime import datetime, timedelta, date
import asyncio
import mimetypes
import os
from tempfile import NamedTemporaryFile
from operator import itemgetter
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
from webargs import fields
from webargs.aiohttpparser import parser
from statistics.export.export_repository import ExportRepository
from tasks.celery_base import BaseTask

class FlexibleExport(BaseHandler, ExportBase, DataExcludeMixin):
    """
    自定义导出报表

    实体 (总部，大区，渠道，市场，学校，年级，班级，个人)

    报表类型 (年，季度，月，周，日)

    数据字段 ( 起止时间， 顺序 )

    """
    def __init__(self):
        self.db = "sales"
        self.instance_coll = "instance"
        self.exportrepository = ExportRepository()
        self.thread_pool = ThreadPoolExecutor(10)

    async def export_channel_daily(self, request: Request):
        """
        按时间导出渠道新增数据 可选择时间 可选择导出
        :param request:
        :return:
        """
        request_param = await get_params(request)
        area_name = request_param.get("area_name")
        channel_name = request_param.get("channel_name")

        begin_time = request_param.get("begin_time")
        end_time = request_param.get("end_time")

        #渠道 begin
        query = { "role": Roles.AREA.value, "status": 1}
        if area_name:
            query.update({"name": {"$in": area_name.split(',')}})

        areas = request.app['mongodb'][self.db][self.instance_coll].find(query)
        areas = await areas.to_list(None)
        area_ids = [str(item['_id']) for item in areas]
        # print(areas)
        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": {"$in": area_ids},
                                                                          "role": Roles.CHANNEL.value,
                                                                          "status": 1})
        channels = await channels.to_list(None)
        # print(channels)
        old_ids = [item['old_id'] for item in channels]


        area_map = {}
        for area in areas:
            area_map[str(area['_id'])] = area

        sql = "select id, name from sigma_account_us_user where available = 1 and id in (%s) " % \
              ','.join([str(id) for id in old_ids])

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                real_channels = await cur.fetchall()
        channels_map = {}
        for channel in channels:
            channels_map[channel['old_id']] = channel
        channel_mongo_map = {}
        for channel in real_channels:
            channel.update({"name": channel.get("name", ""),
                            "area_id": channels_map.get(channel['id'],{}).get("parent_id", ""),
                            "area_name": area_map.get(channels_map.get(channel['id'],{}).get("parent_id", "")).get("name","")})
            channel_mongo_map[channel['id']] = channel
        items = await self.exportrepository.channel_new_delta(request, old_ids, begin_time, end_time)
        #渠道 end
        #学校 begin
        sql = "select id, name from sigma_account_us_user where available = 1 and name in (%s) " % \
              ','.join(["'"+str(username)+"'" for username in channel_name.split(",")])
        print(sql)
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                real_channels = await cur.fetchall()

        channel_ids = [item['id'] for item in real_channels]

        school_items= []
        channel_mysql_map = {}
        school_mysql_map = {}
        if channel_ids:
            school_sql = "select id, full_name, owner_id " \
                         "from sigma_account_ob_school " \
                         "where available = 1 " \
                         "and owner_id in (%s)" % (','.join([str(id) for id in channel_ids]))
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(school_sql)
                    real_schools = await cur.fetchall()


            for channel in real_channels:
                channel_mysql_map[channel['id']] = channel
            print("real_channels", real_channels)
            for school in real_schools:
                school_mysql_map[school['id']] = school
            school_items = await self.exportrepository.school_new_delta(request, channel_ids, begin_time, end_time)
        print(json.dumps(school_items, indent=4))
        print('channel_mysql_map', channel_mysql_map)
        sheet = await request.app.loop.run_in_executor(self.thread_pool,
                                                       self.exportrepository.sheet,
                                                       items,
                                                       area_map,
                                                       channel_mongo_map, school_items,channel_mysql_map,school_mysql_map)
        return await self.replay_stream(sheet, "渠道和学校"+ begin_time +'|'+end_time + "周期新增数据", request)

    async def export_channel_pay_daily(self, request: Request):
        """
        学校每日新增
        {
            "area_name":""
            "begin_time":"",
            "end_time": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        area_name = request_param.get("area_name")

        begin_time = request_param.get("begin_time")
        end_time = request_param.get("end_time")

        # 渠道 begin
        query = {"role": Roles.AREA.value, "status": 1}
        if area_name:
            query.update({"name": {"$in": area_name.split(',')}})

        areas = request.app['mongodb'][self.db][self.instance_coll].find(query)
        areas = await areas.to_list(None)
        area_ids = [str(item['_id']) for item in areas]
        # print(areas)
        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": {"$in": area_ids},
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1})
        channels = await channels.to_list(None)
        # print(channels)
        old_ids = [item['old_id'] for item in channels]

        area_map = {}
        for area in areas:
            area_map[str(area['_id'])] = area

        sql = "select id, name from sigma_account_us_user where available = 1 and id in (%s) " % \
              ','.join([str(id) for id in old_ids])
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                real_channels = await cur.fetchall()
        channels_map = {}
        for channel in channels:
            channels_map[channel['old_id']] = channel
        channel_mongo_map = {}
        for channel in real_channels:
            channel.update({"name": channel.get("name", ""),
                            "area_id": channels_map.get(channel['id'], {}).get("parent_id", ""),
                            "area_name": area_map.get(channels_map.get(channel['id'], {}).get("parent_id", "")).get(
                                "name", "")})
            channel_mongo_map[channel['id']] = channel
        # print(old_ids)
        items = await self.exportrepository.daily_new_pay(request, old_ids, begin_time, end_time)
        # 渠道 end
        # print(json.dumps(items,indent=4))
        sheet = await request.app.loop.run_in_executor(self.thread_pool,
                                                       self.exportrepository.channel_daily_sheet,
                                                       items,
                                                       area_map,
                                                       channel_mongo_map,begin_time,end_time)


        if not begin_time:
            begin_time = BaseTask().start_time.strftime("%Y-%m-%d")
        if not end_time:
            end_time = datetime.now().strftime("%Y-%m-%d")
        return await self.replay_stream(sheet, "渠道" + begin_time + '|' + end_time + "周期每日付费", request)

