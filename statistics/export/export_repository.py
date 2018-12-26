#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 导出仓库方法
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

class ExportRepository(BaseHandler, ExportBase, DataExcludeMixin):
    """
    导出仓库方法
    """

    def __init__(self):
        self.db = "sales"
        self.channel_per_day_coll = "channel_per_day"


    async def daily_new_pay(self, request, channel_ids=[]):
        """
        每日新增付费
        :param request:
        :param channel_ids:
        :return:
        """
        last_week = self.last_week_from_7_to_6()
        print(last_week)
        items = []
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]

        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "channel": {"$in": channel_ids},
                        "day": {"$gte": "2018-01-01", "$lte": last_week[0]}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "day": 1,
                        "pay_amount": "$pay_amount"
                    }
                },

                {"$group": {"_id": {"day": "$day", "channel": "$channel"},
                            "total_pay_amount_day": {"$sum": "$pay_amount"},
                            }
                },
                {
                    "$project": {
                        "total_pay_amount_day": 1,

                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items