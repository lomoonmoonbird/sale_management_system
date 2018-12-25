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

class FlexibleExport(BaseHandler, ExportBase, DataExcludeMixin):
    """
    自定义导出报表

    实体 (总部，大区，渠道，市场，学校，年级，班级，个人)

    报表类型 (年，季度，月，周，日)

    数据字段 ( 起止时间， 顺序 )

    """

    async def test_arge(self, request):
        handler_args = {"name": fields.Str(missing="World")}
        args = await parser.parse(handler_args, request)
        return self.reply_ok({"name": args['name']})
