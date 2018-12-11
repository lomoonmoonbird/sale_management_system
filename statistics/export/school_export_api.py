#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 学校专项导出
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


class SchoolExportReport(BaseHandler, ExportBase):
    """
    学校相关项导出
    """

    def __init__(self):
        self.thread_pool = ThreadPoolExecutor(10)

    async def contest_related(self, request: Request):
        """
        考试相关导出
        {
            "school_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        school_id = request_param.get("school_id")
        if not school_id:
            raise RequestError("school_id must not be empty")


