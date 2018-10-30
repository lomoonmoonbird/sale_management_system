#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, \
ob_groupuser, ob_exercise, as_hermes, ob_order, re_userwechat, Roles, StageEnum, StudentRelationEnum
import pymysql
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from sqlalchemy import select, func, asc, distinct, text
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.dialects import mysql
from configs import MONGODB_CONN_URL
from loggings import logger
import pickle
import time
import datetime
from datetime import timedelta
from collections import defaultdict
import json
from sshtunnel import SSHTunnelForwarder
from tasks.celery_base import BaseTask

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime)):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

class StudentTask(BaseTask):
    def __init__(self):
        super(StudentTask, self).__init__()
        # self.connection = pymysql.connect(host='mysql.hexin.im',
        #                                   user='root',
        #                                   password='sigmalove',
        #                                   db='sigma_centauri_new',
        #                                   charset='utf8mb4',
        #                                   cursorclass=pymysql.cursors.DictCursor)
        self.server = SSHTunnelForwarder(
            ssh_address_or_host=('139.196.77.128', 5318),  # B机器的配置

            ssh_password="PengKim@89527",
            ssh_username="jinpeng",
            remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
        self.server.start()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

        self.student_schema = {
            "school_id": 0,
            "class_uid": '',
            "grade_uid": '',
            "user_uid": '',
            "date": ''
        }

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.connection = pymysql.connect(host='127.0.0.1',
                                          port=self.server.local_bind_port,
                                          user='sigma',
                                          password='sigmaLOVE2017',
                                          db='sigma_centauri_new',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        cursor = self.connection.cursor()
        logger.debug(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        cursor.close()
        return ret

    def run(self):
        students = self._students()


    def _time_per_day(self):
        self.time_threshold_table = self.mongo[self.coll_time_threshold].find_one({"_id": "sale_time_threshold"}) or {}
        print("self.time_threshold_table", self.time_threshold_table)


    def _exam_images(self):
