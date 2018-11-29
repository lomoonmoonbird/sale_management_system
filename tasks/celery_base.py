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
from bson import ObjectId
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime)):
            return str(obj)
        if isinstance(obj, (ObjectId)):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class BaseTask(Task):
    def __init__(self):
        super(BaseTask, self).__init__()
        self.start_time = datetime.datetime(2018,1,1)
        self.coll_time_threshold = 'sale_time_threshold'
        self.bulk_update = []
        self.total_dict = defaultdict(float)
        self.coll_total = "total"
        self.time_threshold_table = {}

        self.schoo_per_day_schema = {
            "teacher_counts": 0, #老师数
            "student_counts": 0, #学生数
            "guardian_counts": 0, #家长数
            "valid_exercise_student_counts": 0, #有效考试的学生数
            "valid_exercise_counts": 0, #有效考试数
            "exam_images": 0, #考试图片数
            "paid_counts": 0, #付费数
            "paid_amount": 0, #付费金额
            "school_id": 0, #学校id

        }


    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print (einfo)

    def on_success(self, retval, task_id, args, kwargs):
        print ('success')
        print(retval)
        print(task_id)
        print(args)
        print(kwargs)


    def get_connection(self):
        if self.connection:
            return self.connection
        if DEBUG:
            print("this is debug")
            server = SSHTunnelForwarder(
                ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

                ssh_password="PengKim@89527",
                ssh_username="jinpeng",
                remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
            server.start()
            connection = pymysql.connect(host="127.0.0.1",
                                         port=server.local_bind_port,
                                         user="sigma",
                                         password="sigmaLOVE2017",
                                         db=MYSQL_NAME,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
        else:
            connection = pymysql.connect(host=MYSQL_HOST,
                                         port=MYSQL_PORT,
                                         user=MYSQL_USER,
                                         password=MYSQL_PASSWORD,
                                         db=MYSQL_NAME,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
        return  connection

    def _date_range(self, field):
        self.time_threshold_table = self.mongo[self.coll_time_threshold].find_one({"_id": "sale_time_threshold"}) or {}

        start_date = self.time_threshold_table.get(field, self.start_time) if self.time_threshold_table.get(field, self.start_time) else self.start_time
        end_date = datetime.datetime.now()

        delta = end_date - start_date  # timedelta

        date_range = []
        for i in range(1, delta.days + 1):
            # print(datetime.datetime.strftime((start_date + timedelta(i)), "%Y-%m-%d"))
            date_range.append((datetime.datetime.strftime((start_date + timedelta(i-1)), "%Y-%m-%d"),
                               datetime.datetime.strftime((start_date + timedelta(i)), "%Y-%m-%d")))

        return date_range

    def _set_time_threadshold(self, field, value):
        self.mongo[self.coll_time_threshold].update({"_id": "sale_time_threshold"}, {"$set": {field: value}}, upsert=True)

