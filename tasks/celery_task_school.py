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
from pymongo.errors import BulkWriteError
from celery.signals import worker_process_init,worker_process_shutdown

connection = None

@worker_process_init.connect
def init_worker(**kwargs):
    global connection
    print('Initializing database connection for worker.')
    server = SSHTunnelForwarder(
        ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

        ssh_password="PengKim@89527",
        ssh_username="jinpeng",
        remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
    server.start()
    connection = pymysql.connect(host='127.0.0.1',
                                      port=server.local_bind_port,
                                      user='sigma',
                                      password='sigmaLOVE2017',
                                      db='sigma_centauri_new',
                                      charset='utf8mb4',
                                      cursorclass=pymysql.cursors.DictCursor)
@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global connection
    if connection:
        print('Closing database connectionn for worker.')
        connection.close()

class PerDayTask_SCHOOL_NUMBER(BaseTask):
    """
    学校维度的教师数，学生数，家长数，一天有效考试数，一天每次有效考试的学生数，一天考试图像上传数，一天有效词汇图像上传数，一天有效阅读数，一天付费数,一天付费额
    """

    def __init__(self):
        super(PerDayTask_SCHOOL_NUMBER, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


    def run(self):
        date_range = self._date_range("school_number_per_day_begin_time")  # 时间分段
        self._school_number(date_range)


    def _query(self, query):
        """
        执行查询 返回数据库结果
        """

        cursor = connection.cursor()
        logger.debug(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        return ret


    def _school_number(self, date_range)->list:
        """
        每天学校数量
        """
        for one_date in date_range:
            q_schools = select([ob_school]).where(and_(ob_school.c.available==1,
                                                       ob_school.c.time_create >= one_date[0],
                                                       ob_school.c.time_create < one_date[1]))
            schools = self._query(q_schools)

            bulk_update = []

            school_per_day_schema = {
                "count": len(schools)
            }
            bulk_update.append(UpdateOne({"day": one_date[0]}, \
                                             {'$set': school_per_day_schema}, upsert=True))
            if bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("school_number_per_day_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

