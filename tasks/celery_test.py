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
from tasks.celery_base import BaseTask, CustomEncoder

class TestTask(BaseTask):
    def __init__(self):
        super(TestTask, self).__init__()
        self.server =  SSHTunnelForwarder(
                ssh_address_or_host = ('139.196.77.128', 5318),  # B机器的配置

                ssh_password="PengKim@89527",
                ssh_username="jinpeng",
                remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
        self.server.start()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

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

        # self.connection = pymysql.connect(host='mysql.hexin.im',
        #                                   user='root',
        #                                   password='sigmalove',
        #                                   db='sigma_centauri_new',
        #                                   charset='utf8mb4',
        #                                   cursorclass=pymysql.cursors.DictCursor)

        cursor = self.connection.cursor()
        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        cursor.close()
        print ("_query reutrn .......")
        return ret


    def run(self):

        print ("run start ... ")
        q = select([us_user]).where(and_(us_user.c.available == 1,
                                         us_user.c.time_create > (datetime.datetime.now() - timedelta(1)).strftime("%Y-%m-%d"),
                                         us_user.c.time_create < (datetime.datetime.now()).strftime(
                                             "%Y-%m-%d")))

        # j =
        # count = 0
        # students = self._query(q)
        # for student in students:
        #     from tasks.celery_init import sales_celery
        #     sales_celery.send_task("tasks.celery_test.SubTestTask", args=[student])



        # print ("run stop ...", len(students))

class SubTestTask(BaseTask):
    def __init__(self):
        super(SubTestTask, self).__init__()
        self.server =  SSHTunnelForwarder(
                ssh_address_or_host = ('139.196.77.128', 5318),  # B机器的配置

                ssh_password="PengKim@89527",
                ssh_username="jinpeng",
                remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
        self.server.start()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


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
        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        cursor.close()
        print ("_query reutrn .......")
        return ret

    def run(self, student):

        q_q = select([ob_groupuser]).where(and_(ob_groupuser.c.user_id == student['id'], ob_groupuser.c.available == 1))
        student_group = self._query(q_q)
        print (student_group)

