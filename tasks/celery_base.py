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



class BaseTask(Task):
    def __init__(self):
        super(BaseTask, self).__init__()
        self.start_time = datetime.datetime(2015,1,1)
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

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        try:
            with self.connection.cursor() as cursor:
                # logger.debug(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string.replace("%%","%"))
                cursor.execute(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string.replace("%%","%"))
                ret = cursor.fetchall()
                cursor.close()
        except:
            import traceback
            traceback.print_exc()
            ret = []
        return ret
        # cursor = self.connection.cursor()
        # cursor.execute(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        # ret = cursor.fetchall()
        # cursor.close()
        # return ret

    def _school(self)->list:
        """
        正序获取学校记录
        如果有记录最后时间则从最后时间开始到前一天，否则从默认开始时间到当前时间前一天
        """
        print ("schooll..............")
        t = self.time_threshold_table.get("school_begin_time", self.start_time)
        q_schools = select([ob_school]).where(and_(ob_school.c.available==1,
                                    ob_school.c.time_create > t.strftime("%Y-%m-%d"))).\
                                    order_by(asc(ob_school.c.time_create))
        schools = self._query(q_schools)
        print ("return school..........")
        return schools