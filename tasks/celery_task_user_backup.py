#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列
同步不同角色的用户信息，信息包括学校，年级，班级
"""
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, \
ob_groupuser, ob_exercise, as_hermes, ob_order, re_userwechat, Roles, StageEnum, \
    StudentRelationEnum, ExerciseTypeEnum, ob_exercisemeta
import pymysql
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from sqlalchemy import select, func, asc, distinct, text, desc
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.dialects import mysql
from configs import MONGODB_CONN_URL
from loggings import logger
import pickle
import time
import datetime
from datetime import timedelta, date
from collections import defaultdict
import json
from sshtunnel import SSHTunnelForwarder
from tasks.celery_base import BaseTask, CustomEncoder
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

class UserTask(BaseTask):
    def __init__(self):
        super(UserTask, self).__init__()


        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

        # self.cursor = self.connection.cursor()

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



    def run(self):
        try:
            date_range = self._date_range("user_begin_time") #时间分段
            self._user_info(date_range) #用户基本信息
            self.server.stop()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)





    def _user_info(self, date_range):
        """
        用户id，用户角色，学校id，年级id，年级，班级id，班级，创建时间，当天日期
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_user_in_date = select([us_user])\
                .where(and_(us_user.c.available == 1,
                            us_user.c.role_id.in_((Roles.STUDENT.value, Roles.TEACHER.value)),
                            us_user.c.time_create >= one_date[0],
                            us_user.c.time_create < one_date[1])).\
                order_by(asc(us_user.c.time_create))

            users = self._query(q_user_in_date)
            bulk_update = []
            for user in users:
                join = ob_groupuser.join(ob_group,
                                         and_(ob_group.c.available == 1,
                                              ob_groupuser.c.available == 1,
                                              ob_groupuser.c.user_id == user['id'],
                                              ob_groupuser.c.group_id == ob_group.c.id) )
                q_group = select([join]).select_from(join)
                group_groupuser_per_user = self._query(q_group)

                user_schema = {
                    "user_id": user['id'],
                    "user_role": user['role_id'],
                    "school_id": user['school_id'],
                    "user_grade_id": group_groupuser_per_user[0]['group_id'] if group_groupuser_per_user else 0,
                    "user_grade": group_groupuser_per_user[0]['grade'] if group_groupuser_per_user else 0,
                    "user_class_id": group_groupuser_per_user[0]['group_id'] if group_groupuser_per_user else 0,
                    "user_class": group_groupuser_per_user[0]['name'] if group_groupuser_per_user else 0,
                    "user_time_create": user['time_create'],
                    "date_per_day": one_date[0]
                }
                bulk_update.append(UpdateOne({"_id": str(user['id'])}, {'$set': user_schema}, upsert=True))
            if bulk_update:
                try:
                    bulk_update_ret = self.mongo.users.bulk_write(bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
                self._set_time_threadshold("user_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))


class UserPerDayTask(BaseTask):
    def __init__(self):
        super(UserPerDayTask, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


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


    def run(self):
        try:
            date_range = self._date_range("user_exercise_images_per_day_begin_time") #时间分段
            self._exercise_images(date_range) #考试 单词图片数

            date_range = self._date_range("user_guardian_per_day_begin_time")  # 时间分段
            self._guardian_info(date_range) #家长数也为绑定数

            date_range = self._date_range("user_pay_per_day_begin_time")  # 时间分段
            self._pay_amount(date_range) #付费数 付费额
            self.server.stop()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)



    def _exercise_images(self, date_range):
        """
        考试图片数，单词图片数，用户id，当天日期
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_exercise_images = select([
                as_hermes.c.student_id,
                as_hermes.c.exercise_id,
                func.count(as_hermes.c.id).label("image_counts"),
                as_hermes.c.time_create]) \
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.time_create >= one_date[0],
                            as_hermes.c.time_create < one_date[1])) \
                .group_by(func.date_format(as_hermes.c.time_create, "%Y-%m-%d")) \
                .group_by(as_hermes.c.student_id)

            q_exercise_images = self._query(q_exercise_images)

            def chunks(l, n):
                for i in range(0, len(l), n):
                    yield l[i:i + n]

            for chunk in list(chunks(q_exercise_images, 3000)):
                bulk_update = []
                for c in chunk:
                    q_exercise_meta = select([ob_exercisemeta]).where(and_(
                        ob_exercisemeta.c.available == 1,
                        ob_exercisemeta.c.exercise_id == c['exercise_id'],
                        ob_exercisemeta.c.key == 'category',
                        ob_exercisemeta.c.value == '"word"'
                    ))
                    exercise_meta = self._query(q_exercise_meta)
                    exercise_type = ExerciseTypeEnum.normal_exercise.value


                    user_per_day_schema = {
                        "user_id": c['student_id'],
                        # "e_image_counts": 0,
                        # "r_image_counts": 0,
                        # "image_type": exercise_type,

                        "image_time": datetime.datetime.strftime(c['time_create'], "%Y-%m-%d"),
                        # "date_per_day": one_date[0]
                    }
                    if exercise_meta:
                        exercise_type = ExerciseTypeEnum.word_exercise.value
                        user_per_day_schema['r_image_counts'] = c['image_counts']
                        bulk_update.append(
                            UpdateOne({"user_id": c['student_id'], "date_per_day": one_date[0]},
                                      {'$set': user_per_day_schema}, upsert=True))
                    else:
                        user_per_day_schema['e_image_counts'] = c['image_counts']
                        bulk_update.append(
                            UpdateOne({"user_id": c['student_id'], "date_per_day": one_date[0]},
                                      {'$set': user_per_day_schema}, upsert=True))

                if bulk_update:
                    try:
                        bulk_update_ret = self.mongo.user_per_day.bulk_write(bulk_update)
                        # print(bulk_update_ret.bulk_api_result)
                    except BulkWriteError as bwe:
                        print(bwe.details)
                self._set_time_threadshold("user_exercise_images_per_day_begin_time",
                                           datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _guardian_info(self, date_range):
        """
        家长数
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_guardian_counts = select([
                re_userwechat.c.user_id,
                re_userwechat.c.time_create,
                func.count(re_userwechat.c.wechat_id).label("guardian_counts"),
                func.date_format(re_userwechat.c.time_create, "%Y-%m-%d")
            ], and_(
                re_userwechat.c.available == 1,
                re_userwechat.c.relationship > StudentRelationEnum.sich.value,
                re_userwechat.c.time_create >= one_date[0],
                re_userwechat.c.time_create < one_date[1]
            )
            ) \
                .group_by(func.date_format(re_userwechat.c.time_create, "%Y-%m-%d")) \
                .group_by(re_userwechat.c.user_id) \
                .order_by(asc(re_userwechat.c.time_create))

            guardian_counts = self._query(q_guardian_counts)

            bulk_update = []
            for g_c in guardian_counts:
                guardian_count_schema = {
                    "guardian_counts": g_c['guardian_counts'],

                }
                bulk_update.append(UpdateOne({"user_id": g_c['user_id'], "date_per_day": one_date[0]},
                                             {'$set': guardian_count_schema}, upsert=True))

            if bulk_update:
                try:
                    bulk_update_ret = self.mongo.user_per_day.bulk_write(bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("user_guardian_per_day_begin_time",
                                           datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _pay_amount(self, date_range):
        """
        付费数，付费额
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_pay_amount = select([ob_order.c.user_id,
                                   func.count(ob_order.c.id).label("pay_number"),
                                   func.count(ob_order.c.coupon_amount).label("pay_amount")])\
                .where(and_(ob_order.c.available == 1,
                            ob_order.c.status > 3,
                            ob_order.c.time_create > one_date[0],
                            ob_order.c.time_create < one_date[1])
                       )\
                .group_by(func.date_format(ob_order.c.time_create, "%Y-%m-%d"))\
                .group_by(ob_order.c.user_id).order_by(asc(ob_order.c.time_create))

            pay_amount = self._query(q_pay_amount)

            bulk_update = []
            for p_a in pay_amount:
                pay_amount_schema = {
                    "pay_number": p_a['pay_number'],
                    "pay_amount": p_a['pay_amount']
                }
                bulk_update.append(UpdateOne({"user_id": p_a['user_id'], "date_per_day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))
            if bulk_update:
                try:
                    bulk_update_ret = self.mongo.user_per_day.bulk_write(bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("user_pay_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))


