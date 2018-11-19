#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列
同步测评，包括考试，单词，阅读
"""
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, \
ob_groupuser, ob_exercise, as_hermes, ob_order, re_userwechat, Roles, StageEnum, StudentRelationEnum, \
    ob_exercisemeta, ExerciseTypeEnum
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


class ContestTask(BaseTask):
    def __init__(self):
        super(ContestTask, self).__init__()
        self.server =  SSHTunnelForwarder(
                ssh_address_or_host = ('139.196.77.128', 5318),  # 跳板机

                ssh_password="PengKim@89527",
                ssh_username="jinpeng",
                remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
        self.server.start()
        self.connection = pymysql.connect(host='127.0.0.1',
                                          port=self.server.local_bind_port,
                                          user='sigma',
                                          password='sigmaLOVE2017',
                                          db='sigma_centauri_new',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()
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
            date_range = self._date_range("exercise_begin_time") #时间分段
            self._normal_exercise(date_range) #有效考试
            self.server.stop()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)





    def _normal_exercise(self, date_range):
        """
        有效考试，有效单词考试， 有效考试学生数，考试日期，考试创建时间
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_exercise_in_date = select([ob_exercise])\
                .where(and_(ob_exercise.c.available == 1,
                            ob_exercise.c.time_create >= one_date[0],
                            ob_exercise.c.time_create < one_date[1]
                            )
                       ).\
                order_by(asc(ob_exercise.c.time_create))

            exercises = self._query(q_exercise_in_date)
            bulk_update = []
            for exercise in exercises:
                q_students_count_per_day_per_exercise = select([as_hermes.c.exercise_id,
                                                           as_hermes.c.student_id,
                                                           as_hermes.c.time_create,
                                                           func.count(distinct(as_hermes.c.student_id)).label("student_counts")
                                                           ])\
                    .where(
                        and_(as_hermes.c.available==1,
                             as_hermes.c.exercise_id == exercise['id']
                             )
                    ).group_by(func.date_format(as_hermes.c.time_create, "%Y-%m-%d"))\
                    .group_by(as_hermes.c.student_id).order_by(asc(as_hermes.c.student_id))\
                    # .having(text("student_counts > 10"))

                #考试元数据
                q_exercise_meta = select([ob_exercisemeta]).where(and_(
                    ob_exercisemeta.c.available == 1,
                    ob_exercisemeta.c.exercise_id == exercise['id'],
                    ob_exercisemeta.c.key == 'category',
                    ob_exercisemeta.c.value == '"word"'
                ))
                exercise_meta = self._query(q_exercise_meta)
                exercise_type = ExerciseTypeEnum.normal_exercise.value
                if exercise_meta:
                    exercise_type = ExerciseTypeEnum.word_exercise.value


                counts = self._query(q_students_count_per_day_per_exercise)
                total_students = sum([ k['student_counts'] for k in counts ])

                #考试所在学校 年级
                join = ob_groupuser.join(ob_group,
                                         and_(ob_groupuser.c.group_id == ob_group.c.id,
                                              ob_group.c.available == 1,
                                              ob_groupuser.c.available == 1)
                                         ).join(us_user, and_(
                    ob_groupuser.c.available == 1,
                    ob_groupuser.c.user_id == us_user.c.id,
                    us_user.c.id == exercise['user_id'],
                )).alias("a")

                q_exercise_school_grade = select([ join.c.sigma_account_us_user_school_id,
                                   join.c.sigma_account_ob_group_grade,join.c.sigma_account_ob_group_id]) \
                    .select_from(join).group_by(join.c.sigma_account_us_user_school_id)

                exercise_school_grade = self._query(q_exercise_school_grade)


                # if total_students >= 10:
                valid_exercise_schema = {
                    "exercise_id": exercise['id'],
                    "school_id": exercise_school_grade[0]['sigma_account_us_user_school_id'] if exercise_school_grade else 0,
                    "grade": exercise_school_grade[0]['sigma_account_ob_group_grade'] if exercise_school_grade else 0,
                    "grade_id": exercise_school_grade[0]['sigma_account_ob_group_id'] if exercise_school_grade else 0,
                    "student_counts": total_students,
                    "exercise_type": exercise_type,
                    "exercise_time": exercise['time_create'] if not counts else counts[0]['time_create'],
                    "exercise_time_create": one_date[0]
                }
                bulk_update.append(UpdateOne({"_id": str(exercise['id'])}, {'$set': valid_exercise_schema}, upsert=True))
                if bulk_update:
                    try:
                        bulk_update_ret = self.mongo.exercises.bulk_write(bulk_update)
                        # print(bulk_update_ret.bulk_api_result)
                    except BulkWriteError as bwe:
                        print(bwe.details)
                    self._set_time_threadshold("exercise_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

