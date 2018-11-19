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
server = None
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
        server.stop()


class PerDayTask_VALIADCONTEST(BaseTask):
    def __init__(self):
        super(PerDayTask_VALIADCONTEST, self).__init__()

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
            date_range = self._date_range("valid_exercise_word_begin_time") #时间分段
            self._exercise_number(date_range) #有效考试 有效词汇
            self.server.stop()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)





    def _exercise_number(self, date_range):
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

            exercises_words = self._query(q_exercise_in_date)

            exercise_ids = list(set([item['id'] for item in exercises_words]))

            q_exercise_word_meta = select([ob_exercisemeta]).where(and_(
                ob_exercisemeta.c.available == 1,
                ob_exercisemeta.c.exercise_id.in_(exercise_ids),
                ob_exercisemeta.c.value == '"word"'
            ))
            exercise_word_meta = self._query(q_exercise_word_meta)

            word_exercise_ids = set([item['exercise_id'] for item in exercise_word_meta])

            q_exercise_images = select([
                as_hermes.c.student_id,
                as_hermes.c.exercise_id,
                as_hermes.c.time_create]) \
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.exercise_id.in_(exercise_ids)))

            exercise_word_images = self._query(q_exercise_images)

            user_ids = list(set([item['student_id'] for item in exercise_word_images]))

            q_usergroup = select([ob_groupuser]).where(and_(
                ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))

            q_group = select([ob_group]).where(and_(
                ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in group]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']

            group_map = {}
            # print (group)
            for g in group:
                group_map[g['id']] = g
            # print( json.dumps(group_map, indent=4, cls=CustomEncoder))

            usergroup_map = {}
            usergroup_single_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))

                if usergroup_map[u_g['user_id']]:
                    usergroup_map[u_g['user_id']].append(u_g)
                else:
                    usergroup_map[u_g['user_id']] = [u_g]
                usergroup_single_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            valid_exercise_defaultdict = defaultdict(lambda: defaultdict)
            valid_word_default = defaultdict(lambda: defaultdict)
            exercise_user_map = {}
            for e_w_image in exercise_word_images:
                exercise_user_map[e_w_image['exercise_id']] = usergroup_single_map.get(e_w_image.get("student_id", {}))
                if e_w_image['exercise_id'] in word_exercise_ids:  # 单词
                    if valid_word_default[e_w_image['exercise_id']]['n']:
                        valid_word_default[e_w_image['exercise_id']]['n'].append(1)
                    else:
                        valid_word_default[e_w_image['exercise_id']]['n'] = [1]

                    if valid_word_default[e_w_image['exercise_id']]['time']:
                        valid_word_default[e_w_image['exercise_id']]['time'].append(e_w_image['time_create'])
                    else:
                        valid_word_default[e_w_image['exercise_id']]['time_create'] = [e_w_image['time_create']]

                else:  # 考试
                    if valid_exercise_defaultdict[e_w_image['exercise_id']]['n']:
                        valid_exercise_defaultdict[e_w_image['exercise_id']]['n'].append(1)
                    else:
                        valid_exercise_defaultdict[e_w_image['exercise_id']]['n'] = [1]

                    if valid_exercise_defaultdict[e_w_image['exercise_id']]['time']:
                        valid_exercise_defaultdict[e_w_image['exercise_id']]['time'].append(e_w_image['time_create'])
                    else:
                        valid_exercise_defaultdict[e_w_image['exercise_id']]['time'].append(e_w_image['time_create'])

            exercise_begin_time_map = {}
            class_valid_exervise_number_defaultdict = defaultdict(list)
            grade_valid_exervise_number_defaultdict = defaultdict(list)
            channel_valid_exervise_number_defaultdict = defaultdict(list)

            class_valid_word_number_defaultdict = defaultdict(list)
            grade_valid_word_number_defaultdict = defaultdict(list)
            channel_valid_word_number_defaultdict = defaultdict(list)

            for e_w_image in exercise_word_images:
                if e_w_image['exercise_id'] in word_exercise_ids: #单词
                    pass
                else: #考试

                    total = sum(valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['n'])
                    if total>= 10:
                        exercise_begin_time_map["exercise_id"] = \
                        sorted(valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['time'])[0] \
                            if valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['time'] else e_w_image[
                            'time_create']
                        # 班级
                        for u in usergroup_map.get(e_w_image['student_id'], []):
                            class_valid_exervise_number_defaultdict[u.get("group_id", -1)].append(1)

                        #年级
                        grade_valid_exervise_number_defaultdict[usergroup_single_map.get(e_w_image['student_id'], {}).get("grade", -1)].append(1)
                        #渠道
                        channel_valid_exervise_number_defaultdict[school_channel_map.gt( usergroup_single_map.get(e_w_image['student_id'], {}).get("school_id", -1) ,-1)].append(1)

                        # 班级
                        for u in usergroup_map.get(e_w_image['student_id'], []):
                            class_valid_word_number_defaultdict[u.get("group_id", -1)].append(1)

                        # 年级
                        grade_valid_word_number_defaultdict[
                            usergroup_single_map.get(e_w_image['student_id'], {}).get("grade", -1)].append(1)
                        # 渠道
                        channel_valid_word_number_defaultdict[school_channel_map.gt(
                            usergroup_single_map.get(e_w_image['student_id'], {}).get("school_id", -1), -1)].append(1)

            class_valid_exervise_number_defaultdict
            grade_valid_exervise_number_defaultdict
            channel_valid_exervise_number_defaultdict

            class_valid_word_number_defaultdict
            grade_valid_word_number_defaultdict
            channel_valid_word_number_defaultdict

            class_valid_exercise_number_bulk = []
            grade_valid_exercise_number_bulk = []
            channel_valid_exercise_number_bulk = []

            class_valid_word_number_bulk = []
            grade_valid_word_number_bulk = []
            channel_valid_word_number_bulk = []
            for k, v in class_valid_exervise_number_defaultdict.items():
                exercise_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "valid_exercise_count": sum(v)
                }
                class_valid_exercise_number_bulk.append(
                    UpdateOne({"group_id": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))

            for k, v in grade_valid_exervise_number_defaultdict.items():
                exercise_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
                    "valid_exercise_count": sum(v)
                }
                grade_valid_exercise_number_bulk.append(
                    UpdateOne({"grade": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))

            for k, v in channel_valid_exervise_number_defaultdict.items():
                exercise_schema = {
                    "valid_exercise_count": sum(v)
                }
                channel_valid_exercise_number_bulk.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))


            for k, v in class_valid_word_number_defaultdict.items():
                exercise_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "valid_exercise_count": sum(v)
                }
                class_valid_word_number_bulk.append(
                    UpdateOne({"group_id": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))


            for k, v in grade_valid_word_number_defaultdict.items():
                exercise_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
                    "valid_exercise_count": sum(v)
                }
                grade_valid_word_number_bulk.append(
                    UpdateOne({"grade": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))


            for k, v in channel_valid_word_number_defaultdict.items():
                exercise_schema = {
                    "valid_exercise_count": sum(v)
                }
                channel_valid_word_number_bulk.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))


            if class_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            if class_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("valid_exercise_word_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

