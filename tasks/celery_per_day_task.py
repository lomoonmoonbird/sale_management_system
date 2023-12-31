#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列
同步不同角色的用户信息，信息包括学校，年级，班级
"""
import json
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, \
ob_groupuser, ob_exercise, as_hermes, ob_order, re_userwechat, Roles, StageEnum, \
    StudentRelationEnum, ExerciseTypeEnum, ob_exercisemeta, st_location, ob_reading, ob_refund
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
from sshtunnel import SSHTunnelForwarder
from tasks.celery_base import BaseTask, CustomEncoder
from pymongo.errors import BulkWriteError
from celery.signals import worker_process_init,worker_process_shutdown, beat_init
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT


# @beat_init.connect()
# def tttt(**kwargs):
#     print("@@@@@@@@ mmb ")
#
# class task11(BaseTask):
#     def run(self):
#         print('this is task')
#         return 'moonmoonbird'
#
# connection = None
# server = None
#
#
# @worker_process_init.connect
# def init_worker(**kwargs):
#     global connection
#     global server
#     print('Initializing database connection for worker.')
#     if DEBUG:
#         print("this is debug")
#         server = SSHTunnelForwarder(
#             ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机
#
#             ssh_password="PengKim@89527",
#             ssh_username="jinpeng",
#             remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
#         server.start()
#         connection = pymysql.connect(host="127.0.0.1",
#                                           port=server.local_bind_port,
#                                           user="sigma",
#                                           password="sigmaLOVE2017",
#                                           db=MYSQL_NAME,
#                                           charset='utf8mb4',
#                                           cursorclass=pymysql.cursors.DictCursor)
#     else:
#         connection = pymysql.connect(host=MYSQL_HOST,
#                                      port=MYSQL_PORT,
#                                      user=MYSQL_USER,
#                                      password=MYSQL_PASSWORD,
#                                      db=MYSQL_NAME,
#                                      charset='utf8mb4',
#                                      cursorclass=pymysql.cursors.DictCursor)
# @worker_process_shutdown.connect
# def shutdown_worker(**kwargs):
#     global connection
#     if connection:
#         print('Closing database connectionn for worker.')
#         connection.close()
#         if DEBUG:
#             if server:
#                 server.stop()

class PerDaySubTask_IMAGES(BaseTask):
    def __init__(self):
        super(PerDaySubTask_IMAGES, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.connection = None
        self.cursor = None
    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.cursor = self.connection.cursor()
        logger.debug(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret

    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("class_grade_channel_exercise_images_per_day_begin_time")  # 时间分段
            self._exercise_images(date_range)
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=10)

    def _exercise_images(self, date_range):
        """
        考试图片数，单词图片数
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_exercise_images = select([
                as_hermes.c.student_id,
                as_hermes.c.exercise_id,
                as_hermes.c.time_create]) \
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.time_create >= one_date[0],
                            as_hermes.c.time_create < one_date[1]))

            exercise_images = self._query(q_exercise_images)
            # def chunks(l, n):
            #     for i in range(0, len(l), n):
            #         yield l[i:i + n]
            #
            # chunks = list(chunks(q_exercise_images, 3000))
            # exercise_images_map = {}
            # for e_i in exercise_images:
            #     exercise_images_map[e_i['student_id']] = e_i

            exercise_ids = list(set([item['exercise_id'] for item in exercise_images]))

            q_exercise_meta = select([ob_exercisemeta]).where(and_(
                    ob_exercisemeta.c.available == 1,
                    ob_exercisemeta.c.exercise_id.in_(exercise_ids),
                    ob_exercisemeta.c.value == '"word"'
                ))
            exercise_meta = self._query(q_exercise_meta)


            word_exercise_ids = set([item['exercise_id'] for item in exercise_meta])

            user_ids = list(set([item['student_id'] for item in exercise_images]))

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
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            # print (json.dumps(usergroup_map_grade_key, indent=4 , cls=CustomEncoder))
            # print (json.dumps(usergroup_map, indent=4, cls=CustomEncoder))
            # self._set_time_threadshold("user_exercise_images_per_day_begin_time",
            #                                datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

            class_e_image_default_dict = defaultdict(list)
            class_w_image_default_dict = defaultdict(list)
            grade_e_image_default_dict = defaultdict(list)
            grade_w_image_default_dict = defaultdict(list)
            school_e_image_default_dict = defaultdict(list)
            school_w_image_default_dict = defaultdict(list)
            channel_e_image_default_dict = defaultdict(list)
            channel_w_image_default_dict = defaultdict(list)

            for image in exercise_images:
                if image['exercise_id'] in word_exercise_ids:
                    class_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_w_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_w_image_default_dict[str(usergroup_map.get(image['student_id'], {}).get("school_id", -1)) + "@"+str(usergroup_map.get(image['student_id'], {}).get("grade", -1))].append(1)
                    school_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("school_id", -1)].append(1)

                else:
                    class_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_e_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_e_image_default_dict[str(usergroup_map.get(image['student_id'], {}).get("school_id", -1)) + "@" + str(usergroup_map.get(image['student_id'], {}).get("grade", -1))].append(1)
                    school_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("school_id", -1)].append(1)

            #班级
            class_bulk_update = []
            for k, v in class_e_image_default_dict.items():
                class_image_counts_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},{'$set': class_image_counts_schema}, upsert=True))

            for k, v in class_w_image_default_dict.items():
                class_image_counts_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},{'$set': class_image_counts_schema}, upsert=True))

            #年级
            grade_bulk_update = []

            for k, v in grade_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": int(k.split('@')[0]),
                    "channel": school_channel_map.get(int(k.split('@')[0]), -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split('@')[0]), "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            for k, v in grade_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": int(k.split('@')[0]),
                    "channel": school_channel_map.get(int(k.split('@')[0]), -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            #学校

            school_bulk_update = []

            for k, v in school_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            for k, v in school_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            #渠道
            channel_bulk_update = []

            for k, v in channel_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            for k, v in channel_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("class_grade_channel_exercise_images_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _exercise_images_today(self, date_range):
        """
        考试图片数，单词图片数，当天日期
        :param date_range:
        :return:
        """
        self.connection = self.get_connection()
        for one_date in date_range:

            q_exercise_images = select([
                as_hermes.c.student_id,
                as_hermes.c.exercise_id,
                as_hermes.c.time_create]) \
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.time_create >= one_date[0],
                            as_hermes.c.time_create < one_date[1]))

            exercise_images = self._query(q_exercise_images)

            exercise_ids = list(set([item['exercise_id'] for item in exercise_images]))

            q_exercise_meta = select([ob_exercisemeta]).where(and_(
                    ob_exercisemeta.c.available == 1,
                    ob_exercisemeta.c.exercise_id.in_(exercise_ids),
                    ob_exercisemeta.c.value == '"word"'
                ))
            exercise_meta = self._query(q_exercise_meta)


            word_exercise_ids = set([item['exercise_id'] for item in exercise_meta])

            user_ids = list(set([item['student_id'] for item in exercise_images]))

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

            usergroup_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            class_e_image_default_dict = defaultdict(list)
            class_w_image_default_dict = defaultdict(list)
            grade_e_image_default_dict = defaultdict(list)
            grade_w_image_default_dict = defaultdict(list)
            school_e_image_default_dict = defaultdict(list)
            school_w_image_default_dict = defaultdict(list)
            channel_e_image_default_dict = defaultdict(list)
            channel_w_image_default_dict = defaultdict(list)

            for image in exercise_images:
                if image['exercise_id'] in word_exercise_ids:
                    class_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_w_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_w_image_default_dict[str(usergroup_map.get(image['student_id'], {}).get("school_id", -1)) + "@"+str(usergroup_map.get(image['student_id'], {}).get("grade", -1))].append(1)
                    school_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("school_id", -1)].append(1)

                else:
                    class_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_e_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_e_image_default_dict[str(usergroup_map.get(image['student_id'], {}).get("school_id", -1)) + "@" + str(usergroup_map.get(image['student_id'], {}).get("grade", -1))].append(1)
                    school_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("school_id", -1)].append(1)

            #班级
            class_bulk_update = []
            for k, v in class_e_image_default_dict.items():
                class_image_counts_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},{'$set': class_image_counts_schema}, upsert=True))

            for k, v in class_w_image_default_dict.items():
                class_image_counts_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},{'$set': class_image_counts_schema}, upsert=True))

            #年级
            grade_bulk_update = []

            for k, v in grade_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": int(k.split('@')[0]),
                    "channel": school_channel_map.get(int(k.split('@')[0]), -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split('@')[0]), "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            for k, v in grade_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": int(k.split('@')[0]),
                    "channel": school_channel_map.get(int(k.split('@')[0]), -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            #学校

            school_bulk_update = []

            for k, v in school_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            for k, v in school_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            #渠道
            channel_bulk_update = []

            for k, v in channel_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            for k, v in channel_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None

class PerDaySubTask_GUARDIAN(BaseTask):
    def __init__(self):
        super(PerDaySubTask_GUARDIAN, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.connection = None
        self.cursor = None
        self.record_class_unique_guardian_delta_coll = 'record_class_unique_guardian_delta'
        self.record_grade_unique_guardian_delta_coll = 'record_grade_unique_guardian_delta'
        self.record_school_unique_guardian_delta_coll = 'record_school_unique_guardian_delta'
        self.record_channel_unique_guardian_delta_coll = 'record_channel_unique_guardian_delta'
        self.record_class_today_unique_guardian_delta_coll = 'record_class_today_unique_guardian_delta'
        self.record_grade_today_unique_guardian_delta_coll = 'record_grade_today_unique_guardian_delta'
        self.record_school_today_unique_guardian_delta_coll = 'record_school_today_unique_guardian_delta'
        self.record_channel_today_unique_guardian_delta_coll = 'record_channel_today_unique_guardian_delta'

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.cursor = self.connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret

    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("class_grade_channel_guardian_per_day_begin_time")  # 时间分段
            # date_range = [("2018-07-01","2018-07-02")]
            self._guardian_info(date_range)
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise self.retry(exc=e, countdown=30, max_retries=10)

    def _guardian_info(self, date_range):
        """
        家长数
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_guardian_counts = select([
                re_userwechat.c.user_id,
                re_userwechat.c.wechat_id,
                re_userwechat.c.time_create,
            ], and_(
                re_userwechat.c.available == 1,
                # re_userwechat.c.relationship > StudentRelationEnum.sich.value,
                re_userwechat.c.time_create >= one_date[0],
                re_userwechat.c.time_create < one_date[1]
            )
            )

            guardians = self._query(q_guardian_counts)

            user_ids = list(set([item['user_id'] for item in guardians]))

            q_users = select([us_user.c.id,
                              us_user.c.school_id]).where(and_( us_user.c.available == 1,
                                                                us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))
            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']

            group_map = {}

            for g in group:
                group_map[g['id']] = g

            usergroup_map = {}
            usergroup_multi_map = defaultdict(list)
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))

                if usergroup_multi_map[u_g['user_id']]:
                    usergroup_multi_map[u_g['user_id']].append(u_g)
                else:
                    usergroup_multi_map[u_g['user_id']] = [u_g]

                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g


            class_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            class_unique_guardian_default_dict = defaultdict(set)
            grade_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_unique_guardian_default_dict = defaultdict(set)
            school_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            school_unique_guardian_default_dict = defaultdict(set)
            channel_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_unique_guardian_default_dict = defaultdict(set)

            for guardian in guardians:

                for u in usergroup_multi_map.get(guardian['user_id'], []):
                    #班级
                    if class_guardian_default_dict[u.get("group_id", -1)]['n']:
                        class_guardian_default_dict[u.get("group_id", -1)]['n'].append(1)
                    else:
                        class_guardian_default_dict[u.get("group_id", -1)]['n'] = [1]
                    if class_guardian_default_dict[u.get("group_id", -1)]['wechats']:
                        class_guardian_default_dict[u.get("group_id", -1)]['wechats'].append(guardian['wechat_id'])
                    else:
                        class_guardian_default_dict[u.get("group_id", -1)]['wechats'] = [guardian['wechat_id']]
                    #班级unique
                    class_unique_guardian_default_dict[u.get("group_id", -1)].add(guardian['user_id'])

                # 年级
                if grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n']:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n'].append(1)
                else:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n'] = [1]

                if grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats']:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats'].append(guardian['wechat_id'])
                else:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats'] = [guardian['wechat_id']]
                    # 年级unique
                grade_unique_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))].add(guardian['user_id'])

                #学校
                if school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n']:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n'].append(1)
                else:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n'] = [1]


                if school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats']:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats'].append(guardian['wechat_id'])
                else:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats'] = [guardian['wechat_id']]

                #学校unique
                school_unique_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)].add(guardian['user_id'])
                #渠道
                if not school_channel_map.get(user_school_map.get(guardian['user_id'], -1)):
                    self.mongo.no_channel_school_student.insert({"user_id": guardian['user_id'],"school_id": user_school_map.get(guardian['user_id'],-1), "channel": school_channel_map.get(user_school_map.get(guardian['user_id'], -1))})
                if channel_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['n']:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['n'].append(1)
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'],-1))]['n'] = [1]
                if channel_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats']:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats'].append(guardian['wechat_id'])
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats'] = [guardian['wechat_id']]
                #渠道unique
                channel_unique_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))].add(guardian['user_id'])
                class_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("group_id", -1)]['group_info'] = usergroup_map.get(guardian['user_id'], {})
                grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['group_info'] = usergroup_map.get(guardian['user_id'],{})
                channel_guardian_default_dict[school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['group_info'] = usergroup_map.get(guardian['user_id'],{})

            # print(json.dumps(usergroup_map_class_key,indent=4,cls=CustomEncoder))

            # 班级
            class_bulk_update = []

            for k, v in class_guardian_default_dict.items():

                guardian_schema = {
                    "school_id": v.get("group_info", {}).get("school_id", -1),
                    "channel": school_channel_map.get(v.get("group_info", {}).get("school_id", -1), -1),
                    "grade": v.get("group_info", {}).get("grade", -1),
                    "guardian_count":len(v['n']),
                    "wechats": v['wechats'],
                }
                class_bulk_update.append(UpdateOne({"group_id": v['group_info'].get('group_id', -1), "day": one_date[0]},
                                             {'$set': guardian_schema}, upsert=True))
            class_unique_bulk_update = []
            history_class_unique_guardian_bulk_update = []
            for k, v in class_unique_guardian_default_dict.items():
                history_record = self.mongo[self.record_class_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list( set(v).difference( set (history_record.get("user_id", []))) )
                record = list( set(v).union( set (history_record.get("user_id", []))) )
                guardian_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                class_unique_bulk_update.append(
                    UpdateOne({"group_id": int(k), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))
                history_class_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": int(k)},
                              {'$set': {"user_id": record}}, upsert=True))

            #年级
            grade_bulk_update = []

            for k, v in grade_guardian_default_dict.items():


                guardian_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            grade_unique_bulk_update = []
            history_grade_unique_guardian_bulk_update = []
            for k, v in grade_unique_guardian_default_dict.items():
                history_record = self.mongo[self.record_grade_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                grade_unique_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

                history_grade_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": k},
                              {'$set': {"user_id": record}}, upsert=True))

            # 学校
            school_bulk_update = []

            for k, v in school_guardian_default_dict.items():
                guardian_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            school_unique_bulk_update = []
            history_school_unique_guardian_bulk_update = []
            for k, v in school_unique_guardian_default_dict.items():

                history_record = self.mongo[self.record_school_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "school_id": int(k),
                    "channel": school_channel_map.get(k, -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                school_unique_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

                history_school_unique_guardian_bulk_update.append(
                UpdateOne({"_id": k},
                          {'$set': {"user_id": record}}, upsert=True))

            #渠道
            channel_bulk_update = []
            for k, v in channel_guardian_default_dict.items():
                guardian_schema = {
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            channel_unique_bulk_update = []
            history_channel_unique_guardian_bulk_update = []
            for k, v in channel_unique_guardian_default_dict.items():

                history_record = self.mongo[self.record_channel_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                channel_unique_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))
                history_channel_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": k},
                              {'$set': {"user_id": record}}, upsert=True))

            if history_class_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_class_unique_guardian_delta_coll].bulk_write(history_class_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_grade_unique_guardian_delta_coll].bulk_write(history_grade_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_school_unique_guardian_delta_coll].bulk_write(history_school_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_channel_unique_guardian_delta_coll].bulk_write(history_channel_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if school_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if channel_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("class_grade_channel_guardian_per_day_begin_time",
                                           datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _guardian_info_today(self, date_range):
        """
        家长数 当天
        :param date_range:
        :return:
        """
        for one_date in date_range:

            q_guardian_counts = select([
                re_userwechat.c.user_id,
                re_userwechat.c.wechat_id,
                re_userwechat.c.time_create,
            ], and_(
                re_userwechat.c.available == 1,
                # re_userwechat.c.relationship > StudentRelationEnum.sich.value,
                re_userwechat.c.time_create >= one_date[0],
                re_userwechat.c.time_create < one_date[1]
            )
            )

            guardians = self._query(q_guardian_counts)

            user_ids = list(set([item['user_id'] for item in guardians]))

            q_users = select([us_user.c.id,
                              us_user.c.school_id]).where(and_( us_user.c.available == 1,
                                                                us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))
            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']

            group_map = {}

            for g in group:
                group_map[g['id']] = g

            usergroup_map = {}
            usergroup_multi_map = defaultdict(list)
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))

                if usergroup_multi_map[u_g['user_id']]:
                    usergroup_multi_map[u_g['user_id']].append(u_g)
                else:
                    usergroup_multi_map[u_g['user_id']] = [u_g]

                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g


            class_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            class_unique_guardian_default_dict = defaultdict(set)
            grade_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_unique_guardian_default_dict = defaultdict(set)
            school_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            school_unique_guardian_default_dict = defaultdict(set)
            channel_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_unique_guardian_default_dict = defaultdict(set)

            for guardian in guardians:

                for u in usergroup_multi_map.get(guardian['user_id'], []):
                    #班级
                    if class_guardian_default_dict[u.get("group_id", -1)]['n']:
                        class_guardian_default_dict[u.get("group_id", -1)]['n'].append(1)
                    else:
                        class_guardian_default_dict[u.get("group_id", -1)]['n'] = [1]
                    if class_guardian_default_dict[u.get("group_id", -1)]['wechats']:
                        class_guardian_default_dict[u.get("group_id", -1)]['wechats'].append(guardian['wechat_id'])
                    else:
                        class_guardian_default_dict[u.get("group_id", -1)]['wechats'] = [guardian['wechat_id']]
                    #班级unique
                    class_unique_guardian_default_dict[u.get("group_id", -1)].add(guardian['user_id'])

                # 年级
                if grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n']:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n'].append(1)
                else:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['n'] = [1]

                if grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats']:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats'].append(guardian['wechat_id'])
                else:
                    grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['wechats'] = [guardian['wechat_id']]
                    # 年级unique
                grade_unique_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))].add(guardian['user_id'])

                #学校
                if school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n']:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n'].append(1)
                else:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['n'] = [1]


                if school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats']:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats'].append(guardian['wechat_id'])
                else:
                    school_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)]['wechats'] = [guardian['wechat_id']]

                #学校unique
                school_unique_guardian_default_dict[user_school_map.get(guardian['user_id'], -1)].add(guardian['user_id'])
                #渠道
                if not school_channel_map.get(user_school_map.get(guardian['user_id'], -1)):
                    self.mongo.no_channel_school_student.insert({"user_id": guardian['user_id'],"school_id": user_school_map.get(guardian['user_id'],-1), "channel": school_channel_map.get(user_school_map.get(guardian['user_id'], -1))})
                if channel_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['n']:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['n'].append(1)
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'],-1))]['n'] = [1]
                if channel_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats']:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats'].append(guardian['wechat_id'])
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(user_school_map.get(guardian['user_id'], -1))]['wechats'] = [guardian['wechat_id']]
                #渠道unique
                channel_unique_guardian_default_dict[school_channel_map.get(user_school_map.get(guardian['user_id'], -1))].add(guardian['user_id'])
                class_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("group_id", -1)]['group_info'] = usergroup_map.get(guardian['user_id'], {})
                grade_guardian_default_dict[str(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_map.get(guardian['user_id'], {}).get("grade", -1))]['group_info'] = usergroup_map.get(guardian['user_id'],{})
                channel_guardian_default_dict[school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['group_info'] = usergroup_map.get(guardian['user_id'],{})

            # print(json.dumps(usergroup_map_class_key,indent=4,cls=CustomEncoder))

            # 班级
            class_bulk_update = []

            for k, v in class_guardian_default_dict.items():

                guardian_schema = {
                    "school_id": v.get("group_info", {}).get("school_id", -1),
                    "channel": school_channel_map.get(v.get("group_info", {}).get("school_id", -1), -1),
                    "grade": v.get("group_info", {}).get("grade", -1),
                    "guardian_count":len(v['n']),
                    "wechats": v['wechats'],
                }
                class_bulk_update.append(UpdateOne({"group_id": v['group_info'].get('group_id', -1), "day": one_date[0]},
                                             {'$set': guardian_schema}, upsert=True))
            class_unique_bulk_update = []
            history_class_unique_guardian_bulk_update = []
            for k, v in class_unique_guardian_default_dict.items():
                history_record = self.mongo[self.record_class_today_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list( set(v).difference( set (history_record.get("user_id", []))) )
                record = list( set(v).union( set (history_record.get("user_id", []))) )
                guardian_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                class_unique_bulk_update.append(
                    UpdateOne({"group_id": int(k), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))
                history_class_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": int(k)},
                              {'$set': {"user_id": record}}, upsert=True))

            #年级
            grade_bulk_update = []

            for k, v in grade_guardian_default_dict.items():


                guardian_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            grade_unique_bulk_update = []
            history_grade_unique_guardian_bulk_update = []
            for k, v in grade_unique_guardian_default_dict.items():
                history_record = self.mongo[self.record_grade_today_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                grade_unique_bulk_update.append(
                    UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

                history_grade_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": k},
                              {'$set': {"user_id": record}}, upsert=True))

            # 学校
            school_bulk_update = []

            for k, v in school_guardian_default_dict.items():
                guardian_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                school_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            school_unique_bulk_update = []
            history_school_unique_guardian_bulk_update = []
            for k, v in school_unique_guardian_default_dict.items():

                history_record = self.mongo[self.record_school_today_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "school_id": int(k),
                    "channel": school_channel_map.get(k, -1),
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                school_unique_bulk_update.append(
                    UpdateOne({"school_id": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

                history_school_unique_guardian_bulk_update.append(
                UpdateOne({"_id": k},
                          {'$set': {"user_id": record}}, upsert=True))

            #渠道
            channel_bulk_update = []
            for k, v in channel_guardian_default_dict.items():
                guardian_schema = {
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            channel_unique_bulk_update = []
            history_channel_unique_guardian_bulk_update = []
            for k, v in channel_unique_guardian_default_dict.items():

                history_record = self.mongo[self.record_channel_today_unique_guardian_delta_coll].find_one({"_id": k})
                if not history_record:
                    history_record = {}
                v = list(set(v).difference(set(history_record.get("user_id", []))))
                record = list(set(v).union(set(history_record.get("user_id", []))))
                guardian_schema = {
                    "guardian_unique_count": len(v),
                    "wechat_user_ids": list(v),
                }
                channel_unique_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))
                history_channel_unique_guardian_bulk_update.append(
                    UpdateOne({"_id": k},
                              {'$set': {"user_id": record}}, upsert=True))

            if history_class_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_class_today_unique_guardian_delta_coll].bulk_write(history_class_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_grade_today_unique_guardian_delta_coll].bulk_write(history_grade_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_school_today_unique_guardian_delta_coll].bulk_write(history_school_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_unique_guardian_bulk_update:
                try:
                    bulk_update_ret = self.mongo[self.record_channel_today_unique_guardian_delta_coll].bulk_write(history_channel_unique_guardian_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if school_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if channel_unique_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_unique_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

class PerDaySubTask_PAYMENTS(BaseTask):
    def __init__(self):
        super(PerDaySubTask_PAYMENTS, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.cursor = None
        self.connection = None

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.cursor = self.connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret

    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("class_grade_channel_pay_per_day_begin_time")  # 时间分段
            # date_range = [("2018-01-01", "2018-12-03")]
            self._pay_amount(date_range) #付费数 付费额
            date_range = self._date_range("class_grade_channel_refund_per_day_begin_time")  # 时间分段
            # date_range = [("2018-01-01", "2018-12-03")]
            self._refund_amount(date_range)  # 退款数 退款额
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=10)

    def _pay_amount(self, date_range):
        """
        付费数，付费额
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_payments = select([ob_order.c.user_id, ob_order.c.coupon_amount, ob_order.c.time_create])\
                .where(and_(ob_order.c.available == 1,
                            ob_order.c.status == 3,
                            ob_order.c.time_create >= one_date[0],
                            ob_order.c.time_create < one_date[1])
                       )
            # print(q_payments.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
            payments = self._query(q_payments)

            user_ids = list(set([item['user_id'] for item in payments]))

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            q_users = select([us_user.c.id, us_user.c.school_id]).where(and_(us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))

            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']


            group_map = {}

            for g in group:
                group_map[g['id']] = g


            # usergroup_map = {}
            # usergroup_mapmulti = defaultdict(list)
            # usergroup_map_class_key = {}
            # usergroup_map_grade_key = {}
            # for u_g in usergroup:
            #
            #     u_g.update(group_map.get(u_g['group_id'], {}))
            #     print(u_g)
            #     if usergroup_mapmulti[u_g['user_id']]:
            #         usergroup_mapmulti[u_g['user_id']].append(u_g)
            #     else:
            #         usergroup_mapmulti[u_g['user_id']] = [u_g]
            #     usergroup_map[u_g['user_id']] = u_g
            #     usergroup_map_class_key[u_g['group_id']] = u_g
            #     usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
            class_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            school_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            # print(payments)
            for payment in payments:
                # if payment['user_id'] in [5019,5020,5021, 5024,5028,]:
                    # print("###########!##@#@!#@#@#@@@@@", payment, usergroup_map.get(payment['user_id'], {}).get("group_id", 0))
                if float(payment['coupon_amount']) <=0:
                    continue
                for data in usergroup_map.get(payment['user_id'], {}):
                    # print(data, '@@@@####')
                    #班级
                    if class_payment_default_dict[data.get("group_id", 0)]["pay_n"]:
                        class_payment_default_dict[data.get("group_id", 0)]["pay_n"].append(1)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_n"] = [1]
                    if class_payment_default_dict[data.get("group_id", 0)]["pay_amount"]:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_amount"].append(payment['coupon_amount'])
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_amount"] = [payment['coupon_amount']]

                #年级
                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n'].append(1)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n'] = [1]

                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount'].append(payment['coupon_amount'])
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount'] = [payment['coupon_amount']]

                #学校
                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n'].append(1)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n'] = [1]

                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount'].append(payment['coupon_amount'])
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount'] = [payment['coupon_amount']]
                #渠道
                if school_channel_map.get(school_channel_map.get(payment['user_id'], {}).get("school_id", -1), -1) == -1:
                    self.mongo.no_channel_students.update_one({"name": "no_channels_students"}, {"$set": {"student": payment['user_id']}},upsert=True)
                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['pay_n']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_n'].append(1)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_n'] = [1]

                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['pay_amount']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1),-1)][
                        'pay_amount'].append(payment['coupon_amount'])
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_amount'] = [payment['coupon_amount']]



            #班级
            class_bulk_update = []

            for k, v in class_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))
            #年级
            grade_bulk_update = []
            for k, v in grade_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                grade_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))

            # 学校
            school_bulk_update = []
            for k, v in school_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                school_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))
            #渠道
            channel_bulk_update = []
            for k, v in channel_payment_default_dict.items():
                pay_amount_schema = {
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                channel_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            self._set_time_threadshold("class_grade_channel_pay_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _pay_amount_today(self, date_range):
        """
        付费数，付费额  当日
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_payments = select([ob_order.c.user_id, ob_order.c.coupon_amount, ob_order.c.time_create])\
                .where(and_(ob_order.c.available == 1,
                            ob_order.c.status == 3,
                            ob_order.c.time_create >= one_date[0],
                            ob_order.c.time_create < one_date[1])
                       )
            # print(q_payments.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
            payments = self._query(q_payments)

            user_ids = list(set([item['user_id'] for item in payments]))

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            q_users = select([us_user.c.id, us_user.c.school_id]).where(and_(us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))

            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']


            group_map = {}

            for g in group:
                group_map[g['id']] = g


            # usergroup_map = {}
            # usergroup_mapmulti = defaultdict(list)
            # usergroup_map_class_key = {}
            # usergroup_map_grade_key = {}
            # for u_g in usergroup:
            #
            #     u_g.update(group_map.get(u_g['group_id'], {}))
            #     print(u_g)
            #     if usergroup_mapmulti[u_g['user_id']]:
            #         usergroup_mapmulti[u_g['user_id']].append(u_g)
            #     else:
            #         usergroup_mapmulti[u_g['user_id']] = [u_g]
            #     usergroup_map[u_g['user_id']] = u_g
            #     usergroup_map_class_key[u_g['group_id']] = u_g
            #     usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
            class_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            school_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            # print(payments)
            for payment in payments:
                # if payment['user_id'] in [5019,5020,5021, 5024,5028,]:
                    # print("###########!##@#@!#@#@#@@@@@", payment, usergroup_map.get(payment['user_id'], {}).get("group_id", 0))
                if float(payment['coupon_amount']) <=0:
                    continue
                for data in usergroup_map.get(payment['user_id'], {}):
                    # print(data, '@@@@####')
                    #班级
                    if class_payment_default_dict[data.get("group_id", 0)]["pay_n"]:
                        class_payment_default_dict[data.get("group_id", 0)]["pay_n"].append(1)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_n"] = [1]
                    if class_payment_default_dict[data.get("group_id", 0)]["pay_amount"]:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_amount"].append(payment['coupon_amount'])
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "pay_amount"] = [payment['coupon_amount']]

                #年级
                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n'].append(1)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_n'] = [1]

                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount'].append(payment['coupon_amount'])
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['pay_amount'] = [payment['coupon_amount']]

                #学校
                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n'].append(1)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_n'] = [1]

                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount'].append(payment['coupon_amount'])
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['pay_amount'] = [payment['coupon_amount']]
                #渠道
                if school_channel_map.get(school_channel_map.get(payment['user_id'], {}).get("school_id", -1), -1) == -1:
                    self.mongo.no_channel_students.update_one({"name": "no_channels_students"}, {"$set": {"student": payment['user_id']}},upsert=True)
                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['pay_n']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_n'].append(1)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_n'] = [1]

                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['pay_amount']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1),-1)][
                        'pay_amount'].append(payment['coupon_amount'])
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'pay_amount'] = [payment['coupon_amount']]



            #班级
            class_bulk_update = []

            for k, v in class_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))
            #年级
            grade_bulk_update = []
            for k, v in grade_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                grade_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))

            # 学校
            school_bulk_update = []
            for k, v in school_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                school_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))
            #渠道
            channel_bulk_update = []
            for k, v in channel_payment_default_dict.items():
                pay_amount_schema = {
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                channel_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

    def _refund_amount(self, date_range):
        """
        退款数，退款额
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_refund =  select([ob_refund.c.order_id, ob_refund.c.amount])\
                .where(and_(
                            ob_refund.c.status == 6,
                            ob_refund.c.time_create >= one_date[0],
                            ob_refund.c.time_create < one_date[1])
                       )
            refunds = self._query(q_refund)
            refund_order_ids = list(set([item['order_id'] for item in refunds]))
            q_payments = select([ob_order.c.id, ob_order.c.user_id, ob_order.c.coupon_amount])\
                .where(and_(
                            # ob_order.c.available == 1,
                            # ob_order.c.status == 6,
                            ob_order.c.id.in_(refund_order_ids),
                            # ob_order.c.time_modify >= one_date[0],
                            # ob_order.c.time_modify < one_date[1]
                            )
                       )

            payments = self._query(q_payments)

            refund_map = defaultdict(list)
            for refund in refunds:
                refund_map[refund['order_id']].append(refund['amount'])


            user_ids = list(set([item['user_id'] for item in payments]))

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            q_users = select([us_user.c.id, us_user.c.school_id]).where(and_(us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))

            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']


            group_map = {}

            for g in group:
                group_map[g['id']] = g


            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
            class_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            school_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            # print(payments)
            for payment in payments:
                refund_number = len(refund_map.get(payment['id'], []))
                refund_amount = sum(refund_map.get(payment['id'], []))
                if float(payment['coupon_amount']) <=0:
                    continue
                for data in usergroup_map.get(payment['user_id'], {}):
                    # print(data, '@@@@####')
                    #班级
                    if class_payment_default_dict[data.get("group_id", 0)]["refund_n"]:
                        class_payment_default_dict[data.get("group_id", 0)]["refund_n"].append(refund_number)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_n"] = [refund_number]
                    if class_payment_default_dict[data.get("group_id", 0)]["refund_amount"]:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_amount"].append(refund_amount)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_amount"] = [refund_amount]

                #年级
                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n'].append(refund_number)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n'] = [refund_number]

                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount'].append(refund_amount)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount'] = [refund_amount]

                #学校
                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n'].append(refund_number)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n'] = [refund_number]

                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount'].append(refund_amount)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount'] = [refund_amount]
                #渠道
                # if school_channel_map.get(school_channel_map.get(payment['user_id'], {}).get("school_id", -1), -1) == -1:
                #     self.mongo.no_channel_students.update_one({"name": "no_channels_students"}, {"$set": {"student": payment['user_id']}},upsert=True)
                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['refund_n']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_n'].append(refund_number)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_n'] = [refund_number]

                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['refund_amount']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1),-1)][
                        'refund_amount'].append(refund_amount)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_amount'] = [refund_amount]



            #班级
            class_bulk_update = []

            for k, v in class_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))
            #年级
            grade_bulk_update = []
            for k, v in grade_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                grade_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))

            # 学校
            school_bulk_update = []
            for k, v in school_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                school_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))
            #渠道
            channel_bulk_update = []
            for k, v in channel_payment_default_dict.items():
                pay_amount_schema = {
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                channel_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            self._set_time_threadshold("class_grade_channel_refund_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _refund_amount_today(self, date_range):
        """
        退款数，退款额 当日
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_refund =  select([ob_refund.c.order_id, ob_refund.c.amount])\
                .where(and_(
                            ob_refund.c.status == 6,
                            ob_refund.c.time_create >= one_date[0],
                            ob_refund.c.time_create < one_date[1])
                       )
            refunds = self._query(q_refund)
            refund_order_ids = list(set([item['order_id'] for item in refunds]))
            q_payments = select([ob_order.c.id, ob_order.c.user_id, ob_order.c.coupon_amount])\
                .where(and_(
                            # ob_order.c.available == 1,
                            # ob_order.c.status == 6,
                            ob_order.c.id.in_(refund_order_ids),
                            # ob_order.c.time_modify >= one_date[0],
                            # ob_order.c.time_modify < one_date[1]
                            )
                       )

            payments = self._query(q_payments)

            refund_map = defaultdict(list)
            for refund in refunds:
                refund_map[refund['order_id']].append(refund['amount'])


            user_ids = list(set([item['user_id'] for item in payments]))

            q_usergroup = select([ob_groupuser]).where(and_(
                # ob_groupuser.c.available == 1,
                ob_groupuser.c.user_id.in_(user_ids),
            ))

            q_users = select([us_user.c.id, us_user.c.school_id]).where(and_(us_user.c.id.in_(user_ids)))

            users = self._query(q_users)
            user_school_map = {}
            for user in users:
                user_school_map[user['id']] = user['school_id']

            usergroup = self._query(q_usergroup)

            group_ids = list(set([item['group_id'] for item in usergroup]))

            q_group = select([ob_group]).where(and_(
                # ob_group.c.available == 1,
                ob_group.c.id.in_(group_ids),
            ))

            group = self._query(q_group)

            school_ids = list(set([item['school_id'] for item in users]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                # ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)
            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']


            group_map = {}

            for g in group:
                group_map[g['id']] = g


            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
            class_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            school_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            # print(payments)
            for payment in payments:
                refund_number = len(refund_map.get(payment['id'], []))
                refund_amount = sum(refund_map.get(payment['id'], []))
                if float(payment['coupon_amount']) <=0:
                    continue
                for data in usergroup_map.get(payment['user_id'], {}):
                    # print(data, '@@@@####')
                    #班级
                    if class_payment_default_dict[data.get("group_id", 0)]["refund_n"]:
                        class_payment_default_dict[data.get("group_id", 0)]["refund_n"].append(refund_number)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_n"] = [refund_number]
                    if class_payment_default_dict[data.get("group_id", 0)]["refund_amount"]:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_amount"].append(refund_amount)
                    else:
                        class_payment_default_dict[data.get("group_id", 0)][
                            "refund_amount"] = [refund_amount]

                #年级
                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n'].append(refund_number)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_n'] = [refund_number]

                if grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount']:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount'].append(refund_amount)
                else:
                    grade_payment_default_dict[str(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1))+"@"+str(usergroup_single_map.get(payment['user_id'], {}).get("grade", -1))]['refund_amount'] = [refund_amount]

                #学校
                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n'].append(refund_number)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_n'] = [refund_number]

                if school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount']:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount'].append(refund_amount)
                else:
                    school_payment_default_dict[usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1)]['refund_amount'] = [refund_amount]
                #渠道
                # if school_channel_map.get(school_channel_map.get(payment['user_id'], {}).get("school_id", -1), -1) == -1:
                #     self.mongo.no_channel_students.update_one({"name": "no_channels_students"}, {"$set": {"student": payment['user_id']}},upsert=True)
                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['refund_n']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_n'].append(refund_number)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_n'] = [refund_number]

                if channel_payment_default_dict[school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)]['refund_amount']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1),-1)][
                        'refund_amount'].append(refund_amount)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_single_map.get(payment['user_id'], {}).get("school_id", -1), -1)][
                        'refund_amount'] = [refund_amount]



            #班级
            class_bulk_update = []

            for k, v in class_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                class_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))
            #年级
            grade_bulk_update = []
            for k, v in grade_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": int(k.split("@")[0]),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                grade_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))

            # 学校
            school_bulk_update = []
            for k, v in school_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": k,
                    "channel": school_channel_map.get(k, -1),
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                school_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))
            #渠道
            channel_bulk_update = []
            for k, v in channel_payment_default_dict.items():
                pay_amount_schema = {
                    "refund_number": sum(v['refund_n']),
                    "refund_amount": sum(v['refund_amount'])
                }

                channel_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                   {'$set': pay_amount_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

class PerDayTask_SCHOOL(BaseTask):
    def __init__(self):
        super(PerDayTask_SCHOOL, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.schoolstage_delta_record_coll = "record_schoolstage_delta"
        self.location_distinct_coll = 'location_distinct'
        self.location_today_distinct_coll = 'location_today_distinct'
        self.cursor = None
        self.connection = None


    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.cursor = self.connection.cursor()
        logger.debug(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret

    def _execute_raw(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        ret = cursor.fetchall()
        return ret

    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("school_number_per_day_begin_time")  # 时间分段
            self._school(date_range) #学校数
            date_range = self._date_range("school_stage_begin_time")  # 时间分段
            # date_range =[("2018-05-27", "2018-05-28")]
            self._schools(date_range)  # 学校

            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)


    def _school(self, date_range):
        """
        学校数
        :param date_range:
        :return:
        """
        q_locations = select([st_location.c.id, st_location.c.city_id])
        locations = self._query(q_locations)
        locations_map = {}
        for location in locations:
            locations_map[location['id']] = location['city_id']
        distinct_location_set = []
        mongo_distinct_locations = self.mongo[self.location_distinct_coll].find_one({"_id": "city"})
        if mongo_distinct_locations:
            distinct_location_set = mongo_distinct_locations.get('city_id', [])

        for one_date in date_range:


            q_schools = select([ob_school.c.id, ob_school.c.owner_id, ob_school.c.location_id])\
                .where(and_(ob_school.c.available == 1,
                            ob_school.c.time_create >= one_date[0],
                            ob_school.c.time_create < one_date[1])
                       )

            schools = self._query(q_schools)

            school_defaultdict = defaultdict(lambda: defaultdict(dict))

            self_school_bulk_update = []
            channel_school_bulk_update = []
            for school in schools:
                if school_defaultdict[school['owner_id']]['school_n']:
                    school_defaultdict[school['owner_id']]['school_n'].append(1)
                else:
                    school_defaultdict[school['owner_id']]['school_n'] = [1]


                if locations_map.get(school['location_id'], "") not in distinct_location_set:
                    if school_defaultdict[school['owner_id']]['location_n']:
                        school_defaultdict[school['owner_id']]['location_n'].append(1)

                    else:
                        school_defaultdict[school['owner_id']]['location_n'] = [1]
                    distinct_location_set.append(locations_map.get(school['location_id'], ""))
                else:
                    pass

            self.mongo[self.location_distinct_coll].update({"_id": "city"}, {"$set": {"city_id": list(set(distinct_location_set))}}, upsert=True)

            for k,v in school_defaultdict.items():
                school_schema = {

                    "school_number": sum(v['school_n']),
                    "city_number": sum(v['location_n'])
                }

                channel_school_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                         {'$set': school_schema}, upsert=True))



            if channel_school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("school_number_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _school_today(self, date_range):
        """
        学校数 当日
        :param date_range:
        :return:
        """
        q_locations = select([st_location.c.id, st_location.c.city_id])
        locations = self._query(q_locations)
        locations_map = {}
        for location in locations:
            locations_map[location['id']] = location['city_id']
        distinct_location_set = []
        mongo_distinct_locations = self.mongo[self.location_today_distinct_coll].find_one({"_id": "city"})
        if mongo_distinct_locations:
            distinct_location_set = mongo_distinct_locations.get('city_id', [])

        for one_date in date_range:


            q_schools = select([ob_school.c.id, ob_school.c.owner_id, ob_school.c.location_id])\
                .where(and_(ob_school.c.available == 1,
                            ob_school.c.time_create >= one_date[0],
                            ob_school.c.time_create < one_date[1])
                       )

            schools = self._query(q_schools)

            school_defaultdict = defaultdict(lambda: defaultdict(dict))

            self_school_bulk_update = []
            channel_school_bulk_update = []
            for school in schools:
                if school_defaultdict[school['owner_id']]['school_n']:
                    school_defaultdict[school['owner_id']]['school_n'].append(1)
                else:
                    school_defaultdict[school['owner_id']]['school_n'] = [1]


                if locations_map.get(school['location_id'], "") not in distinct_location_set:
                    if school_defaultdict[school['owner_id']]['location_n']:
                        school_defaultdict[school['owner_id']]['location_n'].append(1)

                    else:
                        school_defaultdict[school['owner_id']]['location_n'] = [1]
                    distinct_location_set.append(locations_map.get(school['location_id'], ""))
                else:
                    pass

            self.mongo[self.location_today_distinct_coll].update({"_id": "city"}, {"$set": {"city_id": list(set(distinct_location_set))}}, upsert=True)

            for k,v in school_defaultdict.items():
                school_schema = {

                    "school_number": sum(v['school_n']),
                    "city_number": sum(v['location_n'])
                }

                channel_school_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                         {'$set': school_schema}, upsert=True))



            if channel_school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_school_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

    def _schools(self, date_range):
        """
        学校阶段
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_school = select([ob_school]) \
                .where(and_(ob_school.c.available == 1,
                            ob_school.c.time_create >= one_date[0],
                            ob_school.c.time_create < one_date[1]
                            )
                       )
            schools = self._query(q_school)
            update_school_bulk = []

            for school in schools:
                school_schema = {
                    "open_time": school['time_create'],
                    "channel": school['owner_id'],
                    "stage": StageEnum.Register.value
                }
                update_school_bulk.append(UpdateOne({"school_id": school['id'],"stage": {"$in": [StageEnum.Register.value,
                                                                                                 StageEnum.Using.value]}},
                                                    {"$set": school_schema}, upsert=True))

            if update_school_bulk:
                try:
                    bulk_update_ret = self.mongo.school.bulk_write(update_school_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("school_stage_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDaySubTask_USERS(BaseTask):
    def __init__(self):
        super(PerDaySubTask_USERS, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.cursor = None
        self.connection = None

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        self.cursor = self.connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret

    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("teacher_student_number_per_day_begin_time")  # 时间分段
            # date_range = [("2018-07-18", "2018-07-19")]
            self._user_counts(date_range) #老师数 学生数
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise self.retry(exc=e, countdown=30, max_retries=10)

    def _user_counts(self, date_range):
        """
        老师数，学生数
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_teacher_student = select([us_user.c.id, us_user.c.role_id, us_user.c.school_id])\
                .where(and_(us_user.c.available == 1,
                            us_user.c.role_id.in_([Roles.TEACHER.value, Roles.STUDENT.value]),
                            us_user.c.time_create >= one_date[0],
                            us_user.c.time_create < one_date[1]
                            )
                       )

            teacher_student = self._query(q_teacher_student)

            user_ids = list(set([item['id'] for item in teacher_student]))



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


            # school_ids = list(set([item['school_id'] for item in group]))
            school_ids = list(set([item['school_id'] for item in teacher_student]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)

            group_map = {}
            # print (group)
            for g in group:
                group_map[g['id']] = g

            usergroup_map = defaultdict(list)
            usergroup_single_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            usergroup_map_grade_multi_map = defaultdict(list)
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                if usergroup_map[u_g['user_id']]:
                    usergroup_map[u_g['user_id']].append(u_g)
                else:
                    usergroup_map[u_g['user_id']] = [u_g]
                usergroup_single_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
                # print(u_g)
                if usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))]:
                    usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))].append(u_g)
                else:
                    usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))] = [u_g]

            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']
            # for t_s in teacher_student:
            #     school_channel_map[t_s['school_id']] = t_s['owner_id']

            teacher_student_map = {}
            for t_s in teacher_student:
                teacher_student_map[t_s['id']] = t_s


            class_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            school_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))

            class_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            school_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_student_number_defaultdict = defaultdict(lambda: defaultdict(list))

            # print(json.dumps(teacher_student, indent=4, cls=CustomEncoder))
            # print(json.dumps(usergroup_single_map, indent=4, cls=CustomEncoder))
            for t_s in teacher_student:
                # if t_s['school_id'] == 1:
                    # print(usergroup_map.get(t_s['id'], []))

                if int(t_s['role_id']) == Roles.TEACHER.value: #老师
                    for ug_map in usergroup_map.get(t_s['id'], []):
                        # print(ug_map)
                        # if int(ug_map['role_id']) == Roles.TEACHER.value: #老师
                            # 班级
                            # if t_s['school_id'] == 1 and str(ug_map.get("grade", -1)) == '2018':
                            #     print(ug_map)
                        class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['user_info'] = ug_map
                        if class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n']:
                            class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n'].append(1)
                        else:
                            class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n'] = [1]

                    #年级

                    # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    # key2 = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))+"@"+str(t_s['id'])



                    # if key == "1@2017":
                    #     print('------------')
                    #     print(ug_map)
                    #     print('-------------')
                    for u in usergroup_map[t_s['id']]:

                        key = str(u.get("school_id", -1)) + "@" + str(
                            u.get("grade", -1))
                        key2 = str(u.get("school_id", -1)) + "@" + str(
                            u.get("grade", -1)) + "@" + str(u['user_id'])
                        if grade_teacher_number_defaultdict[key]['n']:
                            grade_teacher_number_defaultdict[key]['n'].add(key2)
                        else:
                            grade_teacher_number_defaultdict[key]['n'] = set([key2])
                        if grade_teacher_number_defaultdict[key]['u']:
                            grade_teacher_number_defaultdict[key]['u'].append(t_s['id'])
                        else:
                            grade_teacher_number_defaultdict[key]['u'] = [t_s['id']]
                    # if t_s['id'] == 3341:
                    #     print('jinpeng/..............')
                    #     # print(usergroup_single_map.get(t_s['id'], {}))
                    #     # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(
                    #     #     usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    #     # print(usergroup_map[t_s['id']])
                    #     print(json.dumps(usergroup_map[t_s['id']], indent=4, cls=CustomEncoder))
                    #     # print(usergroup_map_grade_multi_map[key])
                    #     print('jinpeng/..............')
                elif int(t_s['role_id']) == Roles.STUDENT.value: #学生
                    for ug_map in usergroup_map.get(t_s['id'], []):
                        # 班级
                        class_student_number_defaultdict[ug_map.get('group_id', -1)]['user_info'] = ug_map
                        if class_student_number_defaultdict[ug_map.get('group_id', -1)]['n']:
                            class_student_number_defaultdict[ug_map.get('group_id', -1)]['n'].append(1)
                        else:
                            class_student_number_defaultdict[ug_map.get('group_id', -1)]['n'] = [1]
                else:
                    pass
                # print("usergroup_map_grade_multi_map", usergroup_map_grade_multi_map)
                # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                # print(len(usergroup_map_grade_multi_map.keys()))
                # if len(usergroup_map_grade_multi_map.keys()) > 10:
                #     print(len(usergroup_map_grade_multi_map.keys()))
                # for ug_mul_map in usergroup_map_grade_multi_map.get(key, []):
                #     # 年级
                #     if int(ug_mul_map['role_id']) == Roles.TEACHER.value: #老师
                #         # if key == "1@2017":
                #         #     print('------------')
                #         #     print(usergroup_map_grade_multi_map.get(key, []))
                #         #     print('-------------')
                #         # grade_teacher_number_defaultdict[str(ug_mul_map.get('school_id', -1))+"@"+str(ug_mul_map.get("grade", -1))]['user_info'] = ug_mul_map
                #         if grade_teacher_number_defaultdict[key]['n']:
                #             grade_teacher_number_defaultdict[key]['n'].append(1)
                #         else:
                #             grade_teacher_number_defaultdict[key]['n'] = [1]
                #     else:
                #         pass

                if int(t_s['role_id']) == Roles.TEACHER.value:  # 老师

                    #学校
                    school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n']:
                        school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'].append(1)
                    else:
                        school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'] = [1]

                    #渠道
                    channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n']:
                        channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'].append(1)
                    else:
                        channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'] = [1]
                elif int(t_s['role_id']) == Roles.STUDENT.value:  # 学生:
                    # 年级
                    school_grade_key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    grade_student_number_defaultdict[school_grade_key]['user_info'] = usergroup_single_map.get(t_s['id'], {})
                    if grade_student_number_defaultdict[school_grade_key]['n']:
                        grade_student_number_defaultdict[school_grade_key]['n'].append(1)
                    else:
                        grade_student_number_defaultdict[school_grade_key]['n'] = [1]

                    # 学校
                    school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n']:
                        school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'].append(1)
                    else:
                        school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'] = [1]
                    # 渠道
                    channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n']:
                        channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'].append(1)
                    else:
                        channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'] = [1]

                else:
                    pass

            # print(grade_teacher_number_defaultdict)
            # print(json.dumps(class_teacher_number_defaultdict,indent=4,cls=CustomEncoder))
            #老师
            #班级
            class_teacher_number_bulk_update = []
            for k, v in class_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("school_id", -1),
                    "grade": v['user_info'].get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "teacher_number": sum(v['n']),
                }

                class_teacher_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                            {'$set': user_number_schema}, upsert=True))
            #年级
            grade_teacher_number_bulk_update = []
            for k, v in grade_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": int(k.split("@")[0]),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "teacher_number": len(v['n']),
                    "teachers": v['u']
                }

                grade_teacher_number_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #学校
            school_teacher_number_bulk_update = []
            for k, v in school_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": k,
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(k, -1),
                    "teacher_number": sum(v['n']),
                }

                school_teacher_number_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #渠道
            channel_teacher_number_bulk_update = []
            for k, v in channel_teacher_number_defaultdict.items():
                user_number_schema = {
                    "teacher_number": sum(v['n']),
                }

                channel_teacher_number_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

            #学生
            #班级
            class_student_number_bulk_update = []
            for k, v in class_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("school_id", -1),
                    "grade": v['user_info'].get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "student_number": sum(v['n']),
                }

                class_student_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #年级
            grade_student_number_bulk_update = []
            for k, v in grade_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": int(k.split("@")[0]),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "student_number": sum(v['n']),
                }

                grade_student_number_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]),  "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            # 学校
            school_student_number_bulk_update = []
            for k, v in school_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": k,
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "student_number": sum(v['n']),
                }

                school_student_number_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                                   {'$set': user_number_schema}, upsert=True))
            #渠道
            channel_student_number_bulk_update = []
            for k, v in channel_student_number_defaultdict.items():
                user_number_schema = {
                    "student_number": sum(v['n']),
                }

                channel_student_number_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                                    {'$set': user_number_schema}, upsert=True))



            if class_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("teacher_student_number_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

    def _user_counts_today(self, date_range):
        """
        老师数，学生数 当日
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_teacher_student = select([us_user.c.id, us_user.c.role_id, us_user.c.school_id])\
                .where(and_(us_user.c.available == 1,
                            us_user.c.role_id.in_([Roles.TEACHER.value, Roles.STUDENT.value]),
                            us_user.c.time_create >= one_date[0],
                            us_user.c.time_create < one_date[1]
                            )
                       )

            teacher_student = self._query(q_teacher_student)

            user_ids = list(set([item['id'] for item in teacher_student]))



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


            # school_ids = list(set([item['school_id'] for item in group]))
            school_ids = list(set([item['school_id'] for item in teacher_student]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)

            group_map = {}
            # print (group)
            for g in group:
                group_map[g['id']] = g

            usergroup_map = defaultdict(list)
            usergroup_single_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            usergroup_map_grade_multi_map = defaultdict(list)
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                if usergroup_map[u_g['user_id']]:
                    usergroup_map[u_g['user_id']].append(u_g)
                else:
                    usergroup_map[u_g['user_id']] = [u_g]
                usergroup_single_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g
                # print(u_g)
                if usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))]:
                    usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))].append(u_g)
                else:
                    usergroup_map_grade_multi_map[str(u_g.get("school_id", -1)) + "@" + str(u_g.get("grade", -1))] = [u_g]

            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']
            # for t_s in teacher_student:
            #     school_channel_map[t_s['school_id']] = t_s['owner_id']

            teacher_student_map = {}
            for t_s in teacher_student:
                teacher_student_map[t_s['id']] = t_s


            class_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            school_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))

            class_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            school_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_student_number_defaultdict = defaultdict(lambda: defaultdict(list))

            # print(json.dumps(teacher_student, indent=4, cls=CustomEncoder))
            # print(json.dumps(usergroup_single_map, indent=4, cls=CustomEncoder))
            for t_s in teacher_student:
                # if t_s['school_id'] == 1:
                    # print(usergroup_map.get(t_s['id'], []))

                if int(t_s['role_id']) == Roles.TEACHER.value: #老师
                    for ug_map in usergroup_map.get(t_s['id'], []):
                        # print(ug_map)
                        # if int(ug_map['role_id']) == Roles.TEACHER.value: #老师
                            # 班级
                            # if t_s['school_id'] == 1 and str(ug_map.get("grade", -1)) == '2018':
                            #     print(ug_map)
                        class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['user_info'] = ug_map
                        if class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n']:
                            class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n'].append(1)
                        else:
                            class_teacher_number_defaultdict[ug_map.get('group_id', -1)]['n'] = [1]

                    #年级

                    # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    # key2 = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))+"@"+str(t_s['id'])



                    # if key == "1@2017":
                    #     print('------------')
                    #     print(ug_map)
                    #     print('-------------')
                    for u in usergroup_map[t_s['id']]:

                        key = str(u.get("school_id", -1)) + "@" + str(
                            u.get("grade", -1))
                        key2 = str(u.get("school_id", -1)) + "@" + str(
                            u.get("grade", -1)) + "@" + str(u['user_id'])
                        if grade_teacher_number_defaultdict[key]['n']:
                            grade_teacher_number_defaultdict[key]['n'].add(key2)
                        else:
                            grade_teacher_number_defaultdict[key]['n'] = set([key2])
                        if grade_teacher_number_defaultdict[key]['u']:
                            grade_teacher_number_defaultdict[key]['u'].append(t_s['id'])
                        else:
                            grade_teacher_number_defaultdict[key]['u'] = [t_s['id']]
                    # if t_s['id'] == 3341:
                    #     print('jinpeng/..............')
                    #     # print(usergroup_single_map.get(t_s['id'], {}))
                    #     # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(
                    #     #     usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    #     # print(usergroup_map[t_s['id']])
                    #     print(json.dumps(usergroup_map[t_s['id']], indent=4, cls=CustomEncoder))
                    #     # print(usergroup_map_grade_multi_map[key])
                    #     print('jinpeng/..............')
                elif int(t_s['role_id']) == Roles.STUDENT.value: #学生
                    for ug_map in usergroup_map.get(t_s['id'], []):
                        # 班级
                        class_student_number_defaultdict[ug_map.get('group_id', -1)]['user_info'] = ug_map
                        if class_student_number_defaultdict[ug_map.get('group_id', -1)]['n']:
                            class_student_number_defaultdict[ug_map.get('group_id', -1)]['n'].append(1)
                        else:
                            class_student_number_defaultdict[ug_map.get('group_id', -1)]['n'] = [1]
                else:
                    pass
                # print("usergroup_map_grade_multi_map", usergroup_map_grade_multi_map)
                # key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                # print(len(usergroup_map_grade_multi_map.keys()))
                # if len(usergroup_map_grade_multi_map.keys()) > 10:
                #     print(len(usergroup_map_grade_multi_map.keys()))
                # for ug_mul_map in usergroup_map_grade_multi_map.get(key, []):
                #     # 年级
                #     if int(ug_mul_map['role_id']) == Roles.TEACHER.value: #老师
                #         # if key == "1@2017":
                #         #     print('------------')
                #         #     print(usergroup_map_grade_multi_map.get(key, []))
                #         #     print('-------------')
                #         # grade_teacher_number_defaultdict[str(ug_mul_map.get('school_id', -1))+"@"+str(ug_mul_map.get("grade", -1))]['user_info'] = ug_mul_map
                #         if grade_teacher_number_defaultdict[key]['n']:
                #             grade_teacher_number_defaultdict[key]['n'].append(1)
                #         else:
                #             grade_teacher_number_defaultdict[key]['n'] = [1]
                #     else:
                #         pass

                if int(t_s['role_id']) == Roles.TEACHER.value:  # 老师

                    #学校
                    school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n']:
                        school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'].append(1)
                    else:
                        school_teacher_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'] = [1]

                    #渠道
                    channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n']:
                        channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'].append(1)
                    else:
                        channel_teacher_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'] = [1]
                elif int(t_s['role_id']) == Roles.STUDENT.value:  # 学生:
                    # 年级
                    school_grade_key = str(usergroup_single_map.get(t_s['id'], {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(t_s['id'], {}).get("grade", -1))
                    grade_student_number_defaultdict[school_grade_key]['user_info'] = usergroup_single_map.get(t_s['id'], {})
                    if grade_student_number_defaultdict[school_grade_key]['n']:
                        grade_student_number_defaultdict[school_grade_key]['n'].append(1)
                    else:
                        grade_student_number_defaultdict[school_grade_key]['n'] = [1]

                    # 学校
                    school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n']:
                        school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'].append(1)
                    else:
                        school_student_number_defaultdict[teacher_student_map.get(t_s['id'], {}).get("school_id", -1)]['n'] = [1]
                    # 渠道
                    channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['user_info'] = teacher_student_map.get(t_s['id'], {})
                    if channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n']:
                        channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'].append(1)
                    else:
                        channel_student_number_defaultdict[school_channel_map.get(teacher_student_map.get(t_s['id'], {}).get("school_id", -1), -1)]['n'] = [1]

                else:
                    pass

            # print(grade_teacher_number_defaultdict)
            # print(json.dumps(class_teacher_number_defaultdict,indent=4,cls=CustomEncoder))
            #老师
            #班级
            class_teacher_number_bulk_update = []
            for k, v in class_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("school_id", -1),
                    "grade": v['user_info'].get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "teacher_number": sum(v['n']),
                }

                class_teacher_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                            {'$set': user_number_schema}, upsert=True))
            #年级
            grade_teacher_number_bulk_update = []
            for k, v in grade_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": int(k.split("@")[0]),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "teacher_number": len(v['n']),
                    "teachers": v['u']
                }

                grade_teacher_number_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]), "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #学校
            school_teacher_number_bulk_update = []
            for k, v in school_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": k,
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(k, -1),
                    "teacher_number": sum(v['n']),
                }

                school_teacher_number_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #渠道
            channel_teacher_number_bulk_update = []
            for k, v in channel_teacher_number_defaultdict.items():
                user_number_schema = {
                    "teacher_number": sum(v['n']),
                }

                channel_teacher_number_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

            #学生
            #班级
            class_student_number_bulk_update = []
            for k, v in class_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("school_id", -1),
                    "grade": v['user_info'].get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "student_number": sum(v['n']),
                }

                class_student_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            #年级
            grade_student_number_bulk_update = []
            for k, v in grade_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": int(k.split("@")[0]),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(int(k.split("@")[0]), -1),
                    "student_number": sum(v['n']),
                }

                grade_student_number_bulk_update.append(UpdateOne({"grade": k.split("@")[1], "school_id": int(k.split("@")[0]),  "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))
            # 学校
            school_student_number_bulk_update = []
            for k, v in school_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": k,
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": school_channel_map.get(v['user_info'].get("school_id", -1), -1),
                    "student_number": sum(v['n']),
                }

                school_student_number_bulk_update.append(UpdateOne({"school_id": k, "day": one_date[0]},
                                                                   {'$set': user_number_schema}, upsert=True))
            #渠道
            channel_student_number_bulk_update = []
            for k, v in channel_student_number_defaultdict.items():
                user_number_schema = {
                    "student_number": sum(v['n']),
                }

                channel_student_number_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                                    {'$set': user_number_schema}, upsert=True))



            if class_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_teacher_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_student_number_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

class PerDayTask_VALIDCONTEST(BaseTask):
    def __init__(self):
        super(PerDayTask_VALIDCONTEST, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.grade_coll = "grade"
        self.grade_today_coll = "grade_today"
        self.school_coll = "school"
        self.school_today_coll = "school_today"
        self.cursor = None
        self.connection = None
    def _query(self, query):
        """
        执行查询 返回数据库结果
        """

        self.cursor = self.connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret


    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("valid_exercise_word_begin_time") #时间分段
            # date_range = [("2018-05-01", "2018-06-05")]
            self._exercise_number(date_range) #有效考试 有效词汇
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=10)

    def _exercise_number(self, date_range):
        """
        有效考试，有效单词考试， 有效考试学生数，考试日期，考试创建时间
        :param date_range:
        :return:
        """

        for one_date in date_range:
            q_hermes = select([as_hermes.c.exercise_id,
                               as_hermes.c.student_id,
                               as_hermes.c.time_create])\
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.time_create >= one_date[0],
                            as_hermes.c.time_create < one_date[1]
                            )
                       )
            exercise_word_images = self._query(q_hermes)

            user_ids = list(set([item['student_id'] for item in exercise_word_images]))
            exercise_ids = list(set([item['exercise_id'] for item in exercise_word_images])) #考试id 包括考试和单词
            q_exercise_word_meta = select([ob_exercisemeta]).where(and_(
                ob_exercisemeta.c.available == 1,
                ob_exercisemeta.c.exercise_id.in_(exercise_ids),
                ob_exercisemeta.c.value == '"word"'
            ))
            exercise_word_meta = self._query(q_exercise_word_meta)

            word_exercise_ids = set([item['exercise_id'] for item in exercise_word_meta]) #单词考试id


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

            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            exercise_unbind_user_exercise_ids = []
            user_exercise_images = []
            user_word_images = []
            for e_w_images in exercise_word_images:
                if e_w_images['student_id'] == 0:
                    exercise_unbind_user_exercise_ids.append(e_w_images['exercise_id'])
                    continue
                if e_w_images.get("user_info", []):
                    e_w_images.append(usergroup_single_map.get(e_w_images.get("student_id", -1), {}))
                else:
                    e_w_images['user_info'] = [
                        usergroup_single_map.get(e_w_images.get("student_id", -1))] if usergroup_single_map.get(
                        e_w_images.get("student_id", -1)) else []
                if e_w_images['exercise_id'] not in word_exercise_ids: #考试
                    user_exercise_images.append(e_w_images)
                else:
                    user_word_images.append(e_w_images)

            #处理考试
            class_exercise_image_number = defaultdict(lambda : defaultdict(dict))
            grade_exercise_image_number = defaultdict(lambda : defaultdict(dict))
            school_exercise_image_number = defaultdict(lambda: defaultdict(dict))
            channel_exercise_image_number = defaultdict(lambda: defaultdict(dict))
            for u_e_i in user_exercise_images:
                #班级
                for u in u_e_i['user_info']:
                    if class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id']:
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'].append(u['user_id'])
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'] = list(set(class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id']))
                    else:
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'] = [u['user_id']]
                #年级
                exercise_school_grade_key = str(u_e_i['exercise_id']) +"@"+ str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("grade", -1))
                if grade_exercise_image_number[exercise_school_grade_key]['user_id']:
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'].append(u_e_i['student_id'])
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'] = list(set(grade_exercise_image_number[exercise_school_grade_key]['user_id']))
                else:
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'] = [u_e_i['student_id']]
                #学校
                if school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']:
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'].append(u_e_i['student_id'])
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = list(set(school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']))
                else:
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id'] = [
                        u_e_i['student_id']]
                #渠道
                if channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id']:
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'].append(u_e_i['student_id'])
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'] = list(set(channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id']))
                else:
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'] = [u_e_i['student_id']]

            #班级和历史比较
            history_class_exercise_delta_ids = list(class_exercise_image_number.keys())
            history_class_exercise_delta = self.mongo.record_class_exercise_delta.find({"exercise_group_id": {"$in": history_class_exercise_delta_ids}})
            history_class_exercise_id_map = {}
            class_exercise_bulk_update = []
            history_class_exercise_bulk_update = []
            for h in history_class_exercise_delta:
                history_class_exercise_id_map[h['exercise_group_id']] = h

            for exercise_group_id, data in class_exercise_image_number.items():
                history_students = history_class_exercise_id_map.get(exercise_group_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students+today_students))
                history_class_exercise_bulk_update.append(UpdateOne({"exercise_group_id": exercise_group_id}, {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_class_exercise_id_map.get(exercise_group_id, {}).get("status", 0) == 0:
                        class_exercise_bulk_update.append(UpdateOne({"group_id": int(exercise_group_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_class_exercise_bulk_update.append(UpdateOne({"exercise_group_id": exercise_group_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))
                # self.mongo.record_class_exercise_delta.update_one({"exercise_group_id":exercise_group_id }, {"$set": {"user_id": data['user_id']}}, upsert=True)

            #年级和历史比较 额外增加年级阶段的判断
            history_grade_exercise_delta_ids = list(grade_exercise_image_number.keys())
            history_grade_exercise_delta = self.mongo.record_grade_exercise_delta.find({"exercise_school_grade_id": {"$in": history_grade_exercise_delta_ids}})
            history_grade_exercise_id_map = {}
            grade_exercise_bulk_update = []
            history_grade_exercise_bulk_update = []
            grade_stage_bulk_update = []
            for h in history_grade_exercise_delta:
                history_grade_exercise_id_map[h['exercise_school_grade_id']] = h

            for exercise_school_grade_id, data in grade_exercise_image_number.items():
                history_students = history_grade_exercise_id_map.get(exercise_school_grade_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_grade_exercise_bulk_update.append(UpdateOne({"exercise_school_grade_id": exercise_school_grade_id}, {'$set': {"user_id": today_total_students}},upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_grade_exercise_id_map.get(exercise_school_grade_id, {}).get("status", 0) == 0:
                        self.check_school_stage(usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1), one_date[0])
                        grade_stage_bulk_update.append(
                            UpdateOne({"grade": exercise_school_grade_id.split("@")[2], "school_id": usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1)},
                                      {'$set': {"school_id": usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1),
                                                "channel": school_channel_map.get(usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1)),
                                                "using_time": one_date[0],
                                                "stage": StageEnum.Using.value}}, upsert=True))
                        grade_exercise_bulk_update.append(UpdateOne({"grade": exercise_school_grade_id.split("@")[2], "school_id": int(exercise_school_grade_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_grade_exercise_bulk_update.append(UpdateOne({"exercise_school_grade_id": exercise_school_grade_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 学校和历史比较
            history_school_exercise_delta_ids = list(school_exercise_image_number.keys())
            history_school_exercise_delta = self.mongo.record_school_exercise_delta.find(
                {"exercise_school_id": {"$in": history_school_exercise_delta_ids}})
            history_school_exercise_id_map = {}
            school_exercise_bulk_update = []
            history_school_exercise_bulk_update = []
            for h in history_school_exercise_delta:
                history_school_exercise_id_map[h['exercise_school_id']] = h

            for exercise_school_id, data in school_exercise_image_number.items():
                history_students = history_school_exercise_id_map.get(exercise_school_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_school_exercise_bulk_update.append(
                    UpdateOne({"exercise_school_id": exercise_school_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_exercise_id_map.get(exercise_school_id, {}).get("status", 0) == 0:
                        school_exercise_bulk_update.append(UpdateOne(
                            {"school_id": int(exercise_school_id.split('@')[1]), "day": one_date[0]},
                            {'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_school_exercise_bulk_update.append(
                        UpdateOne({"exercise_school_id": exercise_school_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #渠道和历史比较
            history_channel_exercise_delta_ids = list(channel_exercise_image_number.keys())
            history_channel_exercise_delta = self.mongo.record_channel_exercise_delta.find(
                {"exercise_channel_id": {"$in": history_channel_exercise_delta_ids}})
            history_channel_exercise_id_map = {}
            channel_exercise_bulk_update = []
            history_channel_exercise_bulk_update = []
            for h in history_channel_exercise_delta:
                history_channel_exercise_id_map[h['exercise_channel_id']] = h

            for exercise_channel_id, data in channel_exercise_image_number.items():
                history_students = history_channel_exercise_id_map.get(exercise_channel_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_channel_exercise_bulk_update.append(UpdateOne({"exercise_channel_id": exercise_channel_id}, {'$set': {"user_id": today_total_students}},upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_channel_exercise_id_map.get(exercise_channel_id, {}).get("status", 0) == 0:
                        channel_exercise_bulk_update.append(UpdateOne({"channel": int(exercise_channel_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_channel_exercise_bulk_update.append(UpdateOne({"exercise_channel_id": exercise_channel_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 处理单词考试
            class_word_image_number = defaultdict(lambda: defaultdict(dict))
            grade_word_image_number = defaultdict(lambda: defaultdict(dict))
            school_word_image_number = defaultdict(lambda: defaultdict(dict))
            channel_word_image_number = defaultdict(lambda: defaultdict(dict))
            for u_e_i in user_word_images:
                # 班级
                for u in u_e_i['user_info']:
                    if class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                        'user_id']:
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'].append(u['user_id'])
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'] = list(set(class_word_image_number[
                                                      str(u_e_i['exercise_id']) + "@" + str(
                                                          usergroup_single_map.get(u.get("user_id", -1),
                                                                                   {}).get("group_id",
                                                                                           -1))][
                                                      'user_id']))
                    else:
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'] = [u['user_id']]
                # 年级
                exercise_school_group_key = str(u_e_i['exercise_id']) + "@"+ str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1)) +"@" + str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("grade", -1))
                if grade_word_image_number[exercise_school_group_key]['user_id']:
                    grade_word_image_number[exercise_school_group_key]['user_id'].append(u_e_i['student_id'])
                    grade_word_image_number[exercise_school_group_key]['user_id'] = list(set(grade_word_image_number[exercise_school_group_key]['user_id']))
                else:
                    grade_word_image_number[exercise_school_group_key]['user_id'] = [u_e_i['student_id']]

                # 学校
                if school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']:
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'].append(u_e_i['student_id'])
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = list(set(school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                                                  'user_id']))
                else:
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = [
                        u_e_i['student_id']]
                # 渠道
                if channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                                usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id",
                                                                                              -1), -1))][
                    'user_id']:
                    channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'].append(u_e_i['student_id'])
                    channel_word_image_number[
                        str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'] = list(set(channel_word_image_number[
                                                            str(u_e_i['exercise_id']) + "@" + str(
                                                                school_channel_map.get(
                                                                    usergroup_single_map.get(
                                                                        u_e_i.get("student_id", -1),
                                                                        {}).get("school_id", -1), -1))][
                                                            'user_id']))
                else:
                    channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'] = [u_e_i['student_id']]

            # 班级和历史比较
            history_class_word_delta_ids = list(class_word_image_number.keys())
            history_class_word_delta = self.mongo.record_class_word_delta.find(
                {"word_group_id": {"$in": history_class_word_delta_ids}})
            history_class_word_id_map = {}
            class_word_bulk_update = []
            history_class_word_bulk_update = []
            for h in history_class_word_delta:
                history_class_word_id_map[h['word_group_id']] = h

            for word_group_id, data in class_word_image_number.items():
                history_students = history_class_word_id_map.get(word_group_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_class_word_bulk_update.append(
                    UpdateOne({"word_group_id": word_group_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_class_word_id_map.get(word_group_id, {}).get("status", 0) == 0:
                        class_word_bulk_update.append(UpdateOne(
                            {"group_id": int(word_group_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_class_word_bulk_update.append(
                        UpdateOne({"word_group_id": word_group_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))
                # self.mongo.record_class_exercise_delta.update_one({"exercise_group_id":exercise_group_id }, {"$set": {"user_id": data['user_id']}}, upsert=True)

            # 年级和历史比较
            history_grade_word_delta_ids = list(grade_word_image_number.keys())
            history_grade_word_delta = self.mongo.record_grade_word_delta.find(
                {"word_school_grade_id": {"$in": history_grade_word_delta_ids}})
            history_grade_word_id_map = {}
            grade_word_bulk_update = []
            history_grade_word_bulk_update = []
            for h in history_grade_word_delta:
                history_grade_word_id_map[h['word_school_grade_id']] = h

            for word_school_grade_id, data in grade_word_image_number.items():
                history_students = history_grade_word_id_map.get(word_school_grade_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_grade_word_bulk_update.append(
                    UpdateOne({"word_school_grade_id": word_school_grade_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_grade_word_id_map.get(word_school_grade_id, {}).get("status", 0) == 0:
                        grade_word_bulk_update.append(
                            UpdateOne({"grade": word_school_grade_id.split("@")[2], "school_id": int(word_school_grade_id.split("@")[1]), "day": one_date[0]},
                                      {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_grade_word_bulk_update.append(
                        UpdateOne({"word_grade_id": word_school_grade_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 学校和历史比较
            history_school_word_delta_ids = list(school_word_image_number.keys())
            history_school_word_delta = self.mongo.record_school_word_delta.find(
                {"word_school_id": {"$in": history_school_word_delta_ids}})
            history_school_word_id_map = {}
            school_word_bulk_update = []
            history_school_word_bulk_update = []
            for h in history_school_word_delta:
                history_school_word_id_map[h['word_school_id']] = h

            for word_school_id, data in school_word_image_number.items():
                history_students = history_school_word_id_map.get(word_school_id, {}).get("user_id",
                                                                                                  [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_school_word_bulk_update.append(
                    UpdateOne({"word_school_id": word_school_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_word_id_map.get(word_school_id, {}).get("status", 0) == 0:
                        school_word_bulk_update.append(UpdateOne(
                            {"school_id": int(word_school_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_school_word_bulk_update.append(
                        UpdateOne({"word_school_id": word_school_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 渠道和历史比较
            history_channel_word_delta_ids = list(channel_word_image_number.keys())
            history_channel_word_delta = self.mongo.record_channel_word_delta.find(
                {"word_channel_id": {"$in": history_channel_word_delta_ids}})
            history_channel_word_id_map = {}
            channel_word_bulk_update = []
            history_channel_word_bulk_update = []
            for h in history_channel_word_delta:
                history_channel_word_id_map[h['word_channel_id']] = h

            for word_channel_id, data in channel_word_image_number.items():
                history_students = history_channel_word_id_map.get(word_channel_id, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_channel_word_bulk_update.append(
                    UpdateOne({"word_channel_id": word_channel_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_channel_word_id_map.get(word_channel_id, {}).get("status", 0) == 0:
                        channel_word_bulk_update.append(UpdateOne(
                            {"channel": int(word_channel_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_channel_word_bulk_update.append(
                        UpdateOne({"word_channel_id": word_channel_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))



            #考试
            if grade_stage_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade.bulk_write(grade_stage_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if school_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_class_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_exercise_delta.bulk_write(history_class_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_exercise_delta.bulk_write(history_grade_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_exercise_delta.bulk_write(history_school_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_exercise_delta.bulk_write(history_channel_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            #词汇
            if class_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_word_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_class_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_word_delta.bulk_write(history_class_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_word_delta.bulk_write(history_grade_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_word_delta.bulk_write(history_school_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if history_channel_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_word_delta.bulk_write(history_channel_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            self._set_time_threadshold("valid_exercise_word_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))


    def _exercise_number_today(self, date_range):
        """
        有效考试，有效单词考试， 有效考试学生数，考试日期，考试创建时间   当天
        :param date_range:
        :return:
        """

        for one_date in date_range:
            q_hermes = select([as_hermes.c.exercise_id,
                               as_hermes.c.student_id,
                               as_hermes.c.time_create])\
                .where(and_(as_hermes.c.available == 1,
                            as_hermes.c.time_create >= one_date[0],
                            as_hermes.c.time_create < one_date[1]
                            )
                       )
            exercise_word_images = self._query(q_hermes)

            user_ids = list(set([item['student_id'] for item in exercise_word_images]))
            exercise_ids = list(set([item['exercise_id'] for item in exercise_word_images])) #考试id 包括考试和单词
            q_exercise_word_meta = select([ob_exercisemeta]).where(and_(
                ob_exercisemeta.c.available == 1,
                ob_exercisemeta.c.exercise_id.in_(exercise_ids),
                ob_exercisemeta.c.value == '"word"'
            ))
            exercise_word_meta = self._query(q_exercise_word_meta)

            word_exercise_ids = set([item['exercise_id'] for item in exercise_word_meta]) #单词考试id


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

            usergroup_map = defaultdict(list)
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
                # print(json.dumps(u_g, indent=4, cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            exercise_unbind_user_exercise_ids = []
            user_exercise_images = []
            user_word_images = []
            for e_w_images in exercise_word_images:
                if e_w_images['student_id'] == 0:
                    exercise_unbind_user_exercise_ids.append(e_w_images['exercise_id'])
                    continue
                if e_w_images.get("user_info", []):
                    e_w_images.append(usergroup_single_map.get(e_w_images.get("student_id", -1), {}))
                else:
                    e_w_images['user_info'] = [
                        usergroup_single_map.get(e_w_images.get("student_id", -1))] if usergroup_single_map.get(
                        e_w_images.get("student_id", -1)) else []
                if e_w_images['exercise_id'] not in word_exercise_ids: #考试
                    user_exercise_images.append(e_w_images)
                else:
                    user_word_images.append(e_w_images)

            #处理考试
            class_exercise_image_number = defaultdict(lambda : defaultdict(dict))
            grade_exercise_image_number = defaultdict(lambda : defaultdict(dict))
            school_exercise_image_number = defaultdict(lambda: defaultdict(dict))
            channel_exercise_image_number = defaultdict(lambda: defaultdict(dict))
            for u_e_i in user_exercise_images:
                #班级
                for u in u_e_i['user_info']:
                    if class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id']:
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'].append(u['user_id'])
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'] = list(set(class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id']))
                    else:
                        class_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))]['user_id'] = [u['user_id']]
                #年级
                exercise_school_grade_key = str(u_e_i['exercise_id']) +"@"+ str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1)) + "@" + str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("grade", -1))
                if grade_exercise_image_number[exercise_school_grade_key]['user_id']:
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'].append(u_e_i['student_id'])
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'] = list(set(grade_exercise_image_number[exercise_school_grade_key]['user_id']))
                else:
                    grade_exercise_image_number[exercise_school_grade_key]['user_id'] = [u_e_i['student_id']]
                #学校
                if school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']:
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'].append(u_e_i['student_id'])
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = list(set(school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']))
                else:
                    school_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id'] = [
                        u_e_i['student_id']]
                #渠道
                if channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id']:
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'].append(u_e_i['student_id'])
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'] = list(set(channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id']))
                else:
                    channel_exercise_image_number[str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1), -1))]['user_id'] = [u_e_i['student_id']]

            #班级和历史比较
            history_class_exercise_delta_ids = list(class_exercise_image_number.keys())
            history_class_exercise_delta = self.mongo.record_class_today_exercise_delta.find({"exercise_group_id": {"$in": history_class_exercise_delta_ids}})
            history_class_exercise_id_map = {}
            class_exercise_bulk_update = []
            history_class_exercise_bulk_update = []
            for h in history_class_exercise_delta:
                history_class_exercise_id_map[h['exercise_group_id']] = h

            for exercise_group_id, data in class_exercise_image_number.items():
                history_students = history_class_exercise_id_map.get(exercise_group_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students+today_students))
                history_class_exercise_bulk_update.append(UpdateOne({"exercise_group_id": exercise_group_id}, {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_class_exercise_id_map.get(exercise_group_id, {}).get("status", 0) == 0:
                        class_exercise_bulk_update.append(UpdateOne({"group_id": int(exercise_group_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_class_exercise_bulk_update.append(UpdateOne({"exercise_group_id": exercise_group_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))
                # self.mongo.record_class_exercise_delta.update_one({"exercise_group_id":exercise_group_id }, {"$set": {"user_id": data['user_id']}}, upsert=True)

            #年级和历史比较 额外增加年级阶段的判断
            history_grade_exercise_delta_ids = list(grade_exercise_image_number.keys())
            history_grade_exercise_delta = self.mongo.record_grade_today_exercise_delta.find({"exercise_school_grade_id": {"$in": history_grade_exercise_delta_ids}})
            history_grade_exercise_id_map = {}
            grade_exercise_bulk_update = []
            history_grade_exercise_bulk_update = []
            grade_stage_bulk_update = []
            for h in history_grade_exercise_delta:
                history_grade_exercise_id_map[h['exercise_school_grade_id']] = h

            for exercise_school_grade_id, data in grade_exercise_image_number.items():
                history_students = history_grade_exercise_id_map.get(exercise_school_grade_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_grade_exercise_bulk_update.append(UpdateOne({"exercise_school_grade_id": exercise_school_grade_id}, {'$set': {"user_id": today_total_students}},upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_grade_exercise_id_map.get(exercise_school_grade_id, {}).get("status", 0) == 0:
                        self.check_school_stage_today(usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1), one_date[0])
                        grade_stage_bulk_update.append(
                            UpdateOne({"grade": exercise_school_grade_id.split("@")[2], "school_id": usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1)},
                                      {'$set': {"school_id": usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1),
                                                "channel": school_channel_map.get(usergroup_single_map.get(today_total_students[0], {}).get("school_id", -1)),
                                                "using_time": one_date[0],
                                                "stage": StageEnum.Using.value}}, upsert=True))
                        grade_exercise_bulk_update.append(UpdateOne({"grade": exercise_school_grade_id.split("@")[2], "school_id": int(exercise_school_grade_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_grade_exercise_bulk_update.append(UpdateOne({"exercise_school_grade_id": exercise_school_grade_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 学校和历史比较
            history_school_exercise_delta_ids = list(school_exercise_image_number.keys())
            history_school_exercise_delta = self.mongo.record_school_today_exercise_delta.find(
                {"exercise_school_id": {"$in": history_school_exercise_delta_ids}})
            history_school_exercise_id_map = {}
            school_exercise_bulk_update = []
            history_school_exercise_bulk_update = []
            for h in history_school_exercise_delta:
                history_school_exercise_id_map[h['exercise_school_id']] = h

            for exercise_school_id, data in school_exercise_image_number.items():
                history_students = history_school_exercise_id_map.get(exercise_school_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_school_exercise_bulk_update.append(
                    UpdateOne({"exercise_school_id": exercise_school_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_exercise_id_map.get(exercise_school_id, {}).get("status", 0) == 0:
                        school_exercise_bulk_update.append(UpdateOne(
                            {"school_id": int(exercise_school_id.split('@')[1]), "day": one_date[0]},
                            {'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_school_exercise_bulk_update.append(
                        UpdateOne({"exercise_school_id": exercise_school_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #渠道和历史比较
            history_channel_exercise_delta_ids = list(channel_exercise_image_number.keys())
            history_channel_exercise_delta = self.mongo.record_channel_today_exercise_delta.find(
                {"exercise_channel_id": {"$in": history_channel_exercise_delta_ids}})
            history_channel_exercise_id_map = {}
            channel_exercise_bulk_update = []
            history_channel_exercise_bulk_update = []
            for h in history_channel_exercise_delta:
                history_channel_exercise_id_map[h['exercise_channel_id']] = h

            for exercise_channel_id, data in channel_exercise_image_number.items():
                history_students = history_channel_exercise_id_map.get(exercise_channel_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_channel_exercise_bulk_update.append(UpdateOne({"exercise_channel_id": exercise_channel_id}, {'$set': {"user_id": today_total_students}},upsert=True))
                if len(today_total_students) >= 10: #有效
                    if history_channel_exercise_id_map.get(exercise_channel_id, {}).get("status", 0) == 0:
                        channel_exercise_bulk_update.append(UpdateOne({"channel": int(exercise_channel_id.split("@")[1]), "day": one_date[0]},{'$inc': {"valid_exercise_count": 1}}, upsert=True))
                    history_channel_exercise_bulk_update.append(UpdateOne({"exercise_channel_id": exercise_channel_id},{'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 处理单词考试
            class_word_image_number = defaultdict(lambda: defaultdict(dict))
            grade_word_image_number = defaultdict(lambda: defaultdict(dict))
            school_word_image_number = defaultdict(lambda: defaultdict(dict))
            channel_word_image_number = defaultdict(lambda: defaultdict(dict))
            for u_e_i in user_word_images:
                # 班级
                for u in u_e_i['user_info']:
                    if class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                        'user_id']:
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'].append(u['user_id'])
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'] = list(set(class_word_image_number[
                                                      str(u_e_i['exercise_id']) + "@" + str(
                                                          usergroup_single_map.get(u.get("user_id", -1),
                                                                                   {}).get("group_id",
                                                                                           -1))][
                                                      'user_id']))
                    else:
                        class_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                            usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1))][
                            'user_id'] = [u['user_id']]
                # 年级
                exercise_school_group_key = str(u_e_i['exercise_id']) + "@"+ str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1)) +"@" + str(usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("grade", -1))
                if grade_word_image_number[exercise_school_group_key]['user_id']:
                    grade_word_image_number[exercise_school_group_key]['user_id'].append(u_e_i['student_id'])
                    grade_word_image_number[exercise_school_group_key]['user_id'] = list(set(grade_word_image_number[exercise_school_group_key]['user_id']))
                else:
                    grade_word_image_number[exercise_school_group_key]['user_id'] = [u_e_i['student_id']]

                # 学校
                if school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))]['user_id']:
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'].append(u_e_i['student_id'])
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = list(set(school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                                                  'user_id']))
                else:
                    school_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1))][
                        'user_id'] = [
                        u_e_i['student_id']]
                # 渠道
                if channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                                usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id",
                                                                                              -1), -1))][
                    'user_id']:
                    channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'].append(u_e_i['student_id'])
                    channel_word_image_number[
                        str(u_e_i['exercise_id']) + "@" + str(school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'] = list(set(channel_word_image_number[
                                                            str(u_e_i['exercise_id']) + "@" + str(
                                                                school_channel_map.get(
                                                                    usergroup_single_map.get(
                                                                        u_e_i.get("student_id", -1),
                                                                        {}).get("school_id", -1), -1))][
                                                            'user_id']))
                else:
                    channel_word_image_number[str(u_e_i['exercise_id']) + "@" + str(
                        school_channel_map.get(
                            usergroup_single_map.get(u_e_i.get("student_id", -1), {}).get("school_id", -1),
                            -1))]['user_id'] = [u_e_i['student_id']]

            # 班级和历史比较
            history_class_word_delta_ids = list(class_word_image_number.keys())
            history_class_word_delta = self.mongo.record_class_today_word_delta.find(
                {"word_group_id": {"$in": history_class_word_delta_ids}})
            history_class_word_id_map = {}
            class_word_bulk_update = []
            history_class_word_bulk_update = []
            for h in history_class_word_delta:
                history_class_word_id_map[h['word_group_id']] = h

            for word_group_id, data in class_word_image_number.items():
                history_students = history_class_word_id_map.get(word_group_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_class_word_bulk_update.append(
                    UpdateOne({"word_group_id": word_group_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_class_word_id_map.get(word_group_id, {}).get("status", 0) == 0:
                        class_word_bulk_update.append(UpdateOne(
                            {"group_id": int(word_group_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_class_word_bulk_update.append(
                        UpdateOne({"word_group_id": word_group_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))
                # self.mongo.record_class_exercise_delta.update_one({"exercise_group_id":exercise_group_id }, {"$set": {"user_id": data['user_id']}}, upsert=True)

            # 年级和历史比较
            history_grade_word_delta_ids = list(grade_word_image_number.keys())
            history_grade_word_delta = self.mongo.record_grade_today_word_delta.find(
                {"word_school_grade_id": {"$in": history_grade_word_delta_ids}})
            history_grade_word_id_map = {}
            grade_word_bulk_update = []
            history_grade_word_bulk_update = []
            for h in history_grade_word_delta:
                history_grade_word_id_map[h['word_school_grade_id']] = h

            for word_school_grade_id, data in grade_word_image_number.items():
                history_students = history_grade_word_id_map.get(word_school_grade_id, {}).get("user_id",
                                                                                                [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_grade_word_bulk_update.append(
                    UpdateOne({"word_school_grade_id": word_school_grade_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_grade_word_id_map.get(word_school_grade_id, {}).get("status", 0) == 0:
                        grade_word_bulk_update.append(
                            UpdateOne({"grade": word_school_grade_id.split("@")[2], "school_id": int(word_school_grade_id.split("@")[1]), "day": one_date[0]},
                                      {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_grade_word_bulk_update.append(
                        UpdateOne({"word_grade_id": word_school_grade_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 学校和历史比较
            history_school_word_delta_ids = list(school_word_image_number.keys())
            history_school_word_delta = self.mongo.record_school_today_word_delta.find(
                {"word_school_id": {"$in": history_school_word_delta_ids}})
            history_school_word_id_map = {}
            school_word_bulk_update = []
            history_school_word_bulk_update = []
            for h in history_school_word_delta:
                history_school_word_id_map[h['word_school_id']] = h

            for word_school_id, data in school_word_image_number.items():
                history_students = history_school_word_id_map.get(word_school_id, {}).get("user_id",
                                                                                                  [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_school_word_bulk_update.append(
                    UpdateOne({"word_school_id": word_school_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_word_id_map.get(word_school_id, {}).get("status", 0) == 0:
                        school_word_bulk_update.append(UpdateOne(
                            {"school_id": int(word_school_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_school_word_bulk_update.append(
                        UpdateOne({"word_school_id": word_school_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            # 渠道和历史比较
            history_channel_word_delta_ids = list(channel_word_image_number.keys())
            history_channel_word_delta = self.mongo.record_channel_today_word_delta.find(
                {"word_channel_id": {"$in": history_channel_word_delta_ids}})
            history_channel_word_id_map = {}
            channel_word_bulk_update = []
            history_channel_word_bulk_update = []
            for h in history_channel_word_delta:
                history_channel_word_id_map[h['word_channel_id']] = h

            for word_channel_id, data in channel_word_image_number.items():
                history_students = history_channel_word_id_map.get(word_channel_id, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                history_channel_word_bulk_update.append(
                    UpdateOne({"word_channel_id": word_channel_id},
                              {'$set': {"user_id": today_total_students}}, upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_channel_word_id_map.get(word_channel_id, {}).get("status", 0) == 0:
                        channel_word_bulk_update.append(UpdateOne(
                            {"channel": int(word_channel_id.split("@")[1]), "day": one_date[0]},
                            {'$inc': {"valid_word_count": 1}}, upsert=True))
                    history_channel_word_bulk_update.append(
                        UpdateOne({"word_channel_id": word_channel_id},
                                  {'$set': {"last_day": one_date[0], "status": 1}}, upsert=True))



            #考试
            if grade_stage_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_stage_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if school_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_class_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_today_exercise_delta.bulk_write(history_class_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_today_exercise_delta.bulk_write(history_grade_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_today_exercise_delta.bulk_write(history_school_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_exercise_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_today_exercise_delta.bulk_write(history_channel_exercise_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            #词汇
            if class_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_word_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_class_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_word_today_delta.bulk_write(history_class_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_word_today_delta.bulk_write(history_grade_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_word_today_delta.bulk_write(history_school_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            if history_channel_word_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_word_today_delta.bulk_write(history_channel_word_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

    def check_school_stage(self, school_id, date):
        """
        检查并修改学校阶段
        :param self:
        :param school_id:
        :return:
        """
        q_grade  = select([ob_group.c.grade, ob_group.c.school_id])\
                .where(and_(ob_group.c.available == 1,
                            ob_group.c.school_id == school_id
                            )
                       ).group_by(ob_group.c.school_id).group_by(ob_group.c.grade)

        grades_mysql = self._query(q_grade)
        grades = self.mongo[self.grade_coll].find({"school_id": school_id})
        grade_map = {}
        for g in grades:
            grade_map[g['grade']] = g
        stage = []
        for grade in grades_mysql:
            stage.append(grade_map.get(grade['grade'], {}).get('stage', StageEnum.Register.value))
        final_stage = StageEnum.Register.value if not stage else min(stage)
        if final_stage >= 1:
            schema = {
                "stage": final_stage, "using_time": date
            }

            self.mongo[self.school_coll].update_one({"school_id": school_id,
                                                 "stage": {"$in": [StageEnum.Register.value, StageEnum.Using.value]}},
                                                {"$set": schema}, upsert=True)

    def check_school_stage_today(self, school_id, date):
        """
        检查并修改学校阶段
        :param self:
        :param school_id:
        :return:
        """
        q_grade = select([ob_group.c.grade, ob_group.c.school_id]) \
            .where(and_(ob_group.c.available == 1,
                        ob_group.c.school_id == school_id
                        )
                   ).group_by(ob_group.c.school_id).group_by(ob_group.c.grade)

        grades_mysql = self._query(q_grade)
        grades = self.mongo[self.grade_today_coll].find({"school_id": school_id})
        grade_map = {}
        for g in grades:
            grade_map[g['grade']] = g
        stage = []
        for grade in grades_mysql:
            stage.append(grade_map.get(grade['grade'], {}).get('stage', StageEnum.Register.value))
        final_stage = StageEnum.Register.value if not stage else min(stage)
        if final_stage >= 1:
            schema = {
                "stage": final_stage, "using_time": date
            }

            self.mongo[self.school_today_coll].update_one({"school_id": school_id,
                                                     "stage": {"$in": [StageEnum.Register.value,
                                                                       StageEnum.Using.value]}},
                                                    {
    "$set": schema}, upsert=True)

class PerDayTask_VALIDREADING(BaseTask):
    """
    有效阅读
    """
    def __init__(self):
        super(PerDayTask_VALIDREADING, self).__init__()
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.reading_delta_record_coll = "record_reading_delta"
        self.cursor = None
        self.connection = None
    def _query(self, query):
        """
        执行查询 返回数据库结果
        """

        self.cursor = self.connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        self.cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = self.cursor.fetchall()
        return ret


    def run(self):
        try:
            self.connection = self.get_connection()
            date_range = self._date_range("valid_reading_begin_time") #时间分段
            # date_range = [("2018-09-20", "2018-09-21")]
            self._reading_number(date_range) #有效阅读
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise self.retry(exc=e, countdown=30, max_retries=10)


    def _reading_number(self, date_range):
        """
        有效阅读
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_reading_in_date = select([ob_reading])\
                .where(and_(ob_reading.c.available == 1,
                            ob_reading.c.time_create >= one_date[0],
                            ob_reading.c.time_create < one_date[1]
                            )
                       )

            readings = self._query(q_reading_in_date)
            reading_uids = [item['reading_uid'] for item in readings]
            user_ids = list(set([item['student_id'] for item in readings]))

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
            for g in group:
                group_map[g['id']] = g

            usergroup_map = defaultdict(list)
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
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g



            class_keys = defaultdict(lambda: defaultdict(dict))
            grade_keys = defaultdict(lambda: defaultdict(dict))
            school_keys = defaultdict(lambda: defaultdict(dict))
            channel_keys = defaultdict(lambda: defaultdict(dict))


            for reading in readings:

                #班级
                for u in usergroup_map.get(reading['student_id'], []):
                    c_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" +str(u['school_id']) + "@"+ str(u['grade']) + "@" + str(u['group_id'])
                    if class_keys[c_key]['user_id']:
                        class_keys[c_key]['user_id'].append(u['user_id'])
                        class_keys[c_key]['user_id'] = list(set(class_keys[c_key]['user_id']))
                    else:
                        class_keys[c_key]['user_id'] = [u['user_id']]

                #年级
                g_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" +str(u['school_id']) + "@"+ str(u['grade'])
                if grade_keys[g_key]['user_id']:
                    grade_keys[g_key]['user_id'].append(u['user_id'])
                    grade_keys[g_key]['user_id'] = list(set(grade_keys[g_key]['user_id']))
                else:
                    grade_keys[g_key]['user_id'] = [u['user_id']]

                #学校
                s_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" + str(
                    u['school_id'])
                if school_keys[s_key]['user_id']:
                    school_keys[s_key]['user_id'].append(u['user_id'])
                    school_keys[s_key]['user_id'] = list(set(school_keys[s_key]['user_id']))
                else:
                    school_keys[s_key]['user_id'] = [u['user_id']]

                #渠道
                ch_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1))
                if channel_keys[ch_key]['user_id']:
                    channel_keys[ch_key]['user_id'].append(u['user_id'])
                    channel_keys[ch_key]['user_id'] = list(set(channel_keys[ch_key]['user_id']))
                else:
                    channel_keys[ch_key]['user_id'] = [u['user_id']]


            #班级和历史比较
            history_class_reading_delta_ids = list(class_keys.keys())
            history_class_reading_delta = self.mongo.record_class_reading_delta.find(
                {"reading_channel_school_grade_group_id": {"$in": history_class_reading_delta_ids}})
            history_class_reading_id_map = {}
            class_reading_bulk_update = []
            history_class_reading_bulk_update = []
            for h in history_class_reading_delta:
                history_class_reading_id_map[h['reading_channel_school_grade_group_id']] = h

            for reading_channel_school_grade_group_id, data in class_keys.items():
                history_students = history_class_reading_id_map.get(reading_channel_school_grade_group_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school_grade_group_id.split("@")[0]
                channel = int(reading_channel_school_grade_group_id.split("@")[1])
                school = int(reading_channel_school_grade_group_id.split("@")[2])
                grade = reading_channel_school_grade_group_id.split("@")[3]
                group = int(reading_channel_school_grade_group_id.split("@")[4])
                history_class_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school_grade_group_id": reading_channel_school_grade_group_id},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school,
                                        "grade": grade,
                                        "group_id": group}},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_class_reading_id_map.get(reading_channel_school_grade_group_id, {}).get("status", 0) == 0:
                        class_schema = {
                            'channel': int(reading_channel_school_grade_group_id.split('@')[1]),
                            'school_id': int(reading_channel_school_grade_group_id.split('@')[2]),
                            'grade': reading_channel_school_grade_group_id.split('@')[3]
                        }
                        class_reading_bulk_update.append(
                            UpdateOne({"group_id": int(reading_channel_school_grade_group_id.split("@")[4]), "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": class_schema}, upsert=True))
                    history_class_reading_bulk_update.append(UpdateOne({"reading_channel_school_grade_group_id": reading_channel_school_grade_group_id}, {
                    '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #年级和历史比较
            history_grade_reading_delta_ids = list(grade_keys.keys())
            history_grade_reading_delta = self.mongo.record_grade_reading_delta.find(
                {"reading_channel_school_grade": {"$in": history_grade_reading_delta_ids}})
            history_grade_reading_id_map = {}
            grade_reading_bulk_update = []
            history_grade_reading_bulk_update = []
            for h in history_grade_reading_delta:
                history_grade_reading_id_map[h['reading_channel_school_grade']] = h

            for reading_channel_school_grade, data in grade_keys.items():
                history_students = history_grade_reading_id_map.get(reading_channel_school_grade, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school_grade.split("@")[0]
                channel = int(reading_channel_school_grade.split("@")[1])
                school = int(reading_channel_school_grade.split("@")[2])
                grade = reading_channel_school_grade.split("@")[3]
                history_grade_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school_grade": reading_channel_school_grade},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school,
                                        "grade": grade, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_grade_reading_id_map.get(reading_channel_school_grade, {}).get("status",0) == 0:
                        grade_schema = {
                            'channel': int(reading_channel_school_grade.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        }
                        grade_reading_bulk_update.append(
                            UpdateOne({"grade": reading_channel_school_grade.split("@")[3],
                                       "school_id": int(reading_channel_school_grade.split('@')[2]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": grade_schema}, upsert=True))
                    history_grade_reading_bulk_update.append(
                        UpdateOne({"reading_channel_school_grade": reading_channel_school_grade}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #学校和历史比较
            history_school_reading_delta_ids = list(school_keys.keys())
            history_school_reading_delta = self.mongo.record_school_reading_delta.find(
                {"reading_channel_school": {"$in": history_school_reading_delta_ids}})
            history_school_reading_id_map = {}
            school_reading_bulk_update = []
            history_school_reading_bulk_update = []
            for h in history_school_reading_delta:
                history_school_reading_id_map[h['reading_channel_school']] = h

            for reading_channel_school, data in school_keys.items():
                history_students = history_school_reading_id_map.get(reading_channel_school, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school.split("@")[0]
                channel = int(reading_channel_school.split("@")[1])
                school = int(reading_channel_school.split("@")[2])
                history_school_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school": reading_channel_school},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_reading_id_map.get(reading_channel_school, {}).get("status", 0) == 0:
                        school_schema = {
                            'channel': int(reading_channel_school.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        }
                        school_reading_bulk_update.append(
                            UpdateOne({"school_id": int(reading_channel_school.split("@")[2]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": school_schema}, upsert=True))
                    history_school_reading_bulk_update.append(
                        UpdateOne({"reading_channel_school": reading_channel_school}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #渠道和历史比较
            history_channel_reading_delta_ids = list(channel_keys.keys())
            history_channel_reading_delta = self.mongo.record_channel_reading_delta.find(
                {"reading_channel": {"$in": history_channel_reading_delta_ids}})
            history_channel_reading_id_map = {}
            channel_reading_bulk_update = []
            history_channel_reading_bulk_update = []
            for h in history_channel_reading_delta:
                history_channel_reading_id_map[h['reading_channel']] = h

            for reading_channel, data in channel_keys.items():
                history_students = history_channel_reading_id_map.get(reading_channel, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel.split("@")[0]
                channel = int(reading_channel.split("@")[1])
                history_channel_reading_bulk_update.append(
                    UpdateOne({"reading_channel": reading_channel},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_channel_reading_id_map.get(reading_channel, {}).get("status", 0) == 0:
                        # school_schema = {
                        #     'channel': int(reading_channel_school_grade.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        # }
                        channel_reading_bulk_update.append(
                            UpdateOne({"channel": int(reading_channel.split("@")[1]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}}, upsert=True))
                    history_channel_reading_bulk_update.append(
                        UpdateOne({"reading_channel": reading_channel}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))



            if history_class_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_reading_delta.bulk_write(history_class_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_reading_delta.bulk_write(history_grade_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_reading_delta.bulk_write(history_school_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_reading_delta.bulk_write(history_channel_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_reading_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_per_day.bulk_write(school_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("valid_reading_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))


        return


    def _reading_number_today(self, date_range):
        """
        有效阅读 当日
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_reading_in_date = select([ob_reading])\
                .where(and_(ob_reading.c.available == 1,
                            ob_reading.c.time_create >= one_date[0],
                            ob_reading.c.time_create < one_date[1]
                            )
                       )

            readings = self._query(q_reading_in_date)
            reading_uids = [item['reading_uid'] for item in readings]
            user_ids = list(set([item['student_id'] for item in readings]))

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
            for g in group:
                group_map[g['id']] = g

            usergroup_map = defaultdict(list)
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
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g



            class_keys = defaultdict(lambda: defaultdict(dict))
            grade_keys = defaultdict(lambda: defaultdict(dict))
            school_keys = defaultdict(lambda: defaultdict(dict))
            channel_keys = defaultdict(lambda: defaultdict(dict))


            for reading in readings:

                #班级
                for u in usergroup_map.get(reading['student_id'], []):
                    c_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" +str(u['school_id']) + "@"+ str(u['grade']) + "@" + str(u['group_id'])
                    if class_keys[c_key]['user_id']:
                        class_keys[c_key]['user_id'].append(u['user_id'])
                        class_keys[c_key]['user_id'] = list(set(class_keys[c_key]['user_id']))
                    else:
                        class_keys[c_key]['user_id'] = [u['user_id']]

                #年级
                g_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" +str(u['school_id']) + "@"+ str(u['grade'])
                if grade_keys[g_key]['user_id']:
                    grade_keys[g_key]['user_id'].append(u['user_id'])
                    grade_keys[g_key]['user_id'] = list(set(grade_keys[g_key]['user_id']))
                else:
                    grade_keys[g_key]['user_id'] = [u['user_id']]

                #学校
                s_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1)) + "@" + str(
                    u['school_id'])
                if school_keys[s_key]['user_id']:
                    school_keys[s_key]['user_id'].append(u['user_id'])
                    school_keys[s_key]['user_id'] = list(set(school_keys[s_key]['user_id']))
                else:
                    school_keys[s_key]['user_id'] = [u['user_id']]

                #渠道
                ch_key = reading['reading_uid'] + "@" + str(school_channel_map.get(u['school_id'], -1))
                if channel_keys[ch_key]['user_id']:
                    channel_keys[ch_key]['user_id'].append(u['user_id'])
                    channel_keys[ch_key]['user_id'] = list(set(channel_keys[ch_key]['user_id']))
                else:
                    channel_keys[ch_key]['user_id'] = [u['user_id']]


            #班级和历史比较
            history_class_reading_delta_ids = list(class_keys.keys())
            history_class_reading_delta = self.mongo.record_class_today_reading_delta.find(
                {"reading_channel_school_grade_group_id": {"$in": history_class_reading_delta_ids}})
            history_class_reading_id_map = {}
            class_reading_bulk_update = []
            history_class_reading_bulk_update = []
            for h in history_class_reading_delta:
                history_class_reading_id_map[h['reading_channel_school_grade_group_id']] = h

            for reading_channel_school_grade_group_id, data in class_keys.items():
                history_students = history_class_reading_id_map.get(reading_channel_school_grade_group_id, {}).get("user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school_grade_group_id.split("@")[0]
                channel = int(reading_channel_school_grade_group_id.split("@")[1])
                school = int(reading_channel_school_grade_group_id.split("@")[2])
                grade = reading_channel_school_grade_group_id.split("@")[3]
                group = int(reading_channel_school_grade_group_id.split("@")[4])
                history_class_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school_grade_group_id": reading_channel_school_grade_group_id},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school,
                                        "grade": grade,
                                        "group_id": group}},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_class_reading_id_map.get(reading_channel_school_grade_group_id, {}).get("status", 0) == 0:
                        class_schema = {
                            'channel': int(reading_channel_school_grade_group_id.split('@')[1]),
                            'school_id': int(reading_channel_school_grade_group_id.split('@')[2]),
                            'grade': reading_channel_school_grade_group_id.split('@')[3]
                        }
                        class_reading_bulk_update.append(
                            UpdateOne({"group_id": int(reading_channel_school_grade_group_id.split("@")[4]), "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": class_schema}, upsert=True))
                    history_class_reading_bulk_update.append(UpdateOne({"reading_channel_school_grade_group_id": reading_channel_school_grade_group_id}, {
                    '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #年级和历史比较
            history_grade_reading_delta_ids = list(grade_keys.keys())
            history_grade_reading_delta = self.mongo.record_grade_today_reading_delta.find(
                {"reading_channel_school_grade": {"$in": history_grade_reading_delta_ids}})
            history_grade_reading_id_map = {}
            grade_reading_bulk_update = []
            history_grade_reading_bulk_update = []
            for h in history_grade_reading_delta:
                history_grade_reading_id_map[h['reading_channel_school_grade']] = h

            for reading_channel_school_grade, data in grade_keys.items():
                history_students = history_grade_reading_id_map.get(reading_channel_school_grade, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school_grade.split("@")[0]
                channel = int(reading_channel_school_grade.split("@")[1])
                school = int(reading_channel_school_grade.split("@")[2])
                grade = reading_channel_school_grade.split("@")[3]
                history_grade_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school_grade": reading_channel_school_grade},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school,
                                        "grade": grade, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_grade_reading_id_map.get(reading_channel_school_grade, {}).get("status",0) == 0:
                        grade_schema = {
                            'channel': int(reading_channel_school_grade.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        }
                        grade_reading_bulk_update.append(
                            UpdateOne({"grade": reading_channel_school_grade.split("@")[3],
                                       "school_id": int(reading_channel_school_grade.split('@')[2]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": grade_schema}, upsert=True))
                    history_grade_reading_bulk_update.append(
                        UpdateOne({"reading_channel_school_grade": reading_channel_school_grade}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #学校和历史比较
            history_school_reading_delta_ids = list(school_keys.keys())
            history_school_reading_delta = self.mongo.record_school_today_reading_delta.find(
                {"reading_channel_school": {"$in": history_school_reading_delta_ids}})
            history_school_reading_id_map = {}
            school_reading_bulk_update = []
            history_school_reading_bulk_update = []
            for h in history_school_reading_delta:
                history_school_reading_id_map[h['reading_channel_school']] = h

            for reading_channel_school, data in school_keys.items():
                history_students = history_school_reading_id_map.get(reading_channel_school, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel_school.split("@")[0]
                channel = int(reading_channel_school.split("@")[1])
                school = int(reading_channel_school.split("@")[2])
                history_school_reading_bulk_update.append(
                    UpdateOne({"reading_channel_school": reading_channel_school},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel,
                                        "school_id": school, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_school_reading_id_map.get(reading_channel_school, {}).get("status", 0) == 0:
                        school_schema = {
                            'channel': int(reading_channel_school.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        }
                        school_reading_bulk_update.append(
                            UpdateOne({"school_id": int(reading_channel_school.split("@")[2]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}, "$set": school_schema}, upsert=True))
                    history_school_reading_bulk_update.append(
                        UpdateOne({"reading_channel_school": reading_channel_school}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))

            #渠道和历史比较
            history_channel_reading_delta_ids = list(channel_keys.keys())
            history_channel_reading_delta = self.mongo.record_channel_today_reading_delta.find(
                {"reading_channel": {"$in": history_channel_reading_delta_ids}})
            history_channel_reading_id_map = {}
            channel_reading_bulk_update = []
            history_channel_reading_bulk_update = []
            for h in history_channel_reading_delta:
                history_channel_reading_id_map[h['reading_channel']] = h

            for reading_channel, data in channel_keys.items():
                history_students = history_channel_reading_id_map.get(reading_channel, {}).get(
                    "user_id", [])
                today_students = data['user_id']
                today_total_students = list(set(history_students + today_students))
                reading_uid = reading_channel.split("@")[0]
                channel = int(reading_channel.split("@")[1])
                history_channel_reading_bulk_update.append(
                    UpdateOne({"reading_channel": reading_channel},
                              {'$set': {"user_id": today_total_students,
                                        "reading_uid": reading_uid ,
                                        "channel": channel, }},
                              upsert=True))
                if len(today_total_students) >= 10:  # 有效
                    if history_channel_reading_id_map.get(reading_channel, {}).get("status", 0) == 0:
                        # school_schema = {
                        #     'channel': int(reading_channel_school_grade.split('@')[1]),
                            # 'school_id': int(reading_channel_school_grade.split('@')[2]),
                            # 'grade': reading_channel_school_grade.split('@')[3]
                        # }
                        channel_reading_bulk_update.append(
                            UpdateOne({"channel": int(reading_channel.split("@")[1]),
                                       "day": one_date[0]},
                                      {'$inc': {"valid_reading_count": 1}}, upsert=True))
                    history_channel_reading_bulk_update.append(
                        UpdateOne({"reading_channel": reading_channel}, {
                            '$set': {"last_day": one_date[0], "status": 1}}, upsert=True))



            if history_class_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_class_today_reading_delta.bulk_write(history_class_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_grade_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_grade_today_reading_delta.bulk_write(history_grade_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_school_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_school_today_reading_delta.bulk_write(history_school_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if history_channel_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.record_channel_today_reading_delta.bulk_write(history_channel_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_today.bulk_write(class_reading_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_today.bulk_write(grade_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if school_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.school_today.bulk_write(school_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_reading_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_today.bulk_write(channel_reading_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)



        return




class PerDayTask(BaseTask):
    def __init__(self):
        super(PerDayTask, self).__init__()

    def run(self):
        try:
            print ('begin...')
            from tasks.celery_init import sales_celery
            sales_celery.send_task("tasks.celery_per_day_task.PerDaySubTask_IMAGES") #考试 单词图片数
            sales_celery.send_task("tasks.celery_per_day_task.PerDaySubTask_GUARDIAN") #家长数也为绑定数
            sales_celery.send_task("tasks.celery_per_day_task.PerDaySubTask_PAYMENTS") #付费数 付费额
            sales_celery.send_task("tasks.celery_per_day_task.PerDaySubTask_USERS") #学生数 老师数
            sales_celery.send_task("tasks.celery_per_day_task.PerDayTask_SCHOOL") #学校数
            sales_celery.send_task("tasks.celery_per_day_task.PerDayTask_VALIDCONTEST") #有效考试 有效单词
            sales_celery.send_task("tasks.celery_per_day_task.PerDayTask_VALIDREADING")  # 有效阅读

            # sales_celery.send_task("tasks.celery_per_day_task.PerDayTask_SCHOOLSTAGE")  # 学校阶段
            print ('finished...')

        except Exception as e:
            import traceback
            traceback.print_exc()



# i=PerDaySubTask_IMAGES()
# i._exercise_images_today([("2019-01-14", "2019-01-15")])