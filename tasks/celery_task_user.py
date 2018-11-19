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
from sshtunnel import SSHTunnelForwarder
from tasks.celery_base import BaseTask, CustomEncoder
from pymongo.errors import BulkWriteError
from celery.signals import worker_process_init,worker_process_shutdown

connection = None
server = None
@worker_process_init.connect
def init_worker(**kwargs):
    global connection
    global server
    print('Initializing database connection for worker.')
    server = SSHTunnelForwarder(
        ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

        ssh_password="PengKim@89527",
        ssh_username="jinpeng",
        remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
    server.start()
    connection = pymysql.connect(host="127.0.0.1",
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

class PerDaySubTask_IMAGES(BaseTask):
    def __init__(self):
        super(PerDaySubTask_IMAGES, self).__init__()

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
        date_range = self._date_range("class_channel_exercise_images_per_day_begin_time")  # 时间分段
        # date_range = [("2018-03-01","2018-03-02")]
        self._exercise_images(date_range)

    def _exercise_images(self, date_range):
        """
        考试图片数，单词图片数，用户id，当天日期
        :param date_range:
        :return:
        """
        print (date_range)
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
                print (json.dumps(u_g,indent=4,cls=CustomEncoder))
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g

            # print (json.dumps(usergroup_map_grade_key, indent=4 , cls=CustomEncoder))
            # print (json.dumps(usergroup_map, indent=4, cls=CustomEncoder))
            # self._set_time_threadshold("user_exercise_images_per_day_begin_time",
            #                                datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

            class_e_image_default_dict = defaultdict(list)
            class_w_image_default_dict = defaultdict(list)
            grade_e_image_default_dict = defaultdict(list)
            grade_w_image_default_dict = defaultdict(list)
            channel_e_image_default_dict = defaultdict(list)
            channel_w_image_default_dict = defaultdict(list)

            for image in exercise_images:
                if image['exercise_id'] in word_exercise_ids:
                    class_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_w_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_w_image_default_dict[usergroup_map.get(image['student_id'], {}).get("grade", -1)].append(1)

                else:
                    class_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("group_id", -1)].append(1)
                    channel_e_image_default_dict[
                        school_channel_map.get(usergroup_map.get(image['student_id'], {}).get("school_id", -1),
                                               -1)].append(1)

                    grade_e_image_default_dict[usergroup_map.get(image['student_id'], {}).get("grade", -1)].append(1)

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

            grade_bulk_update = []

            for k, v in grade_e_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1),
                    "e_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))


            for k, v in grade_w_image_default_dict.items():
                channel_image_counts_schema = {
                    "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1),
                    "w_image_c": sum(v),
                    "day": one_date[0]
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": k, "day": one_date[0]}, {'$set': channel_image_counts_schema}, upsert=True))

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

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("class_grade_channel_exercise_images_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDaySubTask_GUARDIAN(BaseTask):
    def __init__(self):
        super(PerDaySubTask_GUARDIAN, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        cursor = connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        return ret

    def run(self):
        date_range = self._date_range("class_grade_channel_guardian_per_day_begin_time")  # 时间分段
        # date_range = [("2018-1-1","2018-1-2")]
        self._guardian_info(date_range)

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
                re_userwechat.c.relationship > StudentRelationEnum.sich.value,
                re_userwechat.c.time_create >= one_date[0],
                re_userwechat.c.time_create < one_date[1]
            )
            )

            guardians = self._query(q_guardian_counts)

            user_ids = list(set([item['user_id'] for item in guardians]))

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

            # usergroup_map = {}
            # usergroup_map_grade_key = {}
            # for u_g in usergroup:
            #     u_g.update(group_map.get(u_g['group_id'], {}))
            #     usergroup_map[u_g['user_id']] = u_g
            #     usergroup_map_grade_key[u_g['group_id']] = u_g

            usergroup_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g


            class_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_guardian_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_guardian_default_dict = defaultdict(lambda: defaultdict(dict))

            for guardian in guardians:
                if class_guardian_default_dict[guardian['user_id']]['n']:
                    class_guardian_default_dict[guardian['user_id']]['n'].append(1)
                else:
                    class_guardian_default_dict[guardian['user_id']]['n'] = [1]
                if class_guardian_default_dict[guardian['user_id']]['wechats']:
                    class_guardian_default_dict[guardian['user_id']]['wechats'].append(guardian['wechat_id'])
                else:
                    class_guardian_default_dict[guardian['user_id']]['wechats'] = [guardian['wechat_id']]

                if grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['n']:
                    grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['n'].append(1)
                else:
                    grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['n'] = [1]


                if grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['wechats']:
                    grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['wechats'].append(guardian['wechat_id'])
                else:
                    grade_guardian_default_dict[usergroup_map.get(guardian['user_id'], {}).get("grade", -1)]['wechats'] = [guardian['wechat_id']]

                if channel_guardian_default_dict[school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['n']:
                    channel_guardian_default_dict[
                        school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['n'].append(1)
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['n'] = [1]
                if channel_guardian_default_dict[school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['wechats']:
                    channel_guardian_default_dict[
                        school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['wechats'].append(guardian['wechat_id'])
                else:
                    channel_guardian_default_dict[
                        school_channel_map.get(usergroup_map.get(guardian['user_id'], {}).get("school_id", -1))]['wechats'] = [guardian['wechat_id']]





                class_guardian_default_dict[guardian['user_id']]['group_info'] = usergroup_map.get(guardian['user_id'], {})

            class_bulk_update = []
            for k, v in class_guardian_default_dict.items():
                guardian_schema = {
                    "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                    "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                    "guardian_count":len(v['n']),
                    "wechats": v['wechats'],
                }
                class_bulk_update.append(UpdateOne({"group_id": v['group_info'].get('group_id', -1), "day": one_date[0]},
                                             {'$set': guardian_schema}, upsert=True))

            grade_bulk_update = []
            for k, v in grade_guardian_default_dict.items():
                guardian_schema = {
                    "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1),
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                grade_bulk_update.append(
                    UpdateOne({"grade": v['group_info'].get('grade', -1), "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            channel_bulk_update = []
            for k, v in channel_guardian_default_dict.items():
                guardian_schema = {
                    "guardian_count": len(v['n']),
                    "wechats": v['wechats'],
                }
                channel_bulk_update.append(
                    UpdateOne({"channel": k, "day": one_date[0]},
                              {'$set': guardian_schema}, upsert=True))

            if class_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("class_grade_channel_guardian_per_day_begin_time",
                                           datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDaySubTask_PAYMENTS(BaseTask):
    def __init__(self):
        super(PerDaySubTask_PAYMENTS, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        cursor = connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        return ret

    def run(self):

        date_range = self._date_range("class_grade_channel_pay_per_day_begin_time")  # 时间分段
        self._pay_amount(date_range) #付费数 付费额

    def _pay_amount(self, date_range):
        """
        付费数，付费额
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_payments = select([ob_order.c.user_id, ob_order.c.coupon_amount])\
                .where(and_(ob_order.c.available == 1,
                            ob_order.c.status == 3,
                            ob_order.c.time_create > one_date[0],
                            ob_order.c.time_create < one_date[1])
                       )

            payments = self._query(q_payments)

            user_ids = list(set([item['user_id'] for item in payments]))

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


            usergroup_map = {}
            usergroup_map_class_key = {}
            usergroup_map_grade_key = {}
            for u_g in usergroup:
                u_g.update(group_map.get(u_g['group_id'], {}))
                usergroup_map[u_g['user_id']] = u_g
                usergroup_map_class_key[u_g['group_id']] = u_g
                usergroup_map_grade_key[u_g.get("grade", -1)] = u_g


            class_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            grade_payment_default_dict = defaultdict(lambda: defaultdict(dict))
            channel_payment_default_dict = defaultdict(lambda: defaultdict(dict))

            for payment in payments:
                if class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)]["pay_n"]:
                    class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)]["pay_n"].append(1)
                else:
                    class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)][
                        "pay_n"] = [1]
                if class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)]["pay_amount"]:
                    class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)][
                        "pay_amount"].append(payment['coupon_amount'])
                else:
                    class_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("group_id", 0)][
                        "pay_amount"] = [payment['coupon_amount']]


                if grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_n']:
                    grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_n'].append(1)
                else:
                    grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_n'] = [1]

                if grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_amount']:
                    grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_amount'].append(payment['coupon_amount'])
                else:
                    grade_payment_default_dict[usergroup_map.get(payment['user_id'], {}).get("grade", -1)]['pay_amount'] = [payment['coupon_amount']]

                if channel_payment_default_dict[school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))]['pay_n']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))][
                        'pay_n'].append(1)
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))][
                        'pay_n'] = [1]

                if channel_payment_default_dict[school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))]['pay_amount']:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))][
                        'pay_n'].append(payment['coupon_amount'])
                else:
                    channel_payment_default_dict[
                        school_channel_map.get(usergroup_map.get(payment['user_id'], {}).get("school_id", -1))][
                        'pay_amount'] = [payment['coupon_amount']]




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

            grade_bulk_update = []
            for k, v in grade_payment_default_dict.items():
                pay_amount_schema = {
                    "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                    "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1),
                    "pay_number": len(v['pay_n']),
                    "pay_amount": sum(v['pay_amount'])
                }

                grade_bulk_update.append(UpdateOne({"grade": k, "day": one_date[0]},
                                             {'$set': pay_amount_schema}, upsert=True))

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
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            self._set_time_threadshold("class_grade_channel_pay_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDayTask_SCHOOL(BaseTask):
    def __init__(self):
        super(PerDayTask_SCHOOL, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        cursor = connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        return ret

    def run(self):

        date_range = self._date_range("school_number_per_day_begin_time")  # 时间分段
        self._school(date_range) #学校数

    def _school(self, date_range):
        """
        学校数
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_schools = select([ob_school.c.id, ob_school.c.owner_id])\
                .where(and_(ob_school.c.available == 1,
                            ob_school.c.time_create > one_date[0],
                            ob_school.c.time_create < one_date[1])
                       )

            schools = self._query(q_schools)

            school_defaultdict = defaultdict(list)

            self_school_bulk_update = []
            channel_school_bulk_update = []
            for school in schools:
                school_defaultdict[school['owner_id']].append(1)
                school_schema = {
                    "channel": school['owner_id'],
                }

                self_school_bulk_update.append(UpdateOne({"school_id": school['id'], "day": one_date[0]},
                                                   {'$set': school_schema}, upsert=True))

            for k,v in school_defaultdict.items():
                school_schema = {

                    "school_number": sum(v),
                }

                channel_school_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                         {'$set': school_schema}, upsert=True))

            if self_school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.schools.bulk_write(self_school_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_school_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_school_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("school_number_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDaySubTask_USERS(BaseTask):
    def __init__(self):
        super(PerDaySubTask_USERS, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        cursor = connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

        cursor.execute(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))
        ret = cursor.fetchall()
        return ret

    def run(self):
        date_range = self._date_range("teacher_student_number_per_day_begin_time")  # 时间分段
        # date_range = [("2018-4-1", "2018-4-2")]
        self._user_counts(date_range) #老师数 学生数

    def _user_counts(self, date_range):
        """
        老师数，学生数
        :param date_range:
        :return:
        """
        for one_date in date_range:
            q_teacher_student = select([us_user.c.id, us_user.c.role_id])\
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


            school_ids = list(set([item['school_id'] for item in group]))
            q_school = select([ob_school.c.owner_id, ob_school.c.id]).where(and_(
                ob_school.c.available == 1,
                ob_school.c.id.in_(school_ids),
            ))

            schools = self._query(q_school)

            school_channel_map = {}
            for s_c in schools:
                school_channel_map[s_c['id']] = s_c['owner_id']

            teacher_student_map = {}
            for t_s in teacher_student:
                teacher_student_map[t_s['id']] = t_s


            class_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))

            class_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            grade_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            channel_student_number_defaultdict = defaultdict(lambda: defaultdict(list))


            for u_g in usergroup:
                for g in group:
                    if u_g['group_id'] == g['id']:
                        u_g['group_info'] = g
                        break
                u_g['user_info'] = teacher_student_map.get(u_g['user_id'], {})
                u_g['channel'] = school_channel_map.get(u_g.get('group_info', {}).get("school_id", -1), -1)

                if u_g['role_id'] == Roles.TEACHER.value:
                    class_teacher_number_defaultdict[u_g['group_id']]['user_info'] = u_g
                    if class_teacher_number_defaultdict[u_g['group_id']]['n']:
                        class_teacher_number_defaultdict[u_g['group_id']]['n'].append(1)
                    else:
                        class_teacher_number_defaultdict[u_g['group_id']]['n'] = [1]

                    grade_teacher_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['user_info'] = u_g
                    if grade_teacher_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n']:
                        grade_teacher_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n'].append(1)
                    else:
                        grade_teacher_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n'] = [1]

                    channel_teacher_number_defaultdict[u_g['channel']]['user_info'] = u_g
                    if channel_teacher_number_defaultdict[u_g['channel']]['n']:
                        channel_teacher_number_defaultdict[u_g['channel']]['n'].append(1)
                    else:
                        channel_teacher_number_defaultdict[u_g['channel']]['n'] = [1]

                elif u_g['role_id'] == Roles.STUDENT.value:
                    class_student_number_defaultdict[u_g['group_id']]['user_info'] = u_g
                    if class_student_number_defaultdict[u_g['group_id']]['n']:
                        class_student_number_defaultdict[u_g['group_id']]['n'].append(1)
                    else:
                        class_student_number_defaultdict[u_g['group_id']]['n'] = [1]

                    grade_student_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['user_info'] = u_g
                    if grade_student_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n']:
                        grade_student_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n'].append(1)
                    else:
                        grade_student_number_defaultdict[u_g.get('group_info', {}).get('grade', -1)]['n'] = [1]

                    channel_student_number_defaultdict[u_g['channel']]['user_info'] = u_g
                    if channel_student_number_defaultdict[u_g['channel']]['n']:
                        channel_student_number_defaultdict[u_g['channel']]['n'].append(1)
                    else:
                        channel_student_number_defaultdict[u_g['channel']]['n'] = [1]
                else:
                    pass



            # class_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            # grade_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            # channel_teacher_number_defaultdict = defaultdict(lambda: defaultdict(list))
            #
            # class_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            # grade_student_number_defaultdict = defaultdict(lambda: defaultdict(list))
            # channel_student_number_defaultdict = defaultdict(lambda: defaultdict(list))


            class_teacher_number_bulk_update = []
            for k, v in class_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("group_info", {}).get("school_id", -1),
                    "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": v['user_info'].get("channel", -1),
                    "teacher_number": sum(v['n']),
                }

                class_teacher_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                            {'$set': user_number_schema}, upsert=True))

            grade_teacher_number_bulk_update = []
            for k, v in grade_teacher_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("group_info", {}).get("school_id", -1),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": v['user_info'].get("channel", -1),
                    "teacher_number": sum(v['n']),
                }

                grade_teacher_number_bulk_update.append(UpdateOne({"grade": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

            channel_teacher_number_bulk_update = []
            for k, v in channel_teacher_number_defaultdict.items():
                user_number_schema = {
                    "teacher_number": sum(v['n']),
                }

                channel_teacher_number_bulk_update.append(UpdateOne({"channel": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

            class_student_number_bulk_update = []
            for k, v in class_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("group_info", {}).get("school_id", -1),
                    "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": v['user_info'].get("channel", -1),
                    "student_number": sum(v['n']),
                }

                class_student_number_bulk_update.append(UpdateOne({"group_id": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

            grade_student_number_bulk_update = []
            for k, v in grade_student_number_defaultdict.items():
                user_number_schema = {
                    "school_id": v['user_info'].get("group_info", {}).get("school_id", -1),
                    # "grade": v['user_info'].get("group_info", {}).get("grade", -1),
                    "channel": v['user_info'].get("channel", -1),
                    "student_number": sum(v['n']),
                }

                grade_student_number_bulk_update.append(UpdateOne({"grade": k, "day": one_date[0]},
                                                                  {'$set': user_number_schema}, upsert=True))

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
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_teacher_number_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_teacher_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_teacher_number_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if class_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_student_number_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if grade_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_student_number_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            if channel_student_number_bulk_update:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_student_number_bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            self._set_time_threadshold("teacher_student_number_per_day_begin_time",
                                       datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

class PerDayTask_VALIADCONTEST(BaseTask):
    def __init__(self):
        super(PerDayTask_VALIADCONTEST, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

    def _query(self, query):
        """
        执行查询 返回数据库结果
        """

        cursor = connection.cursor()
        # logger.debug(
        #     query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}).string.replace("%%", "%"))

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
                            as_hermes.c.sheetIndex == 1,
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
            # print(json.dumps(usergroup_map_grade_key, indent=4, cls=CustomEncoder))
            for e_w_images in exercise_word_images:
                if e_w_images.get("user_info", []):
                    e_w_images.append(usergroup_single_map.get(e_w_images.get("student_id", -1), {}))
                else:
                    e_w_images['user_info'] = [usergroup_single_map.get(e_w_images.get("student_id", -1))] if usergroup_single_map.get(e_w_images.get("student_id", -1)) else []

            exercise_image_class_defaultdict = defaultdict(list)
            exercise_image_grade_defaultdict =  defaultdict(list)
            exercise_image_channel_defaultdict = defaultdict(list)

            word_image_class_defaultdict = defaultdict(list)
            word_image_grade_defaultdict = defaultdict(list)
            word_image_channel_defaultdict = defaultdict(list)
            for e_w_images in exercise_word_images:
                if e_w_images['exercise_id'] in word_exercise_ids: #词汇
                    for u in e_w_images['user_info']:
                        word_image_class_defaultdict[usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1)].append(1)
                        word_image_grade_defaultdict[
                            usergroup_single_map.get(e_w_images.get("student_id", -1), {}).get("grade", -1)].append(1)
                        word_image_channel_defaultdict[school_channel_map.get(usergroup_single_map.get(e_w_images.get("student_id", -1), {}).get("school_id", -1), -1)].append(1)
                else:
                    for u in e_w_images['user_info']:

                        exercise_image_class_defaultdict[usergroup_single_map.get(u.get("user_id", -1), {}).get("group_id", -1)].append(1)
                    exercise_image_grade_defaultdict[ usergroup_single_map.get(e_w_images.get("student_id", -1), {}).get("grade", -1) ].append(1)
                    exercise_image_channel_defaultdict[school_channel_map.get(usergroup_single_map.get(e_w_images.get("student_id", -1), {}).get("school_id", -1), -1)].append(1)

            class_valid_exercise_number_bulk = []
            for k, v in exercise_image_class_defaultdict.items():
                #k 是班级  v是数量
                if sum(v) >= 10: #有效考试
                    exercise_schema = {
                            "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                            "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1) ,
                            "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                            # "valid_exercise_count": sum(v)
                        }
                    class_valid_exercise_number_bulk.append(
                        UpdateOne({"group_id": k, "day": one_date[0]}, {'$set': exercise_schema, "$inc":{"valid_exercise_count": 1}}, upsert=True))

            if class_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            grade_valid_exercise_number_bulk = []
            for k, v in exercise_image_grade_defaultdict.items():
                #k 是班级  v是数量
                if sum(v) >= 10: #有效考试

                    exercise_schema = {
                            "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                            "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1) ,
                            # "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                            # "valid_exercise_count": sum(v)
                        }
                    grade_valid_exercise_number_bulk.append(
                        UpdateOne({"grade": k, "day": one_date[0]}, {'$set': exercise_schema, "$inc":{"valid_exercise_count": 1}}, upsert=True))

            if grade_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            channel_valid_exercise_number_bulk = []
            # print(exercise_image_channel_defaultdict)
            for k, v in exercise_image_channel_defaultdict.items():
                # k 是班级  v是数量

                if sum(v) >= 10:  # 有效考试
                    exercise_schema = {
                        # "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                        # "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1)),
                        # "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                        # "valid_exercise_count": sum(v)
                    }
                    channel_valid_exercise_number_bulk.append(
                        UpdateOne({"channel": k, "day": one_date[0]},
                                  { "$inc": {"valid_exercise_count": 1}}, upsert=True))

            if channel_valid_exercise_number_bulk:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_exercise_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)


            class_valid_word_number_bulk = []
            for k, v in word_image_class_defaultdict.items():
                # k 是班级  v是数量
                if sum(v) >= 10:  # 有效考试
                    exercise_schema = {
                        "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                        "channel": school_channel_map.get(usergroup_map_class_key.get(k, {}).get("school_id", -1), -1),
                        "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                        # "valid_exercise_count": sum(v)
                    }
                    class_valid_word_number_bulk.append(
                        UpdateOne({"group_id": k, "day": one_date[0]},
                                  {'$set': exercise_schema, "$inc": {"valid_word_count": 1}}, upsert=True))

            if class_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            grade_valid_word_number_bulk = []
            for k, v in word_image_grade_defaultdict.items():
                # k 是班级  v是数量
                if sum(v) >= 10:  # 有效考试
                    exercise_schema = {
                        "school_id": usergroup_map_grade_key.get(k, {}).get("school_id", -1),
                        "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1), -1),
                        # "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                        # "valid_exercise_count": sum(v)
                    }
                    grade_valid_word_number_bulk.append(
                        UpdateOne({"grade": k, "day": one_date[0]},
                                  {'$set': exercise_schema, "$inc": {"valid_word_count": 1}}, upsert=True))

            if grade_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)

            channel_valid_word_number_bulk = []
            for k, v in word_image_channel_defaultdict.items():
                # k 是班级  v是数量
                if sum(v) >= 10:  # 有效考试
                    exercise_schema = {
                        # "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
                        # "channel": school_channel_map.get(usergroup_map_grade_key.get(k, {}).get("school_id", -1)),
                        # "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
                        # "valid_exercise_count": sum(v)
                    }
                    channel_valid_word_number_bulk.append(
                        UpdateOne({"channel": k, "day": one_date[0]},
                                  { "$inc": {"valid_word_count": 1}}, upsert=True))

            if channel_valid_word_number_bulk:
                try:
                    bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_word_number_bulk)
                    # print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)






            self._set_time_threadshold("valid_exercise_word_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

            # valid_exercise_defaultdict = defaultdict(lambda: defaultdict(dict))
            # valid_word_default = defaultdict(lambda: defaultdict(dict))
            # exercise_user_map = {}
            # for e_w_image in exercise_word_images:
            #     exercise_user_map[e_w_image['exercise_id']] = usergroup_single_map.get(e_w_image.get("student_id", {}))
            #     if e_w_image['exercise_id'] in word_exercise_ids:  # 单词
            #         if valid_word_default[e_w_image['exercise_id']]['n']:
            #             valid_word_default[e_w_image['exercise_id']]['n'].append(1)
            #         else:
            #             valid_word_default[e_w_image['exercise_id']]['n'] = [1]
            #
            #         if valid_word_default[e_w_image['exercise_id']]['time']:
            #             valid_word_default[e_w_image['exercise_id']]['time'].append(e_w_image['time_create'])
            #         else:
            #             valid_word_default[e_w_image['exercise_id']]['time_create'] = [e_w_image['time_create']]
            #
            #     else:  # 考试
            #         if valid_exercise_defaultdict[e_w_image['exercise_id']]['n']:
            #
            #             valid_exercise_defaultdict[e_w_image['exercise_id']]['n'].append(1)
            #         else:
            #             valid_exercise_defaultdict[e_w_image['exercise_id']]['n'] = [1]
            #
            #         if valid_exercise_defaultdict[e_w_image['exercise_id']]['time']:
            #             valid_exercise_defaultdict[e_w_image['exercise_id']]['time'].append(e_w_image['time_create'])
            #         else:
            #             valid_exercise_defaultdict[e_w_image['exercise_id']]['time'] = [e_w_image['time_create']]
            #
            # exercise_begin_time_map = {}
            # class_valid_exervise_number_defaultdict = defaultdict(list)
            # grade_valid_exervise_number_defaultdict = defaultdict(list)
            # channel_valid_exervise_number_defaultdict = defaultdict(list)
            #
            # class_valid_word_number_defaultdict = defaultdict(list)
            # grade_valid_word_number_defaultdict = defaultdict(list)
            # channel_valid_word_number_defaultdict = defaultdict(list)
            #
            # for e_w_image in exercise_word_images:
            #     if e_w_image['exercise_id'] in word_exercise_ids: #单词
            #         pass
            #     else: #考试
            #
            #         total = sum(valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['n'])
            #         if total>= 10:
            #             exercise_begin_time_map["exercise_id"] = \
            #             sorted(valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['time'])[0] \
            #                 if valid_exercise_defaultdict.get(e_w_image['exercise_id'], {})['time'] else e_w_image[
            #                 'time_create']
            #             # 班级
            #             for u in usergroup_map.get(e_w_image['student_id'], []):
            #                 class_valid_exervise_number_defaultdict[u.get("group_id", -1)].append(1)
            #
            #             #年级
            #             grade_valid_exervise_number_defaultdict[usergroup_single_map.get(e_w_image['student_id'], {}).get("grade", -1)].append(1)
            #             #渠道
            #             channel_valid_exervise_number_defaultdict[school_channel_map.get( usergroup_single_map.get(e_w_image['student_id'], {}).get("school_id", -1) ,-1)].append(1)
            #
            #             # 班级
            #             for u in usergroup_map.get(e_w_image['student_id'], []):
            #                 class_valid_word_number_defaultdict[u.get("group_id", -1)].append(1)
            #
            #             # 年级
            #             grade_valid_word_number_defaultdict[
            #                 usergroup_single_map.get(e_w_image['student_id'], {}).get("grade", -1)].append(1)
            #             # 渠道
            #             channel_valid_word_number_defaultdict[school_channel_map.get(
            #                 usergroup_single_map.get(e_w_image['student_id'], {}).get("school_id", -1), -1)].append(1)
            #
            #
            #
            # class_valid_exercise_number_bulk = []
            # grade_valid_exercise_number_bulk = []
            # channel_valid_exercise_number_bulk = []
            #
            # class_valid_word_number_bulk = []
            # grade_valid_word_number_bulk = []
            # channel_valid_word_number_bulk = []
            # for k, v in class_valid_exervise_number_defaultdict.items():
            #     exercise_schema = {
            #         "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
            #         "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
            #         "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
            #         "valid_exercise_count": sum(v)
            #     }
            #     class_valid_exercise_number_bulk.append(
            #         UpdateOne({"group_id": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            # for k, v in grade_valid_exervise_number_defaultdict.items():
            #     exercise_schema = {
            #         "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
            #         "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
            #         "valid_exercise_count": sum(v)
            #     }
            #     grade_valid_exercise_number_bulk.append(
            #         UpdateOne({"grade": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            # for k, v in channel_valid_exervise_number_defaultdict.items():
            #     exercise_schema = {
            #         "valid_exercise_count": sum(v)
            #     }
            #     channel_valid_exercise_number_bulk.append(
            #         UpdateOne({"channel": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            #
            # for k, v in class_valid_word_number_defaultdict.items():
            #     exercise_schema = {
            #         "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
            #         "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
            #         "grade": usergroup_map_class_key.get(k, {}).get("grade", -1),
            #         "valid_exercise_count": sum(v)
            #     }
            #     class_valid_word_number_bulk.append(
            #         UpdateOne({"group_id": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            #
            # for k, v in grade_valid_word_number_defaultdict.items():
            #     exercise_schema = {
            #         "school_id": usergroup_map_class_key.get(k, {}).get("school_id", -1),
            #         "channel": usergroup_map_class_key.get(k, {}).get("channel", -1),
            #         "valid_exercise_count": sum(v)
            #     }
            #     grade_valid_word_number_bulk.append(
            #         UpdateOne({"grade": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            #
            # for k, v in channel_valid_word_number_defaultdict.items():
            #     exercise_schema = {
            #         "valid_exercise_count": sum(v)
            #     }
            #     channel_valid_word_number_bulk.append(
            #         UpdateOne({"channel": k, "day": one_date[0]}, {'$set': exercise_schema}, upsert=True))
            #
            #
            # if class_valid_exercise_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_exercise_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            # if grade_valid_exercise_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_exercise_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            # if channel_valid_exercise_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_exercise_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            #
            # if class_valid_word_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.class_per_day.bulk_write(class_valid_word_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            # if grade_valid_word_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.grade_per_day.bulk_write(grade_valid_word_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            # if channel_valid_word_number_bulk:
            #     try:
            #         bulk_update_ret = self.mongo.channel_per_day.bulk_write(channel_valid_word_number_bulk)
            #         # print(bulk_update_ret.bulk_api_result)
            #     except BulkWriteError as bwe:
            #         print(bwe.details)
            #
            # self._set_time_threadshold("valid_exercise_word_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))




class PerDayTask(BaseTask):
    def __init__(self):
        super(PerDayTask, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


    def run(self):
        try:
            print ('begin')
            from tasks.celery_init import sales_celery
            # sales_celery.send_task("tasks.celery_task_user.PerDaySubTask_IMAGES") #考试 单词图片数
            # sales_celery.send_task("tasks.celery_task_user.PerDaySubTask_GUARDIAN") #家长数也为绑定数
            # sales_celery.send_task("tasks.celery_task_user.PerDaySubTask_PAYMENTS") #付费数 付费额
            # sales_celery.send_task("tasks.celery_task_user.PerDaySubTask_USERS") #学生数 老师数
            # sales_celery.send_task("tasks.celery_task_user.PerDayTask_SCHOOL") #学校数
            sales_celery.send_task("tasks.celery_task_user.PerDayTask_VALIADCONTEST") #有效考试 有效单词

            print ('finished.......')
            # self.server.stop()
            # self.connection.close()
            # self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)



