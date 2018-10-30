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

class SummaryTask(BaseTask):
    def __init__(self):
        super(SummaryTask, self).__init__()
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

    def _time_per_day(self):
        self.time_threshold_table = self.mongo[self.coll_time_threshold].find_one({"_id": "sale_time_threshold"}) or {}
        print("self.time_threshold_table", self.time_threshold_table)


    def run(self):

        # self._time_per_day()
        print ("start .......")
        schools = self._school()
        for school in schools:
            print (school)
            self._stage(school)
            if school:
                self.bulk_update.append(UpdateOne({"_id": school['id']}, {'$set': school}, upsert=True))
        if schools:
            self.mongo[self.coll_time_threshold].update({"_id": "sale_time_threshold"},
                                                        {"$set": {"school_begin_time": schools[-1]['time_create']}},
                                                        upsert=True)


        # # todo 批量插入学校输入，批量插入可优化
        if self.bulk_update:
            bulk_update_ret = self.mongo.school.bulk_write(self.bulk_update)
            print(bulk_update_ret.bulk_api_result)
        self.bulk_update = []
        self.server.stop()

    # def _school(self) -> list:
    #     """
    #     正序获取学校记录
    #     如果有记录最后时间则从最后时间开始到前一天，否则从默认开始时间到当前时间前一天
    #     """
    #     print('self.time_threshold_table.get("school_begin_time", self.start_time)',
    #           self.time_threshold_table.get("school_begin_time", self.start_time),
    #           type(self.time_threshold_table.get("school_begin_time", self.start_time)))
    #
    #     # t = datetime.datetime.strptime(self.time_threshold_table.get("school_begin_time", self.start_time), "%Y-%m-%d")
    #     t = self.time_threshold_table.get("school_begin_time", self.start_time)
    #     q_schools = select([ob_school]).where(and_(ob_school.c.available == 1,
    #                                                ob_school.c.time_create > t.strftime("%Y-%m-%d"),
    #                                                ob_school.c.time_create < (
    #                                                            datetime.datetime.now() - timedelta(1)).strftime(
    #                                                    "%Y-%m-%d"))). \
    #         order_by(asc(ob_school.c.time_create))
    #     schools = self._query(q_schools)
    #     return schools

    def _stage(self, school):
        """
        年级阶段和学校阶段
        获取
        """
        import collections
        #学校年级每天有效考试
        join = ob_groupuser.join(ob_group,
                                 and_(ob_groupuser.c.group_id == ob_group.c.id,
                                      ob_group.c.school_id == school['id'],
                                      ob_group.c.available == 1,
                                      ob_groupuser.c.available == 1)).join(as_hermes, and_(
            ob_groupuser.c.user_id == as_hermes.c.student_id,
            as_hermes.c.available == 1,
            ob_groupuser.c.available == 1
        )).alias("a")

        q_stages = select([join, func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d"),
                           func.count(join.c.sigma_account_re_groupuser_user_id). \
                          label("user_count")])\
            .select_from(join)\
            .group_by(func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d"))\
            .group_by(join.c.sigma_account_ob_group_grade) \
            .having(text('user_count >= 10'))\
            .order_by(asc(join.c.sigma_pool_as_hermes_time_create))
        stages = self._query(q_stages)

        #学校年级
        q_grades_classes = select([ob_group]).where(
            and_(ob_group.c.school_id == school['id'], ob_group.c.available == 1))
        grades_classes = self._query(q_grades_classes)

        bulk_update = []
        using_start_time = []
        for grade_class in grades_classes:
            grade_class['stage'] = StageEnum.Register.value
            for stage in stages:
                if grade_class['grade'] == stage['sigma_account_ob_group_grade']:
                    grade_class['stage'] = StageEnum.Using.value
                    grade_class['using_start_time'] = stage['sigma_pool_as_hermes_time_create']
                    using_start_time.append(stage['sigma_pool_as_hermes_time_create'])
                    break

            bulk_update.append(UpdateOne({"_id": str(grade_class['uid'])}, \
                                         {'$set': grade_class}, upsert=True))


        school["stage"] = min([StageEnum.Register.value] if not grades_classes else [k["stage"] for k in grades_classes])
        if school['stage'] == StageEnum.Register.value:
            school['using_start_time'] = ''
        elif school['stage'] == StageEnum.Using.value:
            school['using_start_time'] = using_start_time[0]

        if bulk_update:
            bulk_update_ret = self.mongo.grades.bulk_write(bulk_update)
            print(bulk_update_ret.bulk_api_result)

        return None

    def _total(self, school):
        # 老师
        q_teachers = select([us_user]).where(
            and_(us_user.c.school_id == school['id'],
                 us_user.c.role_id == Roles.TEACHER.value)
        )
        # 学生
        q_students = select([us_user]).where(
            and_(us_user.c.school_id == school['id'],
                 us_user.c.role_id == Roles.STUDENT.value)
        )
        # 家长
        q_guardian = select([us_user, re_userwechat]). \
            select_from(us_user.join(re_userwechat,
                                     and_(re_userwechat.c.user_id == us_user.c.id,
                                          re_userwechat.c.relationship > 0,
                                          us_user.c.school_id == school['id'],
                                          us_user.c.available == 1,
                                          re_userwechat.c.available == 1))
                        )

        teachers = self._query(q_teachers)
        students = self._query(q_students)
        guardians = self._query(q_guardian)
        self.total_dict['total_teachers'] = len(teachers)
        self.total_dict['total_students'] = len(students)
        self.total_dict['total_guardian'] = len(guardians)
