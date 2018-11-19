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

class SchoolTask(BaseTask):
    """
    学校维度的教师数，学生数，家长数，一天有效考试数，一天每次有效考试的学生数，一天考试图像上传数，一天有效词汇图像上传数，一天有效阅读数，一天付费数,一天付费额
    """

    def __init__(self):
        super(SchoolTask, self).__init__()

        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


    def run(self):
        try:
            date_range = self._date_range("school_begin_time")  # 时间分段
            schools = self._school(date_range)
            # for school in schools:
            #     self._teacher_counts(school)
            #     self._student_counts(school)
            #     self._valid_exercise_students_counts(school)
            #     self._exam_images_counts(school)
            #     self._guardian_counts(school)
            #     self._paid_counts(school)
            self.server.stop()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            import traceback
            traceback.print_exc()

            raise self.retry(exc=e, countdown=30, max_retries=5)


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


    def _school(self, date_range)->list:
        """
        正序获取学校记录
        如果有记录最后时间则从最后时间开始到前一天，否则从默认开始时间到当前时间前一天
        """
        for one_date in date_range:
        # t = self.time_threshold_table.get("school_begin_time", self.start_time)
            q_schools = select([ob_school]).where(and_(ob_school.c.available==1,
                                                       ob_school.c.time_create >= one_date[0],
                                                       ob_school.c.time_create < one_date[1])).\
                                        order_by(asc(ob_school.c.time_create))
            schools = self._query(q_schools)
            bulk_update = []
            for school in schools:
                self._stage(school)
                bulk_update.append(UpdateOne({"_id": str(school['id'])}, \
                                             {'$set': school}, upsert=True))
            if bulk_update:
                try:
                    bulk_update_ret = self.mongo.school.bulk_write(bulk_update)
                    print(bulk_update_ret.bulk_api_result)
                except BulkWriteError as bwe:
                    print(bwe.details)
            self._set_time_threadshold("school_begin_time", datetime.datetime.strptime(one_date[1], "%Y-%m-%d"))

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
            try:
                bulk_update_ret = self.mongo.grades.bulk_write(bulk_update)
                print(bulk_update_ret.bulk_api_result)
            except BulkWriteError as bwe:
                print(bwe.details)

        return None


    def _teacher_counts(self, school):
        """
        每天的老师数
        """

        j = ob_groupuser.join(us_user,
                              and_(ob_groupuser.c.user_id == us_user.c.id,
                                   us_user.c.school_id == school['id'],
                                   ob_groupuser.c.role_id == Roles.TEACHER.value,
                                   us_user.c.available == 1,
                                   ob_groupuser.c.available == 1))
        stmt = select([ob_groupuser, func.date_format(j.c.sigma_account_re_groupuser_time_create, "%Y-%m-%d"),
                       func.count(j.c.sigma_account_re_groupuser_user_id)]).select_from(j). \
            group_by(func.date_format(j.c.sigma_account_re_groupuser_time_create, "%Y-%m-%d")).order_by(
            asc(j.c.sigma_account_re_groupuser_time_create))

        teachers = self._query(stmt)
        bulk_update = []
        for teacher in teachers:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + teacher['date_format_1']}, \
                                         {'$set': {"teacher_counts": teacher['count_1'],
                                                   "date": datetime.datetime.strptime(teacher['date_format_1'],
                                                                                      "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)

    def _student_counts(self, school):
        """
        每天的学生数
        """
        j = ob_groupuser.join(us_user,
                              and_(ob_groupuser.c.user_id == us_user.c.id,
                                   us_user.c.school_id == school['id'],
                                   ob_groupuser.c.role_id == Roles.STUDENT.value,
                                   us_user.c.available == 1,
                                   ob_groupuser.c.available == 1))
        stmt = select([ob_groupuser, func.date_format(j.c.sigma_account_re_groupuser_time_create, "%Y-%m-%d"),
                       func.count(j.c.sigma_account_re_groupuser_user_id)]).select_from(j). \
            group_by(func.date_format(j.c.sigma_account_re_groupuser_time_create, "%Y-%m-%d")).order_by(
            asc(j.c.sigma_account_re_groupuser_time_create))

        students = self._query(stmt)
        bulk_update = []
        for student in students:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + student['date_format_1']}, \
                                         {'$set': {"student_counts": student['count_1'],
                                                   "date": datetime.datetime.strptime(student['date_format_1'],
                                                                                      "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)
        return students

    def _guardian_counts(self, school):
        """
        每天的家长数
        """
        j = re_userwechat.join(us_user,
                               and_(
                                   re_userwechat.c.relationship > StudentRelationEnum.sich.value,
                                   re_userwechat.c.available == 1,
                                   us_user.c.school_id == school['id']))
        q_guardians = select([re_userwechat.c.time_create, func.date_format(re_userwechat.c.time_create, "%Y-%m-%d"), \
                              func.count(distinct(re_userwechat.c.user_id)).label('guardian_count')]).select_from(j). \
            group_by(func.date_format(re_userwechat.c.time_create, "%Y-%m-%d")).order_by(
            asc(re_userwechat.c.time_create))

        guardians = self._query(q_guardians)
        bulk_update = []
        for guardian in guardians:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + guardian['date_format_1']}, \
                                         {'$set': {"guardian_counts": guardian['guardian_count'],
                                                   "date": datetime.datetime.strptime(guardian['date_format_1'],
                                                                                      "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)

    def _exercise_counts(self, school, valid_exercise_data):
        """
        一天的有效考试数
        """
        # from collections import defaultdict
        # data = defaultdict(list)
        # for e_s_c in exercise_students_counts:
        #     data[e_s_c['date_format_1']].append(1)
        # bulk_update = []
        # print(exercise_students_counts)
        # for d in data:
        #     bulk_update.append(UpdateOne({"_id": str(school['id'])+'@'+d},\
        #      {'$set': {"valid_exercise_counts": sum(data[d]),\
        #      "school_id": school['id']}}, upsert=True ))
        # if bulk_update:
        #     bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
        #     print (bulk_update_ret.bulk_api_result)

        bulk_update = []
        for d in valid_exercise_data:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + d}, \
                                         {'$set': {"valid_exercise_counts": sum(valid_exercise_data[d]),
                                                   "date": datetime.datetime.strptime(d, "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)

    def _valid_exercise_students_counts(self, school):
        """
        一天中每次有效考试的学生数
        """

        join = ob_groupuser.join(ob_group,
                                 and_(ob_groupuser.c.group_id == ob_group.c.id,
                                      ob_group.c.school_id == school['id'],
                                      ob_group.c.available == 1,
                                      ob_groupuser.c.available == 1)).join(as_hermes, and_(
            ob_groupuser.c.user_id == as_hermes.c.student_id,
            as_hermes.c.available == 1,
            ob_groupuser.c.available == 1
        )).join(ob_exercise, and_(ob_exercise.c.id == as_hermes.c.exercise_id,
                                  ob_exercise.c.available == 1)).alias("a")
        q_valid_exercises = select([join, func.date_format(join.c.sigma_exercise_ob_exercise_time_create, "%Y-%m-%d"),
                                    func.count(join.c.sigma_account_re_groupuser_user_id). \
                                   label("user_count")]).select_from(join).group_by(
            func.date_format(join.c.sigma_exercise_ob_exercise_time_create, "%Y-%m-%d")). \
            group_by(join.c.sigma_exercise_ob_exercise_id). \
            having(text('user_count >= 10')). \
            order_by(asc(join.c.sigma_exercise_ob_exercise_time_create))
        valid_exercises = self._query(q_valid_exercises)

        from collections import defaultdict
        valid_exercise_student_data = defaultdict(list)
        valid_exercise_data = defaultdict(list)

        for v_e_s_d in valid_exercises:
            valid_exercise_student_data[v_e_s_d['date_format_1']].append(v_e_s_d['user_count'])
            valid_exercise_data[v_e_s_d['date_format_1']].append(1)
        bulk_update = []
        for d in valid_exercise_student_data:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + d}, \
                                         {'$set': {"valid_exercise_student_counts": sum(valid_exercise_student_data[d]),
                                                   "date": datetime.datetime.strptime(d, "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))

        # for valid_exercise in valid_exercises:
        #     bulk_update.append(UpdateOne({"_id": str(school['id'])+'@'+valid_exercise['date_format_1']},\
        #      {'$set': {"valid_exercise_student_counts": valid_exercise['user_count'],\
        #      "school_id": school['id']}}, upsert=True ))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)
        self._exercise_counts(school, valid_exercise_data)

    # def _valid_exercise_students_counts(self, school):
    #     """
    #     一天中每次有效考试的学生数
    #     """
    #     #todo 查询修改
    #     join = ob_groupuser.join(ob_group,
    #         and_(ob_groupuser.c.group_id== ob_group.c.id,
    #             ob_group.c.school_id == school['id'],
    #             ob_group.c.available == 1,
    #             ob_groupuser.c.available == 1)).join(as_hermes, and_(
    #                 ob_groupuser.c.user_id == as_hermes.c.student_id,
    #                 as_hermes.c.available == 1 ,
    #                 ob_groupuser.c.available == 1
    #             )).join(ob_exercise, and_(ob_exercise.c.id == as_hermes.c.exercise_id,
    #              ob_exercise.c.available == 1)).alias("a")
    #     q_valid_exercises = select([join, func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d"), func.count(join.c.sigma_account_re_groupuser_user_id).\
    #     label("user_count")]).select_from(join).group_by(func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d")).\
    #     having('user_count >= 10').\
    #     order_by(asc(join.c.sigma_pool_as_hermes_time_create))
    #     valid_exercises = self._query(q_valid_exercises)
    #     bulk_update = []
    #     for valid_exercise in valid_exercises:
    #         bulk_update.append(UpdateOne({"_id": str(school['id'])+'@'+valid_exercise['date_format_1']},\
    #          {'$set': {"valid_exercise_student_counts": valid_exercise['user_count'],\
    #          "school_id": school['id']}}, upsert=True ))
    #     if bulk_update:
    #         bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
    #         print (bulk_update_ret.bulk_api_result)
    #     self._exercise_counts(school, valid_exercises)

    def _exam_images_counts(self, school):
        """
        一天考试图像上传数
        """
        j = as_hermes.join(us_user,
                           and_(as_hermes.c.student_id == us_user.c.id,
                                us_user.c.school_id == school['id'],
                                us_user.c.available == 1,
                                as_hermes.c.available == 1))
        q_exam_images = select([as_hermes.c.time_create, func.date_format(as_hermes.c.time_create, "%Y-%m-%d"),
                                func.count(as_hermes.c.id).label('image_count')]).select_from(j). \
            group_by(func.date_format(as_hermes.c.time_create, "%Y-%m-%d")).order_by(asc(as_hermes.c.time_create))

        exam_images = self._query(q_exam_images)
        bulk_update = []
        for exam_image in exam_images:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + exam_image['date_format_1']}, \
                                         {'$set': {"exam_images": exam_image['image_count'],
                                                   "date": datetime.datetime.strptime(exam_image['date_format_1'],
                                                                                      "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)

    def _paid_counts(self, school):
        """
        每天付费人数 和 每天付费总数
        """
        j = ob_order.join(us_user,
                          and_(
                              ob_order.c.available == 1,
                              ob_order.c.coupon_amount > 0,
                              ob_order.c.status == 3,
                              us_user.c.school_id == school['id'],
                              ob_order.c.user_id == us_user.c.id)).join(re_userwechat,
                                                                        and_(re_userwechat.c.user_id == us_user.c.id))
        q_pays = select([ob_order.c.time_create, func.date_format(ob_order.c.time_create, "%Y-%m-%d"),
                         func.count(ob_order.c.user_id).label('paid_count'),
                         func.sum(ob_order.c.coupon_amount).label('paid_amount')]). \
            select_from(j). \
            group_by(func.date_format(ob_order.c.time_create, "%Y-%m-%d")).order_by(asc(ob_order.c.time_create))

        pays = self._query(q_pays)
        # print (guardians)
        bulk_update = []
        for pay in pays:
            bulk_update.append(UpdateOne({"_id": str(school['id']) + '@' + pay['date_format_1']}, \
                                         {'$set': {"paid_counts": pay['paid_count'],
                                                   "paid_amount": pay['paid_amount'],
                                                   "date": datetime.datetime.strptime(pay['date_format_1'], "%Y-%m-%d"),
                                                   "school_id": school['id']}}, upsert=True))
        if bulk_update:
            # print (bulk_update)
            bulk_update_ret = self.mongo.school_per_day.bulk_write(bulk_update)
            # print (bulk_update_ret.bulk_api_result)
