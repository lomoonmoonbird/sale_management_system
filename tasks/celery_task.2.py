#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, ob_groupuser, as_hermes, Roles, StageEnum
import pymysql
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from sqlalchemy import select, func, asc
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.dialects import mysql
from configs import MONGODB_CONN_URL
from loggings import logger
import pickle
import time
import datetime
from datetime import timedelta
from collections import defaultdict

class BaseTask(Task):
    def __init__(self):
        super(BaseTask, self).__init__()
        self.start_time = datetime.datetime(2018,1,1)
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print (einfo)

    def on_success(self, retval, task_id, args, kwargs):
        print ('success')

class SummaryTask(BaseTask):
    def __init__(self):
        super(SummaryTask, self).__init__()
        self.connection = pymysql.connect(host='mysql.hexin.im',
                             user='root',
                             password='sigmalove',
                             db='sigma_centauri_new',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        self.mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales
        self.coll_time_threshold = 'sale_time_threshold'
        self.bulk_update = []
        self.total_dict = defaultdict(float)
        self.coll_total = "total"
        self.time_threshold_table = {}
    
    def _time_per_day(self):
        self.time_threshold_table = self.mongo[self.coll_time_threshold].find_one({"_id": "sale_time_threshold"}) or {}
        print ("self.time_threshold_table", self.time_threshold_table)
    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        try:
            with self.connection.cursor() as cursor:
                logger.debug(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string.replace("%%","%"))
                cursor.execute(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string)
                ret = cursor.fetchall()
        except:
            import traceback
            traceback.print_exc()
            ret = []
        return ret

    def run(self):
        self._time_per_day()

        schools = self._school()
        print (schools)
        for school in schools:
            self._stage(school)
            if school:
                self.bulk_update.append(UpdateOne({"_id": school['id']}, {'$set': school}, upsert=True ))
        self.mongo[self.coll_time_threshold].update({"_id": "sale_time_threshold"}, {"$set": {"school_begin_time": schools[-1]['time_create']}},upsert=True)
        #todo 批量插入学校输入，批量插入可优化
        if self.bulk_update:
            bulk_update_ret = self.mongo.school.bulk_write(self.bulk_update)
            print (bulk_update_ret.bulk_api_result)
        self.bulk_update = []

    def _school(self)->list:
        """
        正序获取学校记录
        如果有记录最后时间则从最后时间开始到前一天，否则从默认开始时间到当前时间前一天
        """
        print ('self.time_threshold_table.get("school_begin_time", self.start_time)', self.time_threshold_table.get("school_begin_time", self.start_time),type(self.time_threshold_table.get("school_begin_time", self.start_time)))

        # t = datetime.datetime.strptime(self.time_threshold_table.get("school_begin_time", self.start_time), "%Y-%m-%d")
        t = self.time_threshold_table.get("school_begin_time", self.start_time)
        q_schools = select([ob_school]).where(and_(ob_school.c.available==1, 
                                    ob_school.c.time_create > t.strftime("%Y-%m-%d"), 
                                    ob_school.c.time_create <  (datetime.datetime.now() - timedelta(1)).strftime("%Y-%m-%d"))).\
                                    order_by(asc(ob_school.c.time_create))
        schools = self._query(q_schools)
        return schools

    def _stage(self, school):
        """
        年级阶段和学校阶段
        获取
        """
        import collections
        join = ob_groupuser.join(ob_group, 
            and_(ob_groupuser.c.group_id== ob_group.c.id, 
                ob_group.c.school_id == school['id'], 
                ob_group.c.available == 1,
                ob_groupuser.c.available == 1)).outerjoin(as_hermes, and_(
                    ob_groupuser.c.user_id == as_hermes.c.student_id,
                    as_hermes.c.available == 1 ,
                    ob_groupuser.c.available == 1
                )).alias("a")
        q_stages = select([join, func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d"), func.count(join.c.sigma_account_re_groupuser_user_id).\
        label("user_count")]).select_from(join).group_by(func.date_format(join.c.sigma_pool_as_hermes_time_create, "%Y-%m-%d")).\
        order_by(asc(join.c.sigma_pool_as_hermes_time_create))
        stages = self._query(q_stages)
        classes = {k['sigma_account_ob_group_grade'] for k in stages}
        class_stage = {}
        for clazz in classes:
            for stage in stages:
                if stage['sigma_account_ob_group_grade'] == clazz:
                    if stage['user_count'] >= 10:
                        class_stage[clazz] = {"stage": StageEnum.Using.value, "time": stage['sigma_pool_as_hermes_time_create']}
                        break
            
            class_stage[clazz] = {"stage": StageEnum.Register.value, "time": school['time_create']}
        school['stage'] = StageEnum.Register.value if not class_stage else  min([v['stage'] for k,v in class_stage.items()])
        school['register_start_time'] = school['time_create']
        school['using_start_time'] = '' if school['stage'] == StageEnum.Register.value else class_stage[school['stage']]['time']

        q_grades_classes = select([ob_group]).where(and_(ob_group.c.school_id == school['id'], ob_group.c.available==1))
        grades_classes = self._query(q_grades_classes)
        # print (school)
        # grades_classes_dict = defaultdict(list)
        # for g_c in grades_classes:
        school['grades_classes'] = grades_classes
        
        return None
        
        

    def _total(self, school):
        #老师
        q_teachers = select([us_user]).where(
            and_(us_user.c.school_id == school['id'], 
            us_user.c.role_id == Roles.TEACHER.value)
            )
        #学生
        q_students = select([us_user]).where(
            and_(us_user.c.school_id == school['id'], 
            us_user.c.role_id == Roles.STUDENT.value)
            )
        #家长
        q_guardian = select([us_user, re_userwechat]).\
        select_from(us_user.join(re_userwechat, 
            and_(re_userwechat.c.user_id==us_user.c.id, 
                re_userwechat.c.relationship > 0, 
                us_user.c.school_id==school['id'],
                us_user.c.available == 1,
                re_userwechat.c.available == 1))
            )

            
        teachers = self._query(q_teachers)
        students = self._query(q_students)
        guardians = self._query(q_guardian)
        self.total_dict['total_teachers'] = len(teachers)
        self.total_dict['total_students'] = len(students)
        self.total_dict['total_guardian'] = len(guardians)
        



class Math(BaseTask):
    def run(self, x, y):
        print ("sum = :", x+y)