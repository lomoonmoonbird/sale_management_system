#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, ob_groupuser, as_hermes, Roles
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
        self.bulk_update = []
        self.total_dict = defaultdict(float)
        self.coll_total = "total"
    
    def _query(self, query):
        """
        执行查询 返回数据库结果
        """
        try:
            with self.connection.cursor() as cursor:
                logger.debug(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string)
                cursor.execute(query.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string)
                ret = cursor.fetchall()
        except:
            ret = []
        return ret

    def run(self):

        #查询所有学校
        q_schools = select(["*"]).where(ob_school.c.available==1)
        schools = self._query(q_schools)

        self.total_dict['total_school'] = len(schools)
        for school in schools:
            self._total(school)
            school.update(self.total_dict)
            school['stages'] = self._stage(school)
            if school:
                self.bulk_update.append(UpdateOne({"_id": school['id']}, {'$set': school}, upsert=True ))
            # time.sleep(0.8)
        # from tasks.celery_init import sales_celery
        # sales_celery.send_task('tasks.celery_task.Math', args=[4,5])
        if self.bulk_update:
            bulk_update_ret = self.mongo.school.bulk_write(self.bulk_update)
            print (bulk_update_ret.bulk_api_result)
        self.bulk_update = []
        # print (schools)


    def _stage(self, school):
        """
        判断年级阶段和学校阶段
        """

        join = ob_groupuser.join(ob_group, 
            and_(ob_groupuser.c.group_id== ob_group.c.id, 
                ob_group.c.school_id == school['id'], 
                ob_group.c.available == 1,
                ob_groupuser.c.available == 1)).outerjoin(as_hermes, and_(
                    ob_groupuser.c.user_id == as_hermes.c.student_id,
                    as_hermes.c.available == 1 ,
                    ob_groupuser.c.available == 1
                )).alias("a")
        q_stages = select([join, func.count(join.c.sigma_account_re_groupuser_user_id).\
        label("user_count")]).select_from(join).group_by(join.c.sigma_account_re_groupuser_user_id).\
        order_by(asc(join.c.sigma_pool_as_hermes_time_create))
        stages = self._query(q_stages)
        return stages

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