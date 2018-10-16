#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""
from celery import Task
from models.mysql.centauri import ob_school
import pymysql
from sqlalchemy.dialects import mysql

# sales_celery = Celery("Sales")
# sales_celery.config_from_object('configs')
import pickle

class BaseTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print (einfo)
        # if isinstance(exc, Exception):
        #     raise Exception
    def on_success(self, retval, task_id, args, kwargs):
        print ('success')

class SchoolTask(BaseTask):
    def __init__(self):
        super(SchoolTask, self).__init__()
    
    
    def run(self):
        # while True:
        #     import time
        #     print ("$$$$$$$$$$")
        #     time.sleep(3)
        connection = pymysql.connect(host='mysql.hexin.im',
                             user='root',
                             password='sigmalove',
                             db='sigma_centauri_new',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    
    

    
        u = ob_school.select().where(ob_school.c.uid > "1")

        try:
            with connection.cursor() as cursor:
                # Create a new record
                cursor.execute(u.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string)
                result = cursor.fetchall()
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()
        print (result)


