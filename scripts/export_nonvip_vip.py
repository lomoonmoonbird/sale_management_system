import pymongo
import re
from collections import defaultdict
import json
from celery import Task
from models.mysql.centauri import ob_school, us_user, re_userwechat, ob_group, \
ob_groupuser, ob_exercise, as_hermes, ob_order, re_userwechat, Roles, StageEnum, \
    StudentRelationEnum, ExerciseTypeEnum, ob_exercisemeta, st_location, ob_reading
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
from openpyxl import load_workbook

server = SSHTunnelForwarder(
    ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

    ssh_password="PengKim@89527",
    ssh_username="jinpeng",
    remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
server.start()

server_mongo = SSHTunnelForwarder(
    ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

    ssh_password="PengKim@89527",
    ssh_username="jinpeng",
    remote_bind_address=('dds-uf6fcc4e461ee5a41.mongodb.rds.aliyuncs.com', 3717))
server_mongo.start()

mongo = pymongo.MongoClient('127.0.0.1',
                            username='root',
                            password = 'sigmaLOVE2017',
                            authMechanism='SCRAM-SHA-1',
                            port=server_mongo.local_bind_port).sales
connection = pymysql.connect(host="127.0.0.1",
                                  port=server.local_bind_port,
                                  user="sigma",
                                  password="sigmaLOVE2017",
                                  db=MYSQL_NAME,
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)


s1 = 2730
s2 = 3410

q1 = "select id ,student_vip_expire " \
     "from sigma_account_us_user " \
     "where available = 1 " \
     "and id in (select user_id from sigma_account_re_groupuser " \
     "where available = 1 and role_id = 2 " \
     "and group_id " \
     "in (select id from sigma_account_ob_group where available = 1 and school_id = 3410 and grade in ('2016',' 2018')))"
cursor = connection.cursor()
cursor.execute(q1)
s1 = cursor.fetchall()

# s1_students = [item['id'] for item in s1]

# qq1 = "select * from sigma_pay_ob_order where available =1 and status = 3 and user_id in (%s)" % (",".join([str(id) for id in s1_students]))
# cursor = connection.cursor()
# cursor.execute(qq1)
# order = cursor.fetchall()
# order_map = {}
# for o in order:
#     order_map[o['user_id']]
count = 0
for s in s1:
    if s['student_vip_expire'] >0 and s['student_vip_expire']>time.time():
        count+=1
        print('111111')
print(count)

print(s1)

server_mongo.stop()
server.stop()
cursor.close()
connection.close()