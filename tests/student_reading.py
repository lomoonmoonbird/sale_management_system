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
import pymongo
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT
mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

server = SSHTunnelForwarder(
    ssh_address_or_host=('139.196.77.128', 5318),  # 跳板机

    ssh_password="PengKim@89527",
    ssh_username="jinpeng",
    remote_bind_address=('rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com', 3306))
server.start()
connection = pymysql.connect(host="127.0.0.1",
                                  port=server.local_bind_port,
                                  user="sigma",
                                  password="sigmaLOVE2017",
                                  db=MYSQL_NAME,
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)

# readings = mongo.record_reading_delta.find({"reading_uid": "1bbdeb4421ca4853958c"})
#
#
# students = list(set(list(readings)[0]['students'])) if readings[0] else []
#
#
# sql_user = "select id,username,school_id from sigma_account_us_user where id in (%s)" % (
#     ",".join(["'" + str(id) + "'" for id in students]))
# print(sql_user)
# cursor = connection.cursor()
# cursor.execute(sql_user)
# ret = cursor.fetchall()


old_ids = ['40081','40082','89629','408120','679169','679218', '727722', '825125','872639','913787','922346']
sql = "select * from sigma_account_us_user " \
      "where available = 1 " \
      "and role_id = 6 " \
      "and time_create >= '%s' " \
      "and time_create <= '%s' " \
      "and id in (%s)" % (
          "2018-01-01",
          "2018-12-20",
          ','.join(old_ids))
print(sql)
cursor = connection.cursor()
cursor.execute(sql)
ret = cursor.fetchall()
print(ret)
connection.close()
server.stop()
