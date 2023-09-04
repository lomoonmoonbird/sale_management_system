import pymongo
import re
from collections import defaultdict
import json

from celery import Task
from enumconstant import Roles
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
from openpyxl import load_workbook, Workbook

LEVEL = {"primary": "小学", "junior":"初中", "senior": "高中"}

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
#
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
# mongo =  pymongo.MongoClient().sales
instance_db = mongo.instance


items = mongo.channel_per_day.aggregate([

# {
#         "$match": {"grade": {"$in": grade_ids}}
#     },
    {

        "$project": {
            "channel": 1,
            "day": 1,
            "guardian_unique_count": 1,
            "guardian_count": 1,
            "student_number": 1,
            "valid_exercise_count": {
                            "$cond": [{"$and": [{"$lte": ["$day", "2018-12-26"]}, {
                                "$gte": ["$day", "2018-09-01"]}]}, "$valid_exercise_count", 0]},
            "valid_word_count": {
                            "$cond": [{"$and": [{"$lte": ["$day", "2018-12-26"]}, {
                                "$gte": ["$day", "2018-09-01"]}]}, "$valid_word_count", 0]},

            "valid_exercise_count_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", "2018-12-26"]}, {
                                "$gte": ["$day", "2018-12-01"]}]}, "$valid_exercise_count", 0]},
        }

    },
    {
        "$group": {
            "_id": "$channel",
            "valid_exercise_count": {"$sum": "$valid_exercise_count"},
            "valid_word_count": {"$sum": "$valid_word_count"},
            "valid_exercise_count_month": {"$sum": "$valid_exercise_count_month"},
            "total_guardian": {'$sum': "$guardian_unique_count"},
            "total_guardian_2": {'$sum': "$guardian_count"},
            "total_student_number": {"$sum": "$student_number"}
        }

    },
    {
        "$project": {
            "channel": 1,
            "valid_exercise_count": 1,
            "valid_word_count": 1,
            "valid_exercise_count_month": 1,
            "total_guardian": 1,
            "total_guardian_2":1 ,
            "total_student_number": 1,
            "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian", "$total_student_number"]}]},
        }}

])


print(json.dumps(list(items), indent=4))
server_mongo.stop()
server.stop()
connection.close()
