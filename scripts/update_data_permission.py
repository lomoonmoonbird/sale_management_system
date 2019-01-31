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


# #include area
#
# # mongo.data_permission.update({
# #
# #     "user_id": "3006621"
# # },
# #     {
# #         "$push": {"include_area": ""}
# #     }
# # )
#
# #include channel
# areas = mongo.instance.find({"role": 2, 'status': 1, 'name': {"$ne": '自营大区'}})
# area_ids = [str(item['_id']) for item in areas]
# channels = mongo.instance.find({"parent_id": {"$in": area_ids}, 'status': 1})
# channel_ids =  [str(item['old_id']) for item in channels]
# mongo.data_permission.update({
#
#     "user_id": "3006621"
# },
#     {
#         "$push": {"include_channel": {"$each": channel_ids}}
#     }
# )
#
# #exclude channel
# exclude_channel = mongo.data_permission.find_one({"user_id": "3006621"})
# # e = [id for id in exclude_channel.get("exclude_channel", []) if id not in []]
# mongo.data_permission.update({"user_id": "3006621"}, {"$set": {"exclude_channel": [28444, 376635, 377159, 499054, 509060, 946455 ]}})
#
# #exclude school
# exclude_school_sql = "select id from sigma_account_ob_school where available = 1 and owner_id in (%s)" % (",".join([str(id) for id in [28444, 376635, 377159, 499054, 509060, 946455 ]]))
# cursor = connection.cursor()
# cursor.execute(exclude_school_sql)
# e_school= cursor.fetchall()
# e_school_id = [item['id'] for item in e_school]
# mongo.data_permission.update({
#
#     "user_id": "3006621"
# },
#     {
#         "$set": {"exclude_school": e_school_id}
#     }
# )
#
# # i = mongo.data_permission.update({
# #
# #     "user_id": "3006621"
# # },
# #     {
# #         "$pull": {"exclude_channel": {"$in": [22716]}}
# #     }
# # )
# #
# # i = mongo.data_permission.update({
# #
# #     "user_id": "3006621"
# # },
# #     {
# #         "$push": {"include_channel": 22716}
# #     }
# # )
#
# exclude_channel = mongo.data_permission.find_one({"user_id": "3006621"})
# exclude_channel_ids = exclude_channel.get("exclude_channel", [])
#
# # schoo_id_sql = "select id from sigma_account_ob_school where  available = 1 and owner_id in (%s); " % (",".join([str(id) for id in exclude_channel_ids]))
# # cursor = connection.cursor()
# # cursor.execute(schoo_id_sql)
# # exclude_schoo_id = cursor.fetchall()
#
#
# # i = mongo.data_permission.update({
# #
# #     "user_id": "3006621"
# # },
# #     {
# #         "$set": {"exclude_school": [item['id'] for item in exclude_schoo_id]}
# #     }
# # )
#
#
# area = mongo.instance.find_one({"name": "华南大区", "status": 1})


# aa  = mongo.data_permission.find_one({"user_id": "3006621"})
#
# ccc = aa.get("include_channel", [])
#
# ddd = []
# for c in ccc:
#     try:
#         int(c)
#         ddd.append(int(c))
#     except:
#         continue
# ddd += [1021498 ,1021572, 1022123]
# mongo.data_permission.update({"user_id": "3006621"}, {"$set": {"include_channel": ddd}})

aa = mongo.data_permission.find_one({"user_id": "3006621"})
print ([item for item in aa['include_channel']])
server_mongo.stop()
server.stop()
# cursor.close()
connection.close()