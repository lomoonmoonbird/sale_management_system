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
local_mongo = pymongo.MongoClient().sales

# class_per_day_local = local_mongo.class_per_day.find({})
#
# class_count = 0
# class_update = []
# for c_p_d_local in class_per_day_local:
#     class_count+=1
#     # mongo.class_per_day.update({"group_id": c_p_d_local['group_id'], "day": c_p_d_local['day']},
#     #                            {"$set": {"guardian_count":c_p_d_local.get('guardian_count', 0),
#     #                                      "wechats": c_p_d_local.get('wechats',[]),
#     #                                      "guardian_unique_count": c_p_d_local.get('guardian_unique_count', 0),
#     #                                      "wechat_user_ids":c_p_d_local.get('wechat_user_ids',[])}})
#
#     class_update.append(
#         UpdateOne({"group_id": c_p_d_local['group_id'], "day": c_p_d_local['day']},
#                                {"$set": {"guardian_count":c_p_d_local.get('guardian_count', 0),
#                                          "wechats": c_p_d_local.get('wechats',[]),
#                                          "guardian_unique_count": c_p_d_local.get('guardian_unique_count', 0),
#                                          "wechat_user_ids":c_p_d_local.get('wechat_user_ids',[])}}))
#     print("class_count:", class_count)
# if class_update:
#     try:
#         bulk_update_ret = mongo.class_per_day.bulk_write(
#             class_update)
#         # print(bulk_update_ret.bulk_api_result)
#     except BulkWriteError as bwe:
#         print(bwe.details)
#
#
# grade_per_day_local = local_mongo.grade_per_day.find({})
# grade_count = 0
# grade_update = []
# for g_p_d_local in grade_per_day_local:
#     grade_count +=1
#     # mongo.grade_per_day.update({"school_id":g_p_d_local['school_id'],
#     #                             "grade": g_p_d_local['grade'],
#     #                             "day": g_p_d_local['day']},
#     #                            {"$set": {"guardian_count":g_p_d_local.get('guardian_count', 0),
#     #                                      "wechats": g_p_d_local.get('wechats',[]),
#     #                                      "guardian_unique_count": g_p_d_local.get('guardian_unique_count', 0),
#     #                                      "wechat_user_ids":g_p_d_local.get('wechat_user_ids',[])}})
#     grade_update.append(
#         UpdateOne({"school_id":g_p_d_local['school_id'],
#                                 "grade": g_p_d_local['grade'],
#                                 "day": g_p_d_local['day']},
#                                {"$set": {"guardian_count":g_p_d_local.get('guardian_count', 0),
#                                          "wechats": g_p_d_local.get('wechats',[]),
#                                          "guardian_unique_count": g_p_d_local.get('guardian_unique_count', 0),
#                                          "wechat_user_ids":g_p_d_local.get('wechat_user_ids',[])}}))
#     print("grade_count", grade_count)
# if grade_update:
#     try:
#         bulk_update_ret = mongo.grade_per_day.bulk_write(
#             grade_update)
#         # print(bulk_update_ret.bulk_api_result)
#     except BulkWriteError as bwe:
#         print(bwe.details)

school_per_day_local = local_mongo.school_per_day.find({})
school_count= 0
school_update = []
for s_p_d_local in school_per_day_local:
    school_count+=1
    # mongo.school_per_day.update({"school_id":s_p_d_local['school_id'],
    #                             "day": s_p_d_local['day']},
    #                            {"$set": {"guardian_count":s_p_d_local.get('guardian_count', 0),
    #                                      "wechats": s_p_d_local.get('wechats',[]),
    #                                      "guardian_unique_count": s_p_d_local.get('guardian_unique_count', 0),
    #                                      "wechat_user_ids":s_p_d_local.get('wechat_user_ids',[])}})

    school_update.append(
        UpdateOne({"school_id":s_p_d_local['school_id'],
                                "day": s_p_d_local['day']},
                               {"$set": {"guardian_count":s_p_d_local.get('guardian_count', 0),
                                         "wechats": s_p_d_local.get('wechats',[]),
                                         "guardian_unique_count": s_p_d_local.get('guardian_unique_count', 0),
                                         "wechat_user_ids":s_p_d_local.get('wechat_user_ids',[])}}))
    print("school_count;", school_count)
if school_update:
    try:
        bulk_update_ret = mongo.school_per_day.bulk_write(
            school_update)
        # print(bulk_update_ret.bulk_api_result)
    except BulkWriteError as bwe:
        print(bwe.details)


# channel_count = 0
# channel_per_day_local = local_mongo.channel_per_day.find({})
# for c_p_d_local in channel_per_day_local:
#     channel_count+= 1
#     mongo.channel_per_day.update({"channel":c_p_d_local['channel'],
#                                 "day": c_p_d_local['day']},
#                                {"$set": {"guardian_count":c_p_d_local.get('guardian_count', 0),
#                                          "wechats": c_p_d_local.get('wechats',[]),
#                                          "guardian_unique_count": c_p_d_local.get('guardian_unique_count', 0),
#                                          "wechat_user_ids":c_p_d_local.get('wechat_user_ids',[])}})
#     print(channel_count)

server_mongo.stop()
server.stop()
connection.close()