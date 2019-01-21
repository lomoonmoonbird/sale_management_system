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

channels = [73243.0,
        203886.0,
        282601.0,
        282602.0,
        314353.0,
        322795.0,
        361694.0,
        481174.0,
        499763.0,
        517835.0,
        632454.0,
        641727.0,
        749435.0,
        749436.0,
        873545.0,
        915753.0,
        282599.0,
        659985.0,
        872005.0,
        926127.0,
        946656.0,
        476385.0,
        527872.0,
        539547.0,
        554502.0,
        788478.0,
        956119.0,
        132407.0,
        858173.0,
        532139.0,
        662191.0,
        956211.0,
        903350.0,
        40081.0,
        40082.0,
        89629.0,
        408120.0,
        679169.0,
        679218.0,
        727722.0,
        825125.0,
        872639.0,
        913787.0,
        922346.0,
        618496.0,
        827521.0,
        832599.0,
        873426.0,
        924213.0,
        924672.0]
channels = [73243,203886,282601,282602,314353,322795,361694,481174,499763,517835,632454,641727,749435,749436,873545,915753,282599]
channels = [40081,40082,89629,408120,679169,679218,727722,825125,872639,913787,922346]
channels = [int(id) for id in channels]

print(channels)
begin = "2018-12-01"
end = "2019-01-03"
# items = mongo.channel_per_day.aggregate([
#
#     {
#         "$match": {"channel": {"$in": channels}}
#     },
#     {
#         "$project": {
#             "e_image_c_curr_month": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end ]}, {
#                     "$gte": ["$day", begin]}]}, "$e_image_c", 0]},
#             "e_image_c_last_month": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end]}, {
#                     "$gte": ["$day", begin]}]}, "$e_image_c", 0]},
#
#             "w_image_c_curr_month": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end]}, {
#                     "$gte": ["$day", begin]}]}, "$w_image_c", 0]},
#             "w_image_c_last_month": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end]}, {
#                     "$gte": ["$day", begin]}]}, "$w_image_c", 0]},
#
#             "teacher_number": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end]}, {
#                     "$gte": ["$day", begin]}]}, "$teacher_number", 0]},
#             "student_number": {
#                 "$cond": [{"$and": [{"$lte": ["$day", end]}, {
#                     "$gte": ["$day", begin]}]}, "$student_number", 0]},
#         }
#     },
#     {
#         "$group": {
#                 "_id": None,
#                 "e_image_c_curr_month": {"$sum": "$e_image_c_curr_month"},
#                 "e_image_c_last_month": {"$sum": "$e_image_c_last_month"},
#                 "w_image_c_curr_month": {"$sum": "$w_image_c_curr_month"},
#                 "w_image_c_last_month": {"$sum": "$w_image_c_last_month"},
#                 "total_images_curr_month": {
#                     "$sum": {"$add": ["$e_image_c_curr_month", "$w_image_c_curr_month"]}},
#                 "total_images_last_month": {
#                     "$sum": {"$add": ["$e_image_c_last_month", "$w_image_c_last_month"]}},
#                 "teacher_number": {"$sum": "$teacher_number"},
#                 "student_number": {"$sum": "$student_number"}
#         }
#     },
#     {
#         "$project": {
#             "total_images_curr_month": 1,
#             "total_images_last_month": 1,
#             "teacher_number": 1,
#             "student_number": 1
#         }
#     }
#
# ])
def _curr_and_last_and_last_last_month():
    """
    当月和上月分别开始和结束日期
    :return:
    """
    today = date.today()
    first_day_of_curr_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_curr_month - timedelta(days=1)

    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    last_day_of_last_last_month = first_day_of_last_month - timedelta(days=1)
    first_day_of_last_last_month = last_day_of_last_last_month.replace(day=1)
    return first_day_of_last_last_month.strftime("%Y-%m-%d"), last_day_of_last_last_month.strftime("%Y-%m-%d"), \
           first_day_of_last_month.strftime("%Y-%m-%d"), last_day_of_last_month.strftime("%Y-%m-%d"), \
           first_day_of_curr_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

last_last_month_first_day, last_last_month_last_day, last_month_first_day, last_month_last_day,\
        curr_month_first_day, curr_month_last_day = _curr_and_last_and_last_last_month()
items = mongo.channel_per_day.aggregate(
[
                {
                    "$match": {
                        "channel": {"$in": channels}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "city_number": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "refund_number": 1,
                        "refund_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count":1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "total_images": {"$sum": ["$e_image_c", "$w_image_c"]},

                        # "total_pay_number": {
                        #     "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                        #               "$pay_number", 0]},
                        # "total_pay_amount": {
                        #     "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                        #               "$pay_amount", 0]},
                        #
                        # "total_refund_number": {
                        #     "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                        #               "$refund_number", 0]},
                        # "total_refund_amount": {
                        #     "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                        #               "$refund_amount", 0]},

                        "city_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$city_number", 0]},
                        "city_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$city_number", 0]},

                        "school_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$school_number", 0]},
                        "school_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$school_number", 0]},

                        "teacher_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$teacher_number", 0]},
                        "teacher_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$teacher_number", 0]},

                        "student_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$student_number", 0]},
                        "student_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$student_number", 0]},

                        "guardian_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$guardian_count", 0]},
                        "guardian_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$guardian_count", 0]},

                        "guardian_unique_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$guardian_unique_count", 0]},
                        "guardian_unique_number_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$guardian_unique_count", 0]},

                        # "pay_number_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                        #     "$gte": ["$day", last_month_first_day]}]}, "$pay_number", 0]},
                        # "pay_number_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                        #     "$gte": ["$day", last_last_month_first_day]}]}, "$pay_number", 0]},
                        #
                        # "pay_amount_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                        #     "$gte": ["$day", last_month_first_day]}]}, "$pay_amount", 0]},
                        # "pay_amount_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                        #     "$gte": ["$day", last_last_month_first_day]}]}, "$pay_amount", 0]},

                        "valid_reading_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$valid_reading_count", 0]},
                        "valid_reading_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$valid_reading_count", 0]},

                        "valid_exercise_count_curr_month": {"$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                            "$gte": ["$day", last_month_first_day]}]}, "$valid_exercise_count", 0]},
                        "valid_exercise_count_last_month": {"$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                            "$gte": ["$day", last_last_month_first_day]}]}, "$valid_exercise_count", 0]},

                        "valid_word_count_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$valid_word_count", 0]},
                        "valid_word_count_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$valid_word_count", 0]},

                        "e_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$e_image_c", 0]},
                        "e_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$e_image_c", 0]},

                        "w_image_c_curr_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_month_last_day]}, {
                                "$gte": ["$day", last_month_first_day]}]}, "$w_image_c", 0]},
                        "w_image_c_last_month": {
                            "$cond": [{"$and": [{"$lte": ["$day", last_last_month_last_day]}, {
                                "$gte": ["$day", last_last_month_first_day]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "valid_reading_count": {"$sum": "$valid_reading_count"},
                            "valid_exercise_count": {"$sum": "$valid_exercise_count"},
                            "valid_word_count": {"$sum": "$valid_word_count"},
                            "e_image_c": {"$sum": "$e_image_c"},
                            "w_image_c": {"$sum": "$w_image_c"},
                            "total_images": {"$sum": "$total_images"},
                            "total_city_number": {"$sum": "$city_number"},
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_unique_guardian_number": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
                            "total_refund_amount": {"$sum": "$total_refund_amount"},
                            "city_number_curr_month": {"$sum": "$city_number_curr_month"},
                            "city_number_last_month": {"$sum": "$city_number_last_month"},
                            "school_number_curr_month": {"$sum": "$school_number_curr_month"},
                            "school_number_last_month": {"$sum": "$school_number_last_month"},
                            "teacher_number_curr_month": {"$sum": "$teacher_number_curr_month"},
                            "teacher_number_last_month": {"$sum": "$teacher_number_last_month"},
                            "student_number_curr_month": {"$sum": "$student_number_curr_month"},
                            "student_number_last_month": {"$sum": "$student_number_last_month"},
                            "guardian_number_curr_month": {"$sum": "$guardian_number_curr_month"},
                            "guardian_number_last_month": {"$sum": "$guardian_number_last_month"},
                            "guardian_unique_number_curr_month": {"$sum": "$guardian_unique_number_curr_month"},
                            "guardian_unique_number_last_month": {"$sum": "$guardian_unique_number_last_month"},
                            # "pay_number_curr_month": {"$sum": "$pay_number_curr_month"},
                            # "pay_number_last_month": {"$sum": "$pay_number_last_month"},
                            # "pay_amount_curr_month": {"$sum": "$pay_amount_curr_month"},
                            # "pay_amount_last_month": {"$sum": "$pay_amount_last_month"},
                            "valid_reading_count_curr_month": {"$sum": "$valid_reading_count_curr_month"},
                            "valid_reading_count_last_month": {"$sum": "$valid_reading_count_last_month"},
                            "valid_exercise_count_curr_month": {"$sum": "$valid_exercise_count_curr_month"},
                            "valid_exercise_count_last_month": {"$sum": "$valid_exercise_count_last_month"},
                            "valid_word_count_curr_month": {"$sum": "$valid_word_count_curr_month"},
                            "valid_word_count_last_month": {"$sum": "$valid_word_count_last_month"},
                            "e_image_c_curr_month": {"$sum": "$e_image_c_curr_month"},
                            "e_image_c_last_month": {"$sum": "$e_image_c_last_month"},
                            "w_image_c_curr_month": {"$sum": "$w_image_c_curr_month"},
                            "w_image_c_last_month": {"$sum": "$w_image_c_last_month"},
                            "total_images_curr_month": {"$sum": {"$add": ["$e_image_c_curr_month", "$w_image_c_curr_month"]}},
                            "total_images_last_month": {"$sum": {"$add": ["$e_image_c_last_month", "$w_image_c_last_month"]}},
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_city_number": 1,
                        "total_school_number": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_unique_guardian_number": 1,
                        # "total_pay_number": 1,
                        # "total_pay_amount": 1,
                        "valid_reading_count": 1,
                        "valid_exercise_count": 1,
                        "valid_word_count": 1,
                        "e_image_c": 1,
                        "w_image_c": 1,
                        "total_images": 1,
                        "total_images_curr_month":1,
                        "total_images_last_month": 1,
                        "city_number_curr_month": 1,
                        "city_number_last_month": 1,
                        "school_number_curr_month": 1,
                        "school_number_last_month": 1,
                        "teacher_number_curr_month": 1,
                        "teacher_number_last_month": 1,
                        "student_number_curr_month": 1,
                        "student_number_last_month": 1,
                        "guardian_number_curr_month": 1,
                        "guardian_number_last_month": 1,
                        "guardian_unique_number_curr_month": 1,
                        "guardian_unique_number_last_month": 1,
                        # "pay_number_curr_month": 1,
                        # "pay_number_last_month": 1,
                        # "pay_amount_curr_month": 1,
                        # "pay_amount_last_month": 1,
                        "valid_reading_count_curr_month": 1,
                        "valid_reading_count_last_month": 1,
                        "valid_exercise_count_curr_month": 1,
                        "valid_exercise_count_last_month": 1,
                        "valid_word_count_curr_month": 1,
                        "valid_word_count_last_month": 1,
                        "e_image_c_curr_month": 1,
                        "e_image_c_last_month": 1,
                        "w_image_c_curr_month": 1,
                        "w_image_c_last_month": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                 {"$divide": ["$total_unique_guardian_number", "$total_student_number"]}]},
                        # "pay_ratio": {"$divide": ["$total_pay_number", "$total_student_number"]},
                        # "bind_ratio": {"$divide": ["$total_guardian_number", "$total_student_number"]},
                        # "school_MoM":
                    }

                }

            ])

items_a = []
for item in items:
    items_a.append(item)

itemss = mongo.channel_per_day.find({"channel": {"$in": channels}, "day": {"$lte": end, "$gte":begin}})
itemss = list(itemss)
print(json.dumps(items_a,indent=4))

print(sum([item.get('student_number', 0) for item in itemss]))

server_mongo.stop()
server.stop()
connection.close()