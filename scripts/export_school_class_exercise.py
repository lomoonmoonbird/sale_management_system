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

data = mongo.class_per_day.aggregate([
	{
		"$match": {

			"school_id": 3320
		}
	},
	{
		"$project": {
			"school_id": 1,
			"group_id": 1,
			"valid_exercise_count": 1,
		}
	},
	{
		"$group": {
			"_id": {"school_id": "$school_id", "group_id": "$group_id"},
			"total": {"$sum": "$valid_exercise_count"},
		}
	},
	{
		"$project": {
			"total": 1,
		}
	}

])

data = list(data)

data_map = {}
for d in data:
    data_map[str(d['_id']['school_id']) + "@" + str(d['_id']['group_id'])] = d['total']

# print(json.dumps(data, indent=4))

all_group_id = "select * from sigma_account_ob_group where school_id = 3320 and available = 1; "
cursor = connection.cursor()
cursor.execute(all_group_id)
all_group_ids = cursor.fetchall()



final_data = []
for group_id in all_group_ids:
    final_data.append(
        {
            "school_id": group_id['school_id'],
            "class_name": group_id['name'],
            "exercise_count": data_map.get(str(group_id['school_id']) + "@" + str(group_id['id']))
        }

    )


print(json.dumps(final_data,indent=4), len(final_data))

server_mongo.stop()
server.stop()
cursor.close()
connection.close()