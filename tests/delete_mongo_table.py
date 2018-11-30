import pymongo
import os
os.sys.path.insert(0, '/app/hexin/Sales-management-system')
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT
mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


collections = ["channel_per_day", "class_per_day", "school_per_day","grade", "grade_per_day",
               "record_channel_exercise_delta", "record_channel_word_delta","record_class_exercise_delta",
               "record_class_word_delta", "record_grade_exercise_delta",
               "record_grade_word_delta", "record_reading_delta", "record_school_exercise_delta",
               "record_school_word_delta", "record_schoolstage_delta", "sale_time_threshold", "school",
               "school_tmp", "schools","tmp_channel_per_day","tmp_class_per_day","tmp_grade_per_day", "tmp_school_per_day" ]

for coll in collections:
    mongo[coll].drop()


print('complete .')