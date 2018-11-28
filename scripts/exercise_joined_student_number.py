import pymongo
import re
from collections import defaultdict
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT
mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales


data = mongo.record_grade_exercise_delta.find({"exercise_school_grade_id": re.compile('@3041@')})

item_defaultdict = defaultdict(list)
for d in data:
    item_defaultdict[d["exercise_school_grade_id"].split("@")[2]] += d['user_id']


for grade, students in item_defaultdict.items():
    print(grade, len(list(set(students))))