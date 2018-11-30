import pymongo
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT
mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales

print(mongo)
total_teacher_class = mongo.class_per_day.aggregate([
                # {
                #     "$match": {"channel": {"$nin": ['null', None, -1] + exclude_channels}}
                # },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])

total_teacher_grade = mongo.grade_per_day.aggregate([
                # {
                #     "$match": {"channel": {"$nin": ['null', None, -1] + exclude_channels}}
                # },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])

total_teacher_school = mongo.school_per_day.aggregate([
                # {
                #     "$match": {"channel": {"$nin": ['null', None, -1] + exclude_channels}}
                # },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])

total_teacher_channel = mongo.channel_per_day.aggregate([
                # {
                #     "$match": {"channel": {"$nin": ['null', None, -1] }}
                # },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])


for a1 in total_teacher_class:
    print(a1)

for a2 in total_teacher_grade:
    print(a2)

for a1 in total_teacher_school:
    print(a1)

for a2 in total_teacher_channel:
    print(a2)