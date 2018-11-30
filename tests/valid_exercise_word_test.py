import pymongo
from configs import DEBUG, MONGODB_CONN_URL, MYSQL_NAME, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT
mongo = pymongo.MongoClient(MONGODB_CONN_URL).sales



ag_class = mongo.class_per_day.aggregate([
    # {
    #     "$match": {"channel": {"$nin": ['null', None] + exclude_channels}}
    # },
    {
        "$project": {
            "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
        }
    },

    {"$group": {"_id": None,
                "total": {"$sum": "$total"},
                }
     },
])

ag_grade = mongo.grade_per_day.aggregate([
    # {
    #     "$match": {"channel": {"$nin": ['null', None] + exclude_channels}}
    # },
    {
        "$project": {
            "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
        }
    },

    {"$group": {"_id": None,
                "total": {"$sum": "$total"},
                }
     },
])

ag_school = mongo.school_per_day.aggregate([
    # {
    #     "$match": {"channel": {"$nin": ['null', None] + exclude_channels}}
    # },
    {
        "$project": {
            "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
        }
    },

    {"$group": {"_id": None,
                "total": {"$sum": "$total"},
                }
     },
])

ag_channel = mongo.channel_per_day.aggregate([
    # {
    #     "$match": {"channel": {"$nin": ['null', None] + exclude_channels}}
    # },
    {
        "$project": {
            "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
        }
    },

    {"$group": {"_id": None,
                "total": {"$sum": "$total"},
                }
     },
])



for a1 in ag_class:
    print(a1)

for a2 in ag_grade:
    print(a2)

for a1 in ag_school:
    print(a1)

for a2 in ag_channel:
    print(a2)