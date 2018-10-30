
from datetime import datetime, timedelta
import asyncio
import json
from aiohttp.web import Request
from utils import *

from tasks.celery_init import school_task
from demo.utils import assert_permissions


async def schools(req: Request):
    """
    学校分页列表
    :param req:
    :return:
    """
    url_param = await get_params(req)
    per_page = 10
    print (url_param)
    co_schools = req.app['mongodb'].sales.school\
        .find()\
        .skip((int(url_param['page']) - 1) * per_page)\
        .limit(per_page)
    schools = []
    async for school in co_schools:
        schools.append(school)


    school_ids = [ s['id'] for s in schools ]
    school_agg = req.app['mongodb'].sales.school_per_day.aggregate(
        [


            {"$match": {"school_id": {"$in": school_ids}}},
            {"$group": {"_id": "$school_id",
                        "total_teachers": {"$sum": "$teacher_counts"},
                        "total_students": {"$sum": "$student_counts"},
                        "total_guardians": {"$sum": "$guardian_counts"},
                        "total_valid_exercise_students": {"$sum": "$valid_exercise_student_counts"},
                        "total_valid_exercise": {"$sum": "$valid_exercise_counts"},
                        "total_paid_counts": {"$sum": "$paid_counts"},
                        "total_paid_amount": {"$sum": "$paid_amount"},

                        }
             },
            {"$project":

                {
                    "_id": 1,
                    "total_teachers": 1,
                    "total_students": 1,
                    "total_guardians": 1,
                    "total_valid_exercise_students": 1,
                    "total_valid_exercise": 1,
                    "total_paid_counts": 1,
                    "total_paid_amount": 1,
                    "binding_ratio": {"$cond": [{"$eq": ["$total_students", 0]}, 0,
                                                {"$divide": ["$total_guardians", "$total_students"]}]},
                    "pay_ratio": {"$cond": [{"$eq": ["$total_students", 0]}, 0,
                                            {"$divide": ["$total_paid_counts", "$total_students"]}]},
                }
            },

        ])
    school_agg_month = req.app['mongodb'].sales.school_per_day.aggregate(
        [

            {"$match": {"school_id": {"$in": school_ids},
                        "date":  {"$lt": datetime.combine( (datetime.now() - timedelta(1,0,0,0,0)).date(), datetime.min.time()),
                                  "$gt": datetime.combine( (datetime.now() - timedelta(30,0,0,0,0)).date() , datetime.min.time())}
                        }
            },
            {"$group": {"_id": "$school_id",
                        "total_teachers": {"$sum": "$teacher_counts"},
                        "total_students": {"$sum": "$student_counts"},
                        "total_guardians": {"$sum": "$guardian_counts"},
                        "total_valid_exercise_students": {"$sum": "$valid_exercise_student_counts"},
                        "total_valid_exercise_per_month": {"$sum": "$valid_exercise_counts"},
                        "total_paid_counts": {"$sum": "$paid_counts"},
                        "total_paid_amount": {"$sum": "$paid_amount"},
                        "total_exam_images_per_month": {"$sum": "$exam_images"}
                        }
             },
            {"$project":

                {
                    "_id": 1,
                    "total_valid_exercise_students": 1,
                    "total_valid_exercise_per_month": 1,
                    "total_exam_images_per_month": 1,

                    "exercise_counts_per_person_per_month_ratio": {
                        "$cond": [{"$eq": ["$total_valid_exercise_students", 0]}, 0,
                                  {"$divide": ["$total_valid_exercise_per_month",
                                               "$total_valid_exercise_students"]}]},
                }
            },
        ]
    )
    school_sta= []
    school_sta_per_month = []
    async for s_a in school_agg:
        school_sta.append(s_a)
    async for s_a_m in school_agg_month:
        school_sta_per_month.append(s_a_m)

    for i, j in zip(school_sta, school_sta_per_month):
        i.update(j)
    # print (json.dumps(school_sta, indent=4))
    grade_agg = req.app['mongodb'].sales.grade_per_day.aggregate(
        [
            # {"$project": {"_id": 1, "date": 1, "school_id": 1, "teacher_counts": 1, "student_counts": 1,
            #               "valid_exercise_student_counts": 1}},
            {"$match": {"school_id": {"$in": school_ids}}},
            {"$group": {"_id": {"grade":"$grade","school_id": "$school_id"},
                        "total_teachers": {"$sum": "$teacher_counts"},
                        "total_students": {"$sum": "$student_counts"},
                        "total_guardians": {"$sum": "$guardian_counts"},
                        "total_valid_exercise_students": {"$sum": "$valid_exercise_student_counts"},
                        "total_valid_exercise": {"$sum": "$valid_exercise_counts"},
                        "total_paid_counts": {"$sum": "$paid_counts"},
                        "total_paid_amount": {"$sum": "$paid_amount"},

                        }
             },
            {"$project":

                {
                    "_id": 1,
                    "total_teachers": 1,
                    "total_students": 1,
                    "total_guardians": 1,
                    "total_valid_exercise_students": 1,
                    "total_valid_exercise": 1,
                    "total_paid_counts": 1,
                    "total_paid_amount": 1,
                    "binding_ratio": {"$cond": [{"$eq": ["$total_students", 0]}, 0,
                                                {"$divide": ["$total_guardians", "$total_students"]}]},
                    "pay_ratio": {"$cond": [{"$eq": ["$total_students", 0]}, 0,
                                            {"$divide": ["$total_paid_counts", "$total_students"]}]},
                }
            }
        ])
    grade_agg_month = req.app['mongodb'].sales.grade_per_day.aggregate(
        [
            {"$match": {"school_id": {"$in": school_ids},
                        # "date":  {"$lt": datetime.combine( (datetime.now() - timedelta(1,0,0,0,0)).date(), datetime.min.time()),
                        #           "$gt": datetime.combine( (datetime.now() - timedelta(30,0,0,0,0)).date() , datetime.min.time())}
                        }
             },
            {"$group": {"_id": {"grade":"$grade","school_id": "$school_id"},
                        "total_teachers": {"$sum": "$teacher_counts"},
                        "total_students": {"$sum": "$student_counts"},
                        "total_guardians": {"$sum": "$guardian_counts"},
                        "total_valid_exercise_students": {"$sum": "$valid_exercise_student_counts"},
                        "total_valid_exercise_per_month": {"$sum": "$valid_exercise_counts"},
                        "total_paid_counts": {"$sum": "$paid_counts"},
                        "total_paid_amount": {"$sum": "$paid_amount"},
                        "total_exam_images_per_month": {"$sum": "$exam_images"}
                        }
             },
            {"$project":

                {
                    "_id": 1,
                    "total_valid_exercise_students": 1,
                    "total_valid_exercise_per_month": 1,
                    "total_exam_images_per_month": 1,

                    "exercise_counts_per_person_per_month_ratio": {
                        "$cond": [{"$eq": ["$total_valid_exercise_students", 0]}, 0,
                                  {"$divide": ["$total_valid_exercise_per_month",
                                               "$total_valid_exercise_students"]}]},
                }
            }
        ]
    )

    grade_sta = []
    grade_sta_per_month = []
    async for g_s in grade_agg:
        print (g_s)
        grade_sta.append(g_s)

    async for g_s_p_m in grade_agg_month:
        grade_sta_per_month.append(g_s_p_m)

    a = []
    for g_s in grade_sta:
        for g_s_p_m in grade_sta_per_month:
            if (g_s.get("_id", {}).get("grade", "") + str(g_s.get("_id", {}).get("school_id", ""))) == \
                    (g_s_p_m.get("_id", {}).get("grade", "") + str(g_s_p_m.get("_id", {}).get("school_id", ""))):
                g_s.update(g_s_p_m)
                a.append(g_s)



    return json_response({"schools": schools, "school_sta":school_sta, "grades_sta": a})


# @assert_permissions()
async def label(req: Request):
    """
    标注学校和年级阶段
    :param req:
    :return:
    """
    print ()
    print(await get_json(req))
    return text_response('999')