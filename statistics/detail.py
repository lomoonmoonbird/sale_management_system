#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 某大区 某渠道 某市场 的数据
"""

from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import json
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL, ucAppKey, ucAppSecret, permissionAppKey
import aiohttp
import ujson
from utils import get_json, get_params, validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, \
    RequestError, ChannelNotExist, DataPermissionError
from menu.menu import Menu
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId
from enumconstant import Roles, PermissionRole
from mixins import DataExcludeMixin
from models.mysql.centauri import StageEnum
from tasks.celery_base import BaseTask
from utils import CustomEncoder

class QueryMixin(BaseHandler):

    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'
        self.school_per_day_coll = "school_per_day"

    async def _guardian_number(self, request:Request, channle_ids=[], group_by=None):
        """
        家长数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_guardian_count_list = []
        current_week_new_guardian_count_list = []
        last_week_new_guardian_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_guardian_count = coll.aggregate(
                [
                    {
                        "$match": {"channel": {"$in": channle_ids}}
                    },
                    {
                        "$project": {
                            "guardian_count": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": group_by,
                                "total": {"$sum": "$guardian_count"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                }
                     },


                ])
        current_week_new_guardian_count =  coll.aggregate(
                [
                    {
                        "$match": {
                                "day":  {"$gte": current_week[0],
                                          "$lte": current_week[6]},
                            "channel": {"$in": channle_ids}
                    }
                    },
                    {
                        "$project": {
                            "guardian_count": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": group_by,
                                "total": {"$sum": "$guardian_count"},
                                }
                     },


                ])

        last_week_new_guardian_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "guardian_count": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$guardian_count"},
                            }
                 },

            ])

        async for amount in current_week_new_guardian_count:
            current_week_new_guardian_count_list.append(amount)

        async for amount in last_week_new_guardian_count:
            last_week_new_guardian_count_list.append(amount)

        async for amount in total_guardian_count:
            total_guardian_count_list.append(amount)

        total = total_guardian_count_list[0]['total'] if total_guardian_count_list else 0
        current_week = current_week_new_guardian_count_list[0]['total'] if current_week_new_guardian_count_list else 0
        last_week = last_week_new_guardian_count_list[0]['total'] if last_week_new_guardian_count_list else 0
        return total, current_week, last_week

    async def _images_number(self, request: Request, channle_ids=[], group_by=None):
        """
        图片数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_image_count_list = []
        current_week_new_image_count_list = []
        last_week_new_image_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_image_count = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": [ "$e_image_c", "$w_image_c" ] }
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"}
                            }
                 },

            ])

        current_week_new_image_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum":   [ "$e_image_c", "$w_image_c" ] }
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"}
                            }
                 },

            ])

        last_week_new_image_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum":   [ "$e_image_c", "$w_image_c" ] }
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"}
                            }
                 },

            ])

        async for amount in current_week_new_image_count:
            current_week_new_image_count_list.append(amount)

        async for amount in last_week_new_image_count:
            last_week_new_image_count_list.append(amount)

        async for amount in total_image_count:
            total_image_count_list.append(amount)

        total = total_image_count_list[0]['total'] if total_image_count_list else 0
        current_week = current_week_new_image_count_list[0]['total'] if current_week_new_image_count_list else 0
        last_week = last_week_new_image_count_list[0]['total'] if last_week_new_image_count_list else 0
        return total, current_week, last_week

    async def _teacher_number(self, request: Request, channle_ids=[], group_by=None):
        """
        老师数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_teacher_count_list = []
        current_week_new_teacher_count_list = []
        last_week_new_teacher_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_teacher_count = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids}}
                },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])
        current_week_new_teacher_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])

        last_week_new_teacher_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "teacher_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$teacher_number"}
                            }
                 },

            ])

        async for amount in current_week_new_teacher_count:
            current_week_new_teacher_count_list.append(amount)

        async for amount in last_week_new_teacher_count:
            last_week_new_teacher_count_list.append(amount)

        async for amount in total_teacher_count:
            total_teacher_count_list.append(amount)

        total = total_teacher_count_list[0]['total'] if total_teacher_count_list else 0
        current_week = current_week_new_teacher_count_list[0]['total'] if current_week_new_teacher_count_list else 0
        last_week = last_week_new_teacher_count_list[0]['total'] if last_week_new_teacher_count_list else 0
        return total, current_week, last_week

    async def _student_number(self, request: Request, channle_ids=[], group_by=None):
        """
        学生数
        :param coll:
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_student_count_list = []
        current_week_new_student_count_list = []
        last_week_new_student_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_student_count = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids}}
                },
                {
                    "$project": {
                        "student_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$student_number"}
                            }
                 },

            ])
        current_week_new_student_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "student_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$student_number"}
                            }
                 },

            ])

        last_week_new_student_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "student_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$student_number"}
                            }
                 },

            ])

        async for amount in current_week_new_student_count:
            current_week_new_student_count_list.append(amount)

        async for amount in last_week_new_student_count:
            last_week_new_student_count_list.append(amount)

        async for amount in total_student_count:
            total_student_count_list.append(amount)

        total = total_student_count_list[0]['total'] if total_student_count_list else 0
        current_week = current_week_new_student_count_list[0]['total'] if current_week_new_student_count_list else 0
        last_week = last_week_new_student_count_list[0]['total'] if last_week_new_student_count_list else 0
        return total, current_week, last_week

    async def _school_number(self, request: Request, channle_ids=[], group_by=None):
        """
        学校数
        :param coll:
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_school_count_list = []
        current_week_new_school_count_list = []
        last_week_new_school_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_school_count = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids}}
                },
                {
                    "$project": {
                        "school_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$school_number"}
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])
        current_week_new_school_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "school_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$school_number"}
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        last_week_new_school_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "school_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$school_number"}
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        async for amount in current_week_new_school_count:
            current_week_new_school_count_list.append(amount)

        async for amount in last_week_new_school_count:
            last_week_new_school_count_list.append(amount)

        async for amount in total_school_count:
            total_school_count_list.append(amount)

        total = total_school_count_list[0]['total'] if total_school_count_list else 0
        current_week = current_week_new_school_count_list[0]['total'] if current_week_new_school_count_list else 0
        last_week = last_week_new_school_count_list[0]['total'] if last_week_new_school_count_list else 0
        return total, current_week, last_week

    async def _pay_number(self, request: Request, channle_ids=[], group_by=None):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_pay_count_list = []
        current_week_new_pay_count_list = []
        last_week_new_pay_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        current_week_new_pay_number =  coll.aggregate(
                [
                    {
                        "$match": {
                                "day":  {"$gte": current_week[0],
                                          "$lte": current_week[6]},
                            "channel": {"$in": channle_ids}
                    }
                    },
                    {
                        "$project": {
                            "pay_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": group_by,
                                "total": {"$sum": "$pay_number"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                }
                     },


                ])

        last_week_new_pay_number = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "pay_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$pay_number"},
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        total_pay_number = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids},
                               "day": {"$gte": request['data_permission']['pay_stat_start_time']}}
                },
                {
                    "$project": {
                        "pay_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$pay_number"},
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        async for amount in current_week_new_pay_number:
            current_week_new_pay_count_list.append(amount)

        async for amount in last_week_new_pay_number:
            last_week_new_pay_count_list.append(amount)

        async for amount in total_pay_number:
            total_pay_count_list.append(amount)

        total = total_pay_count_list[0]['total'] if total_pay_count_list else 0
        current_week = current_week_new_pay_count_list[0]['total'] if current_week_new_pay_count_list else 0
        last_week = last_week_new_pay_count_list[0]['total'] if last_week_new_pay_count_list else 0
        return total,current_week,last_week

    async def _pay_amount(self, request: Request, channle_ids=[], group_by=None):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_pay_amount_list = []
        current_week_new_pay_amount_list = []
        last_week_new_pay_amount_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        current_week_new_pay_amount =  coll.aggregate(
                [
                    {
                        "$match": {
                                "day":  {"$gte": current_week[0],
                                          "$lte": current_week[6]},
                            "channel": {"$in": channle_ids}
                    }
                    },
                    {
                        "$project": {
                            "pay_amount": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": group_by,
                                "total": {"$sum": "$pay_amount"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                }
                     },


                ])

        last_week_new_pay_amount = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "pay_amount": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$pay_amount"},
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        total_pay_amount = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids},
                               "day": {"$gte": request['data_permission']['pay_stat_start_time']}}
                },
                {
                    "$project": {
                        "pay_amount": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$pay_amount"},
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                            }
                 },

            ])

        async for amount in current_week_new_pay_amount:
            current_week_new_pay_amount_list.append(amount)

        async for amount in last_week_new_pay_amount:
            last_week_new_pay_amount_list.append(amount)

        async for amount in total_pay_amount:
            total_pay_amount_list.append(amount)

        total = total_pay_amount_list[0]['total'] if total_pay_amount_list else 0
        current_week = current_week_new_pay_amount_list[0]['total'] if current_week_new_pay_amount_list else 0
        last_week = last_week_new_pay_amount_list[0]['total'] if last_week_new_pay_amount_list else 0
        return total,current_week,last_week

    async def _valid_contest_number(self, request:Request, channle_ids=[], group_by=None):
        """
        有效测评书
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_guardian_count_list = []
        current_week_new_guardian_count_list = []
        last_week_new_guardian_count_list = []
        current_week = self.current_week()
        last_week = self.last_week()
        total_guardian_count = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channle_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"},
                            }
                 },

            ])
        current_week_new_guardian_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"},
                            }
                 },

            ])

        last_week_new_guardian_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channle_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
                    }
                },

                {"$group": {"_id": group_by,
                            "total": {"$sum": "$total"},

                            }
                 },

            ])

        async for amount in current_week_new_guardian_count:
            current_week_new_guardian_count_list.append(amount)

        async for amount in last_week_new_guardian_count:
            last_week_new_guardian_count_list.append(amount)

        async for amount in total_guardian_count:
            total_guardian_count_list.append(amount)

        total = total_guardian_count_list[0]['total'] if total_guardian_count_list else 0
        current_week = current_week_new_guardian_count_list[0]['total'] if current_week_new_guardian_count_list else 0
        last_week = last_week_new_guardian_count_list[0]['total'] if last_week_new_guardian_count_list else 0
        return total, current_week, last_week

    async def _list(self, request: Request, channel_ids: list):
        """
        渠道维度统计
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$channel",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            "total_exercise_image_number": {"$sum": "$e_image_c"},
                            "total_word_image_number": {"$sum": "$w_image_c"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_school_number": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_market(self, request: Request, school_ids: list):
        """
        渠道详情
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.school_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_ids}
                    }
                },
                {
                    "$project": {
                        "school_id":1,
                        "channel": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            "total_exercise_image_number": {"$sum": "$e_image_c"},
                            "total_word_image_number": {"$sum": "$w_image_c"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_grade(self, request: Request, school_id: int, grade:str):
        """
        年级详情
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        items = []

        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": school_id,
                        "grade": grade
                    }
                },
                {
                    "$project": {
                        "school_id":1,
                        "group_id": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "day": 1
                    }
                },

                {"$group": {"_id": "$group_id",
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},

                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "group_id": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,

                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_school_clazz(self, request: Request, school_id: int):
        """
        学校数
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        items = []
        print('school id', school_id)
        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": school_id,
                    }
                },
                {
                    "$project": {
                        "school_id":1,
                        "group_id": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "day": 1
                    }
                },

                {"$group": {"_id": "$group_id",
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},

                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "group_id": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,

                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_school(self, request: Request, school_ids: list, exclude_channels=[]):
        """
        渠道维度统计
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.school_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_ids}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
                        "grade": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            "total_exercise_image_number": {"$sum": "$e_image_c"},
                            "total_word_image_number": {"$sum": "$w_image_c"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_school_number": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_school_grade(self, request: Request, school_ids: list, exclude_channels=[]):
        """
        学校年级维度
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.grade_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": {"$in": school_ids}
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
                        "grade": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "guardian_unique_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "total_pay_number": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_number", 0]},
                        "total_pay_amount": {
                            "$cond": [{"$and": [{"$gte": ["$day", request['data_permission']['pay_stat_start_time']]}]},
                                      "$pay_amount", 0]},
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": {"school_id": "$school_id", "grade": "$grade"},
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_guardian_unique_count": {"$sum": "$guardian_unique_count"},
                            "total_pay_number": {"$sum": "$total_pay_number"},
                            "total_pay_amount": {"$sum": "$total_pay_amount"},
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            "total_exercise_image_number": {"$sum": "$e_image_c"},
                            "total_word_image_number": {"$sum": "$w_image_c"}
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_school_number": 1,
                        "total_teacher_number": 1,
                        "total_student_number": 1,
                        "total_guardian_number": 1,
                        "total_guardian_unique_count": 1,
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_unique_count", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _wap_list_grade_clazz(self, request: Request, school_id: int, grade: str):
        """
        渠道维度统计
        :param request:
        :param channel_ids:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        items = []
        yesterday = datetime.now() - timedelta(1)
        yesterday_before_30day = yesterday - timedelta(30)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_before_30day_str = yesterday_before_30day.strftime("%Y-%m-%d")


        item_count = coll.aggregate(
            [
                {
                    "$match": {
                        "school_id": school_id,
                        "grade": grade
                    }
                },
                {
                    "$project": {
                        "channel": 1,
                        "school_id": 1,
                        "grade": 1,
                        "group_id": 1,
                        "valid_reading_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_reading_count", 0]},
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lte": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$group_id",
                            "total_valid_reading_number": {"$sum": "$valid_reading_count"},
                            "total_valid_exercise_number": {"$sum": "$valid_exercise_count"},
                            "total_valid_word_number": {"$sum": "$valid_word_count"},
                            }
                 },
                {
                    "$project": {
                        "_id": 1,
                        "total_valid_reading_number": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

class AreaDetail(QueryMixin, DataExcludeMixin):
    """
    大区详情
    """
    def __init__(self):
        super(AreaDetail, self).__init__()
        self.grade_coll = "grade"
        self.school_coll = "school"
        self.start_time = BaseTask().start_time

    @validate_permission(data_validation=True)
    async def overview(self, request: Request):
        """
        大区详情总览
        {
            "area_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        area_id = request_param.get("area_id", "")
        if not area_id:
            return self.reply_ok([])

        include_area = request['data_permission']['include_area']
        if include_area and area_id not in include_area:
            raise DataPermissionError("you have no right to access data")

        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1})
        channels = await channels.to_list(10000)

        old_ids = [item['old_id'] for item in channels]
        exclude_channels = await self.exclude_channel(request.app['mysql'])
        exclude_channels += request['data_permission']['exclude_channel']
        old_ids = list(set(old_ids).difference(set(exclude_channels)))
        pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number(request, old_ids, "$channel")

        pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount(request, old_ids, "$channel")

        total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number(
            request, old_ids, "$channel")

        teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number(
            request, old_ids, "$channel")
        student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number(
            request, old_ids, "$channel")

        image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number(request,
                                                                                                             old_ids, "$channel")

        guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number(
            request, old_ids, "$channel")

        contest_total, contest_curr_week_new_number, contest_last_week_new_number = await self._valid_contest_number(
            request, old_ids, "$channel")
        return self.reply_ok({"pay_total": pay_total,
                              "pay_curr_week_new_number": pay_curr_week_new_number,
                              "pay_last_week_new_number": pay_last_week_new_number,
                              "pay_amount": pay_amount,
                              "pay_curr_week_new_amount": pay_curr_week_new_amount,
                              "pay_last_week_new_amount": pay_last_week_new_amount,
                              "total_school_number": total_school_number,
                              "curr_week_new_school_number": curr_week_new_school_number,
                              "last_week_new_school_number": last_week_new_school_number,
                              "total_teacher_number": teacher_total,
                              "teacher_curr_week_new_number": teacher_curr_week_new_number,
                              "teacher_last_week_new_number": teacher_last_week_new_number,
                              "student_total": student_total,
                              "student_curr_week_new_number": student_curr_week_new_number,
                              "student_last_week_new_number": student_last_week_new_number,
                              "image_total": image_total,
                              "image_curr_week_new_number": image_curr_week_new_number,
                              "image_last_week_new_number": image_last_week_new_number,
                              "guardian_total": guardian_total,
                              "guardian_curr_week_new_number": guardian_curr_week_new_number,
                              "guardian_last_week_new_number": guardian_last_week_new_number,
                              "contest_total": contest_total,
                              "contest_curr_week_new_number": contest_curr_week_new_number,
                              "contest_last_week_new_number": contest_last_week_new_number
                              })

    @validate_permission(data_validation=True)
    async def channel_list(self, request: Request):
        """
        渠道列表
        {
            "area_id": ""
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        area_id = request_param.get("area_id", "")
        page = int(request_param.get("page", 1)) - 1
        per_page = 10
        total_count = 0
        if not area_id:
            return self.reply_ok([])

        include_area = request['data_permission']['include_area']
        if include_area and area_id not in include_area:
            raise DataPermissionError("you have no right to access data")

        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1}).skip(page*per_page).limit(per_page)
        channels = await channels.to_list(10000)
        total_count = await request.app['mongodb'][self.db][self.instance_coll].count_documents({"parent_id": area_id,
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1})
        old_ids = [item['old_id'] for item in channels]


        items = []
        if old_ids:
            sql = "select id, name from sigma_account_us_user where available = 1 and id in (%s) " % \
                  ','.join([str(id) for id in old_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    real_channels = await cur.fetchall()
            exclude_channels = await self.exclude_channel(request.app['mysql'])
            exclude_channels += request['data_permission']['exclude_channel']
            old_ids = list(set(old_ids).difference(set(exclude_channels)))
            items = await self._list(request, old_ids)
            channel_id_map = {}
            for channel in real_channels:
                channel_id_map[channel['id']] = channel

            for item in items:
                item['contest_coverage_ratio'] = 0
                item['contest_average_per_person'] = 0
                item['channel_info'] = channel_id_map.get(item['_id'], {})

        return self.reply_ok({"channel_list": items, "extra": {"total": total_count, "number_per_page": per_page, "curr_page": page + 1}})

    @validate_permission()
    async def school_list(self, request: Request):
        """
        大区学校列表

        学校列表
        {
            "page":"",
            "school_name":"",
            "stage": "",
            "open_time_range":
        }
        :
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 1)) - 1
        area_id = request['user_info']['area_id']
        per_page = 10

        school_page_sql = ''
        total_sql = ''
        total_school_count = 0
        flag = 0
        channels_of_area = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                                     "role": Roles.CHANNEL.value,
                                                                                     "status": 1})
        channels_of_area = await channels_of_area.to_list(None)
        channels_of_area_old_ids = [item['old_id'] for item in channels_of_area]
        request_stage = int(request_param.get('stage')) if request_param.get('stage') else -1
        channel_str = ','.join(['"' + str(id) + '"' for id in channels_of_area_old_ids]) if channels_of_area_old_ids else "''"
        if not request_param.get('school_name') and request_stage not in [StageEnum.Register.value,
                                                                          StageEnum.Using.value,
                                                                          StageEnum.Binding.value,
                                                                          StageEnum.Pay.value] \
                and not request_param.get('open_time_range'):  # 全部
            flag = 1
            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 and owner_id in (%s) and time_create >= '%s' " \
                              "and time_create <= '%s' limit %s,%s" % (channel_str,
                                                                       self.start_time.strftime("%Y-%m-%d"),
                                                                       datetime.now().strftime("%Y-%m-%d"),
                                                                       per_page * page, per_page)
            total_sql = "select count(id) as total_school_count " \
                        "from sigma_account_ob_school" \
                        " where available = 1 and owner_id in (%s) and time_create >= '%s' " \
                        "and time_create <= '%s' " % (channel_str,
                                                      self.start_time.strftime("%Y-%m-%d"),
                                                      datetime.now().strftime("%Y-%m-%d"))
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    if total_sql:
                        await cur.execute(total_sql)
                        total_school = await cur.fetchall()
                        total_school_count = total_school[0]['total_school_count']

        elif request_param.get('school_name'):  # 单个学校
            total_school_count = 1
            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 " \
                              "and owner_id in (%s) " \
                              "and full_name like %s" % (channel_str,
                                                         "'%" + request_param['school_name'] + "%'")
            flag = 2
        elif not request_param.get('school_name'):
            flag = 3
            stage = [StageEnum.Register.value, StageEnum.Using.value, StageEnum.Binding.value, StageEnum.Pay.value]
            request_stage = request_param.get('stage', -1)
            query = {

            }
            if not request_stage:
                request_stage = -1
            if request_stage != -1 and int(request_stage) not in stage:
                query["stage"] = {"$in": stage}
            elif request_stage != -1 and int(request_stage) in stage:
                query["stage"] = int(request_stage)
            else:
                pass
            date_range = request_param.get('open_time_range', '')

            if date_range:
                date_range = request_param.get('open_time_range').split(',')
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"),
                                            "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            else:
                date_range = [self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")]
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"),
                                            "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            query.update({"channel": {"$in": channels_of_area_old_ids}})
            condition_schools = request.app['mongodb'][self.db][self.school_coll].find(query)
            condition_schools = await condition_schools.to_list(10000)
            total_school_count = await request.app['mongodb'][self.db][self.school_coll].count_documents(query)
            if not condition_schools:
                return self.reply_ok({})
            condition_school_ids = [item['school_id'] for item in condition_schools]

            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 " \
                              "and owner_id in (%s)" \
                              " and id in (%s) limit %s,%s" % (channel_str,
                                                               ','.join(['"' + str(id) + '"' for id in
                                                                        condition_school_ids]),
                                                                        per_page * page,
                                                                        per_page)
        else:
            pass

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(school_page_sql)
                schools = await cur.fetchall()
        school_ids = [item['id'] for item in schools]
        grades = []
        if school_ids:
            grade_sql = "select grade, school_id, time_create " \
                        "from sigma_account_ob_group " \
                        "where available = 1 " \
                        "and school_id in (%s) " \
                        "group by school_id, grade" % ",".join([str(id) for id in school_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(grade_sql)
                    grades = await cur.fetchall()

        stage_grade = request.app['mongodb'][self.db][self.grade_coll].find({"school_id": {"$in": school_ids}})
        stage_grade = await stage_grade.to_list(10000)
        stage_grade_union_map = {}
        for s_g in stage_grade:
            stage_grade_union_map[str(s_g['school_id']) + "@" + s_g['grade']] = s_g

        stage_grade_union_map2 = {}
        school_grade_union_defaultdict = defaultdict(list)
        for grade in grades:
            union_id = str(grade['school_id']) + "@" + grade['grade']
            default = {
                "stage": StageEnum.Register.value,
                "using_time": 0
            }
            grade['school_grade'] = union_id
            grade.update(stage_grade_union_map.get(union_id, default))
            stage_grade_union_map2[union_id] = grade
            school_grade_union_defaultdict[grade['school_id']].append(union_id)

        school_item = await self._list_school(request, school_ids)
        school_item_map = {}
        for s_i in school_item:
            s_i["contest_coverage_ratio"] = 0
            s_i["contest_average_per_person"] = 0
            school_item_map[s_i['_id']] = s_i
        grade_item = await self._list_school_grade(request, school_ids)
        grade_item_map = {}
        for g_i in grade_item:
            g_i["contest_coverage_ratio"] = 0
            g_i["contest_average_per_person"] = 0
            grade_item_map[str(g_i['_id']['school_id']) + "@" + g_i['_id']['grade']] = g_i

        data = []
        for index, school in enumerate(schools):

            default = {
                "contest_coverage_ratio": 0,
                "contest_average_per_person": 0,
                "total_teacher_number": 0,
                "total_student_number": 0,
                "total_guardian_number": 0,
                "total_pay_number": 0,
                "total_pay_amount": 0,
                "total_valid_reading_number": 0,
                "total_valid_exercise_number": 0,
                "total_valid_word_number": 0,
                "total_exercise_image_number": 0,
                "total_word_image_number": 0,
                "pay_ratio": 0.0,
                "bind_ratio": 0.0
            }
            school['stat_info'] = school_item_map.get(school['id'], default)
            school['grade_info'] = []
            stage = []
            for school_grade in school_grade_union_defaultdict.get(school['id'], []):
                g_info = grade_item_map.get(school_grade, {})
                if g_info:
                    g_info.update(stage_grade_union_map2.get(school_grade, {}))
                    school['grade_info'].append(g_info)
                    stage.append(stage_grade_union_map2.get(school_grade, {}).get("stage", StageEnum.Register.value))
            school['stage'] = StageEnum.Register.value if not stage else min(stage)

            data.append(school)
        return self.reply_ok({"school_list": data,
                              "extra": {"total": total_school_count,
                                        "number_per_page": per_page,
                                        "curr_page": page + 1}})

class ChannelDetail(QueryMixin):
    """
    渠道详情
    """
    def __init__(self):
        super(ChannelDetail, self).__init__()
        self.grade_coll = "grade"
        self.school_coll = "school"
        self.start_time = BaseTask().start_time

    @validate_permission(data_validation=True)
    async def overview(self, request: Request):
        """
        渠道详情总览
        {
            "channel_old_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_old_id = int(request_param.get("channel_old_id", ""))
        if not channel_old_id:
            return self.reply_ok({"pay_total": 0,
                                  "pay_curr_week_new_number": 0,
                                  "pay_last_week_new_number": 0,
                                  "pay_amount": 0,
                                  "pay_curr_week_new_amount": 0,
                                  "pay_last_week_new_amount": 0,
                                  "total_school_number": 0,
                                  "curr_week_new_school_number": 0,
                                  "last_week_new_school_number": 0,
                                  "total_teacher_number": 0,
                                  "teacher_curr_week_new_number": 0,
                                  "teacher_last_week_new_number": 0,
                                  "student_total": 0,
                                  "student_curr_week_new_number": 0,
                                  "student_last_week_new_number": 0,
                                  "image_total": 0,
                                  "image_curr_week_new_number": 0,
                                  "image_last_week_new_number": 0,
                                  "guardian_total": 0,
                                  "guardian_curr_week_new_number": 0,
                                  "guardian_last_week_new_number": 0,
                                  "contest_total": 0,
                                  "contest_curr_week_new_number": 0,
                                  "contest_last_week_new_number": 0
                                  })
        include_channel = request['data_permission']['include_channel']
        if include_channel and channel_old_id not in include_channel:
            raise DataPermissionError("you have no right to access data")
        if request['user_info']['instance_role_id'] == Roles.AREA.value:
            exist = await request.app['mongodb'][self.db][self.instance_coll]\
                .find_one({"parent_id": request['user_info']['area_id'],
                           'old_id': channel_old_id,
                           'status': 1})
            if not exist:
                raise DataPermissionError("you have no right to access data")
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            exist = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({
                           'old_id': channel_old_id,
                           'status': 1})
            if not exist:
                raise DataPermissionError("you have no right to access data")

        # channel = await request.app['mongodb'][self.db][self.instance_coll].find_one({"old_id": channel_old_id, "status": 1})
        # if channel:
        channel_old_id = [channel_old_id]
        channel_old_id = list(set(channel_old_id).difference(set(request['data_permission']['exclude_channel'])))

        print('channel_old_id', channel_old_id)
        pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number(request, channel_old_id,
                                                                                               "$channel")

        pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount(request, channel_old_id,
                                                                                                "$channel")

        total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number(
            request, channel_old_id, "$channel")

        teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number(
            request, channel_old_id, "$channel")
        student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number(
            request, channel_old_id, "$channel")

        image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number(request,
                                                                                                        channel_old_id,
                                                                                                        "$channel")

        guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number(
            request, channel_old_id, "$channel")

        contest_total, contest_curr_week_new_number, contest_last_week_new_number = await self._valid_contest_number(
            request, channel_old_id, "$channel")

        return self.reply_ok({"pay_total": pay_total,
                              "pay_curr_week_new_number": pay_curr_week_new_number,
                              "pay_last_week_new_number": pay_last_week_new_number,
                              "pay_amount": pay_amount,
                              "pay_curr_week_new_amount": pay_curr_week_new_amount,
                              "pay_last_week_new_amount": pay_last_week_new_amount,
                              "total_school_number": total_school_number,
                              "curr_week_new_school_number": curr_week_new_school_number,
                              "last_week_new_school_number": last_week_new_school_number,
                              "total_teacher_number": teacher_total,
                              "teacher_curr_week_new_number": teacher_curr_week_new_number,
                              "teacher_last_week_new_number": teacher_last_week_new_number,
                              "student_total": student_total,
                              "student_curr_week_new_number": student_curr_week_new_number,
                              "student_last_week_new_number": student_last_week_new_number,
                              "image_total": image_total,
                              "image_curr_week_new_number": image_curr_week_new_number,
                              "image_last_week_new_number": image_last_week_new_number,
                              "guardian_total": guardian_total,
                              "guardian_curr_week_new_number": guardian_curr_week_new_number,
                              "guardian_last_week_new_number": guardian_last_week_new_number,
                              "contest_total": contest_total,
                              "contest_curr_week_new_number": contest_curr_week_new_number,
                              "contest_last_week_new_number": contest_last_week_new_number
                              })
        # return self.reply_ok({"pay_total": 0,
        #                           "pay_curr_week_new_number": 0,
        #                           "pay_last_week_new_number": 0,
        #                           "pay_amount": 0,
        #                           "pay_curr_week_new_amount": 0,
        #                           "pay_last_week_new_amount": 0,
        #                           "total_school_number": 0,
        #                           "curr_week_new_school_number": 0,
        #                           "last_week_new_school_number": 0,
        #                           "total_teacher_number": 0,
        #                           "teacher_curr_week_new_number": 0,
        #                           "teacher_last_week_new_number": 0,
        #                           "student_total": 0,
        #                           "student_curr_week_new_number": 0,
        #                           "student_last_week_new_number": 0,
        #                           "image_total": 0,
        #                           "image_curr_week_new_number": 0,
        #                           "image_last_week_new_number": 0,
        #                           "guardian_total": 0,
        #                           "guardian_curr_week_new_number": 0,
        #                           "guardian_last_week_new_number": 0,
        #                           "contest_total": 0,
        #                           "contest_curr_week_new_number": 0,
        #                           "contest_last_week_new_number": 0
        #                           })

    @validate_permission(data_validation=True)
    async def market_list(self, request: Request):
        """
        市场列表
        {
            "channel_id": ""
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_id = request_param.get("channel_id", "")
        page = int(request_param.get("page", 1)) - 1

        per_page = 10
        total_count = 0
        if not channel_id:
            return self.reply_ok({"market_list": [], "extra": {"total": 0,"number_per_page": per_page,"curr_page": page + 1}})

        valid_channel = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id),
                                                                                            'status': 1})
        channel_id = valid_channel.get('old_id', '')
        include_channel = request['data_permission']['include_channel']
        if include_channel and channel_id not in include_channel:
            raise DataPermissionError("you have no right to access data")

        if request['user_info']['instance_role_id'] == Roles.AREA.value:
            exist = await request.app['mongodb'][self.db][self.instance_coll]\
                .find_one({"parent_id": request['user_info']['area_id'],
                           'role': Roles.CHANNEL.value,
                           'old_id': channel_id,
                           'status': 1})
            if not exist:
                raise DataPermissionError("you have no right to access data")



        schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id,
                                                                            "role": Roles.SCHOOL.value,
                                                                            "status": 1})
        schools = await schools.to_list(100000)
        schools_ids = list(set([item['school_id'] for item in schools]))
        market_users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": channel_id,
                                                                             "instance_role_id": Roles.MARKET.value,
                                                                             "status": 1}).skip(per_page*page).limit(per_page)
        market_users = await market_users.to_list(10000)

        total_count = await request.app['mongodb'][self.db][self.user_coll].count_documents({"channel_id": channel_id,
                                                                             "instance_role_id": Roles.MARKET.value,
                                                                            "status": 1})

        market_users_map = {}
        for market in market_users:
            market_users_map[market['user_id']] = market

        school_market_map = {}
        market_school_map = {}
        for school in schools:
            school_market_map[school['school_id']] = market_users_map.get(str(school['user_id']), {})
            market_school_map[school['user_id']] = school
        items = await self._list_market(request, schools_ids)


        # for user_id, user_data in market_users.items():
        #     pass
        from collections import defaultdict
        channel_campact_data = defaultdict(dict)
        # print(len(items),len(school_market_map),"school_market_map", school_market_map)
        for item in items:
            item['contest_coverage_ratio'] = 0
            item['contest_average_per_person'] = 0
            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_school_number', []).append(1)

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_teacher_number', []).append(item['total_teacher_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_student_number', []).append(item['total_student_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_guardian_number', []).append(item['total_guardian_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_pay_number', []).append(item['total_pay_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_pay_amount', []).append(item['total_pay_amount'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_valid_reading_number', []).append(item['total_valid_reading_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_valid_exercise_number', []).append(item['total_valid_exercise_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_valid_word_number', []).append(item['total_valid_word_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_exercise_image_number', []).append(item['total_exercise_image_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_word_image_number', []).append(item['total_word_image_number'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'pay_ratio', []).append(item['pay_ratio'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'bind_ratio', []).append(item['bind_ratio'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'contest_coverage_ratio', []).append(item['contest_coverage_ratio'])

            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'contest_average_per_person', []).append(item['contest_average_per_person'])





            item["market_info"] = market_users_map.get(str(school_market_map.get(item['_id'], {}).get("user_id", "")), {})

        items = []
        for user_id, item in channel_campact_data.items():
            items.append(
                {
                    "total_school_number": sum(item['total_school_number']),
                    "total_teacher_number": sum(item['total_teacher_number']),
                    "total_student_number": sum(item['total_student_number']),
                    "total_guardian_number": sum(item['total_guardian_number']),
                    "total_pay_number": sum(item['total_pay_number']),
                    "total_pay_amount": sum(item['total_pay_amount']),
                    "total_valid_reading_number": sum(item['total_valid_reading_number']),
                    "total_valid_exercise_number": sum(item['total_valid_exercise_number']),
                    "total_valid_word_number": sum(item['total_valid_word_number']),
                    "total_exercise_image_number": sum(item['total_exercise_image_number']),
                    "total_word_image_number": sum(item['total_word_image_number']),
                    "pay_ratio": sum(item['pay_ratio']),
                    "bind_ratio": sum(item['bind_ratio']),
                    "contest_coverage_ratio": sum(item['contest_coverage_ratio']),
                    "contest_average_per_person": sum(item['contest_average_per_person']),
                    "market_info": market_users_map.get(str(user_id), {})
                }
            )



        return self.reply_ok({"market_list": items, "extra": {"total": total_count, "number_per_page": per_page, "curr_page": page + 1}})


    @validate_permission(data_validation=True)
    async def school_list_detail(self, request: Request):
        """
        渠道详情的学校列表
        {
            "page": "",
            "channel_old_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 1)) - 1
        per_page = 10
        total_school_count = 0
        channel_old_id = int(request_param.get("channel_old_id", ""))

        include_channel = request['data_permission']['include_channel']
        if include_channel and channel_old_id not in include_channel:
            raise DataPermissionError("you have no right to access data")
        if request['user_info']['instance_role_id'] == Roles.AREA.value:
            exist = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({"parent_id": request['user_info']['area_id'],
                           'old_id': channel_old_id,
                           'status': 1})
            if not exist:
                raise DataPermissionError("you have no right to access data")
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            exist = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({
                'old_id': channel_old_id,
                'status': 1})
            if not exist:
                raise DataPermissionError("you have no right to access data")


        total_sql = "select count(id) as total_school_count from sigma_account_ob_school " \
                    "where owner_id = %s " \
                    "and time_create >= '%s' " \
                    "and time_create <= '%s' " % (channel_old_id,
                                               self.start_time.strftime("%Y-%m-%d"),
                                               datetime.now().strftime("%Y-%m-%d"))

        school_sql = "select id,full_name from sigma_account_ob_school " \
                    "where owner_id = %s " \
                     "and available = 1 " \
                    "and time_create >= '%s' " \
                    "and time_create <= '%s' limit %s,%s" % (channel_old_id,
                                                  self.start_time.strftime("%Y-%m-%d"),
                                                  datetime.now().strftime("%Y-%m-%d"), page*per_page, per_page)

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(total_sql)
                total_school = await cur.fetchall()
                total_school_count = total_school[0]['total_school_count']
                await cur.execute(school_sql)
                schools = await cur.fetchall()
        school_map = {}
        for school in schools:
            school_map[school['id']] = school
        school_ids = [item['id'] for item in schools]

        items = await self._list_school(request, school_ids)
        item_map = {}
        for item in items:
            item_map[item['_id']] = item

        default = {
            "total_school_number": 0,
            "total_teacher_number": 0,
            "total_student_number": 0,
            "total_guardian_number": 0,
            "total_guardian_unique_count": 0,
            "total_pay_number": 0,
            "total_pay_amount": 0,
            "total_valid_reading_number": 0,
            "total_valid_exercise_number": 0,
            "total_valid_word_number": 0,
            "total_exercise_image_number": 0,
            "total_word_image_number": 0,
            "contest_coverage_ratio": 0,
            "contest_average_per_person":0,
            "pay_ratio": 0.0,
            "bind_ratio": 0.0,
        }
        for school in schools:
            school['stat_info'] = item_map.get(school['id'], default)
            school['stat_info'].update({"contest_coverage_ratio": 0})
            school['stat_info'].update({"contest_average_per_person": 0})
        return self.reply_ok({"school_list": schools, "extra": {"total": total_school_count, "number_per_page": per_page, "curr_page": page + 1}})


    @validate_permission()
    async def school_list(self, request: Request):
        """

        学校列表
        {
            "page":"",
            "school_name":"",
            "stage": "",
            "open_time_range":
        }
        :
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 1)) - 1
        channel_id = request['user_info']['channel_id']
        per_page = 10

        school_page_sql = ''
        total_sql = ''
        total_school_count = 0
        flag = 0
        channel_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id),
                                                                                     "role": Roles.CHANNEL.value,
                                                                                     "status": 1})
        old_id = channel_info.get("old_id") if channel_info else -1
        request_stage = int(request_param.get('stage')) if request_param.get('stage') else -1

        if not request_param.get('school_name') and request_stage not in [StageEnum.Register.value,
                                                                          StageEnum.Using.value,
                                                                          StageEnum.Binding.value,
                                                                          StageEnum.Pay.value] \
                and not request_param.get('open_time_range'):  # 全部
            flag = 1
            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 and owner_id = %s and time_create >= '%s' " \
                              "and time_create <= '%s' limit %s,%s" % (old_id,
                                                                       self.start_time.strftime("%Y-%m-%d"),
                                                                       datetime.now().strftime("%Y-%m-%d"),
                                                                       per_page * page, per_page)
            total_sql = "select count(id) as total_school_count " \
                        "from sigma_account_ob_school" \
                        " where available = 1 and owner_id = %s and time_create >= '%s' " \
                        "and time_create <= '%s' " % (old_id,
                                                      self.start_time.strftime("%Y-%m-%d"),
                                                      datetime.now().strftime("%Y-%m-%d"))
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    if total_sql:
                        await cur.execute(total_sql)
                        total_school = await cur.fetchall()
                        total_school_count = total_school[0]['total_school_count']

        elif request_param.get('school_name'):  # 单个学校
            total_school_count = 1
            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 " \
                              "and owner_id = %s " \
                              "and full_name like %s" % (old_id,
                                                         "'%" + request_param['school_name'] + "%'")
            flag = 2
        elif not request_param.get('school_name'):
            flag = 3
            stage = [StageEnum.Register.value, StageEnum.Using.value, StageEnum.Binding.value, StageEnum.Pay.value]
            request_stage = request_param.get('stage', -1)
            query = {

            }
            if not request_stage:
                request_stage = -1
            if request_stage != -1 and int(request_stage) not in stage:
                query["stage"] = {"$in": stage}
            elif request_stage != -1 and int(request_stage) in stage:
                query["stage"] = int(request_stage)
            else:
                pass
            date_range = request_param.get('open_time_range', '')

            if date_range:
                date_range = request_param.get('open_time_range').split(',')
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"),
                                            "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            else:
                date_range = [self.start_time.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")]
                query.update({"open_time": {"$gte": datetime.strptime(date_range[0], "%Y-%m-%d"),
                                            "$lte": datetime.strptime(date_range[1], "%Y-%m-%d")}})
            query.update({"channel": old_id})
            condition_schools = request.app['mongodb'][self.db][self.school_coll].find(query)
            condition_schools = await condition_schools.to_list(10000)
            total_school_count = await request.app['mongodb'][self.db][self.school_coll].count_documents(query)
            if not condition_schools:
                return self.reply_ok({})
            condition_school_ids = [item['school_id'] for item in condition_schools]


            school_page_sql = "select id,full_name, time_create  " \
                              "from sigma_account_ob_school" \
                              " where available = 1 " \
                              "and owner_id = %s " \
                              "and id in (%s) limit %s,%s" % (old_id,
                                                              ','.join(['"' + str(id) + '"' for id in
                                                                        condition_school_ids]),
                                                                  per_page * page, per_page)

        else:
            pass
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(school_page_sql)
                schools = await cur.fetchall()
        school_ids = [item['id'] for item in schools]
        grades = []
        if school_ids:
            grade_sql = "select grade, school_id, time_create " \
                        "from sigma_account_ob_group " \
                        "where available = 1 " \
                        "and school_id in (%s) " \
                        "group by school_id, grade" % ",".join([str(id) for id in school_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(grade_sql)
                    grades = await cur.fetchall()

        stage_grade = request.app['mongodb'][self.db][self.grade_coll].find({"school_id": {"$in": school_ids}})
        stage_grade = await stage_grade.to_list(10000)
        stage_grade_union_map = {}
        for s_g in stage_grade:
            stage_grade_union_map[str(s_g['school_id']) + "@" + s_g['grade']] = s_g

        stage_grade_union_map2 = {}
        school_grade_union_defaultdict = defaultdict(list)
        for grade in grades:
            union_id = str(grade['school_id']) + "@" + grade['grade']
            default = {
                "stage": StageEnum.Register.value,
                "using_time": 0
            }
            grade['school_grade'] = union_id
            grade.update(stage_grade_union_map.get(union_id, default))
            stage_grade_union_map2[union_id] = grade
            school_grade_union_defaultdict[grade['school_id']].append(union_id)

        school_item = await self._list_school(request, school_ids)
        school_item_map = {}
        for s_i in school_item:
            s_i["contest_coverage_ratio"] = 0
            s_i["contest_average_per_person"] = 0
            school_item_map[s_i['_id']] = s_i
        grade_item = await self._list_school_grade(request, school_ids)
        grade_item_map = {}
        for g_i in grade_item:
            g_i["contest_coverage_ratio"] = 0
            g_i["contest_average_per_person"] = 0
            grade_item_map[str(g_i['_id']['school_id']) + "@" + g_i['_id']['grade']] = g_i

        data = []
        for index, school in enumerate(schools):

            default = {
                "contest_coverage_ratio": 0,
                "contest_average_per_person": 0,
                "total_teacher_number": 0,
                "total_student_number": 0,
                "total_guardian_number": 0,
                "total_pay_number": 0,
                "total_pay_amount": 0,
                "total_valid_reading_number": 0,
                "total_valid_exercise_number": 0,
                "total_valid_word_number": 0,
                "total_exercise_image_number": 0,
                "total_word_image_number": 0,
                "pay_ratio": 0.0,
                "bind_ratio": 0.0
            }
            school['stat_info'] = school_item_map.get(school['id'], default)
            school['grade_info'] = []
            stage = []
            for school_grade in school_grade_union_defaultdict.get(school['id'], []):
                g_info = grade_item_map.get(school_grade, {})
                if g_info:
                    g_info.update(stage_grade_union_map2.get(school_grade, {}))
                    school['grade_info'].append(g_info)
                    stage.append(stage_grade_union_map2.get(school_grade, {}).get("stage", StageEnum.Register.value))
            school['stage'] = StageEnum.Register.value if not stage else min(stage)

            data.append(school)
        return self.reply_ok({"school_list": data,
                              "extra": {"total": total_school_count,
                                        "number_per_page": per_page,
                                        "curr_page": page + 1}})

class SchoolDetail(QueryMixin):
    """
    学校详情
    {
        "school_id": ""
    }
    """
    @validate_permission(data_validation=True)
    async def clazz_list(self, request: Request):
        """
        班级列表
        {
            "school_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        school_id = int(request_param.get("school_id"))
        if not school_id:
            raise RequestError("school_id must not be empty")

        channel_old_id = ''
        channel_id = ''
        user_id = ''
        one_market = await request.app['mongodb'][self.db][self.instance_coll] \
            .find_one({"school_id": school_id,
                       "role": Roles.SCHOOL.value,
                       'status': 1})
        if one_market:
            user_id = one_market.get("user_id", '')
            one_channel = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({"_id": ObjectId(one_market.get("parent_id")),
                           'role': Roles.CHANNEL.value,
                           'status': 1})
            if one_channel:
                channel_old_id = one_channel.get('old_id')
                channel_id = str(one_channel.get("_id", ''))
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            include_channel = request['data_permission']['include_channel']
            if include_channel and channel_old_id not in include_channel:
                raise DataPermissionError("you have no right to access data")

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            if request['user_info']['channel_id'] != channel_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == Roles.MARKET.value:
            if int(request['user_info']['user_id']) != user_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == -1:
            raise DataPermissionError("you have no right to access data")


        items = await self._list_school_clazz(request, school_id)
        group_ids = [item["_id"] for item in items]
        if group_ids:
            sql = "select id, name from sigma_account_ob_group where available =1 and id in (%s) " % ','.join(
                [str(id) for id in group_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    clazz = await cur.fetchall()

            clazz_map = {}
            for cla in clazz:
                clazz_map[cla['id']] = cla

            for item in items:
                item['class_name'] = clazz_map.get(item['_id'], {}).get("name", "")
                item['total_pay_amount'] = self.rounding(item['total_pay_amount'])
                item['pay_ratio'] = self.rounding(item['pay_ratio'])
                item['bind_ratio'] = self.rounding(item['bind_ratio'])
            return self.reply_ok({"clazz_info": items})
        return self.reply_ok({})

class GradeDetail(QueryMixin):
    """
    年级详情
    """
    def __init__(self):
        super(GradeDetail, self).__init__()
        self.instance_coll = 'instance'

    @validate_permission(data_validation=True)
    async def grade_list(self, request: Request):
        """
        年级详情
        {
            "school_id": "",
            "grade": "",
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        try:
            school_id = int(request_param.get("school_id"))
            grade = request_param.get("grade")
            if not grade:
                raise RequestError("school_id or grade not exist")
        except:
            raise RequestError("school_id or grade is wrong")

        channel_old_id = ''
        channel_id = ''
        user_id = ''
        one_market = await request.app['mongodb'][self.db][self.instance_coll]\
            .find_one({"school_id": school_id,
                       "role": Roles.SCHOOL.value,
                       'status': 1})
        if one_market:
            user_id = one_market.get("user_id", '')
            one_channel = await request.app['mongodb'][self.db][self.instance_coll]\
                .find_one({"_id": ObjectId(one_market.get("parent_id")),
                           'role': Roles.CHANNEL.value,
                           'status': 1})
            if one_channel:
                channel_old_id = one_channel.get('old_id')
                channel_id = str(one_channel.get("_id",''))
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            include_channel = request['data_permission']['include_channel']
            if include_channel and channel_old_id not in include_channel:
                raise DataPermissionError("you have no right to access data")

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            if request['user_info']['channel_id'] != channel_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == Roles.MARKET.value:
            if int(request['user_info']['user_id']) != user_id:
                raise DataPermissionError("you have no right to access data")

        items = await self._list_grade(request, school_id, grade)
        group_ids = [item["_id"] for item in items]
        if group_ids:
            sql = "select id, name from sigma_account_ob_group where available =1 and id in (%s) " % ','.join([str(id) for id in group_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    clazz = await cur.fetchall()

            clazz_map = {}
            for cla in clazz:
                clazz_map[cla['id']] = cla

            for item in items:
                item['class_name'] = clazz_map.get(item['_id'], {}).get("name", "")
            return self.reply_ok({"grade_info": items})
        return self.reply_ok({})

    @validate_permission(data_validation=True)
    async def wap_grade_list(self, request: Request):
        """
        移动端年级班级详情
        {
            "school_id": "",
            "grade": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        school_id = int(request_param.get("school_id"))
        grade = str(request_param.get('grade'))

        channel_old_id = ''
        channel_id = ''
        user_id = ''
        one_market = await request.app['mongodb'][self.db][self.instance_coll] \
            .find_one({"school_id": school_id,
                       "role": Roles.SCHOOL.value,
                       'status': 1})
        if one_market:
            user_id = one_market.get("user_id", '')
            one_channel = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({"_id": ObjectId(one_market.get("parent_id")),
                           'role': Roles.CHANNEL.value,
                           'status': 1})
            if one_channel:
                channel_old_id = one_channel.get('old_id')
                channel_id = str(one_channel.get("_id", ''))
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            include_channel = request['data_permission']['include_channel']
            if include_channel and channel_old_id not in include_channel:
                raise DataPermissionError("you have no right to access data")

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            if request['user_info']['channel_id'] != channel_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == Roles.MARKET.value:
            if int(request['user_info']['user_id']) != user_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == -1:
            raise DataPermissionError("you have no right to access data")

        data = []

        sql = "select id, name, time_create " \
              "from sigma_account_ob_group " \
              "where available = 1 and school_id = %s " \
              "and grade = %s" % (school_id, grade)

        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(sql)
                clazz = await cur.fetchall()
        if clazz:
            default = {
                'total_valid_reading_number': 0,
                'total_valid_exercise_number': 0,
                'total_valid_word_number': 0,
            }
            items = await self._wap_list_grade_clazz(request, school_id, grade)
            item_map = {}
            for item in items:
                item_map[item['_id']] = item

            for cla in clazz:
                data.append(
                    {
                        "clazz_name": cla['name'],
                        "open_time": cla['time_create'],
                        "clazz_stat": item_map.get(cla['id'], default)
                    }
                )

        return self.reply_ok({"clazz_list": data})

class ClazzDetail(BaseHandler):
    """
    班级详情
    """
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'

    @validate_permission(data_validation=True)
    async def clazz_list(self, request: Request):
        """
        班级详情
        {
            "group_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        group_id = request_param.get("group_id")
        school_id = ''
        class_data = []
        if not group_id:
            raise RequestError("group_id must not be empty")

        school_id_sql = "select school_id from sigma_account_ob_group where available =1 and id = %s" % group_id
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(school_id_sql)
                school_id = await cur.fetchone()
        school_id = school_id.get("school_id", '')

        channel_old_id = ''
        channel_id = ''
        user_id = ''
        one_market = await request.app['mongodb'][self.db][self.instance_coll] \
            .find_one({"school_id": school_id,
                       "role": Roles.SCHOOL.value,
                       'status': 1})
        if one_market:
            user_id = one_market.get("user_id", '')
            one_channel = await request.app['mongodb'][self.db][self.instance_coll] \
                .find_one({"_id": ObjectId(one_market.get("parent_id")),
                           'role': Roles.CHANNEL.value,
                           'status': 1})
            if one_channel:
                channel_old_id = one_channel.get('old_id')
                channel_id = str(one_channel.get("_id", ''))
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            include_channel = request['data_permission']['include_channel']
            if include_channel and channel_old_id not in include_channel:
                raise DataPermissionError("you have no right to access data")

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            if request['user_info']['channel_id'] != channel_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == Roles.MARKET.value:
            if int(request['user_info']['user_id']) != user_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == -1:
            raise DataPermissionError("you have no right to access data")


        user_sql = "select u.id, u.name, u.student_vip_expire " \
                   "from sigma_account_us_user as u " \
                   "join sigma_account_re_groupuser as gu " \
                   "on u.available = 1 " \
                   "and gu.available = 1 " \
                   "and u.role_id = 2 " \
                   "and gu.user_id = u.id and gu.group_id = %s" % group_id
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(user_sql)
                users = await cur.fetchall()
        if users:
            user_ids = [item['id'] for item in users]
            user_wechat_sql = "select user_id from sigma_account_re_userwechat where user_id in (%s)" % (','.join([str(id) for id in user_ids]))



            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(user_wechat_sql)
                    userwechat = await cur.fetchall()

            userwechat_ids = [item['user_id'] for item in userwechat]
            pay_sql = "select user_id, coupon_amount " \
                      "from sigma_pay_ob_order " \
                      "where available = 1 " \
                      "and status = 3 " \
                      "and user_id in (%s)" %(','.join([str(id) for id in user_ids]))

            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(pay_sql)
                    pay = await cur.fetchall()

            pay_map = defaultdict(list)
            for p in pay:
                pay_map[p['user_id']].append(p['coupon_amount'])


            current_timestamp = time.time()
            for u in users:
                class_data.append({
                    "name": u["name"],
                    "student_id": u['id'],
                    "is_bind": 1 if u['id'] in userwechat_ids else 0,
                    "is_paid": 1 if sum(pay_map.get(u['id'],[0])) > 0 else 0,
                    "pay_amount": sum(pay_map.get(u['id'],[0])),
                    "duration": 0 if current_timestamp > u['student_vip_expire'] else (
                                datetime.fromtimestamp(u['student_vip_expire']) - datetime.fromtimestamp(
                            current_timestamp)).days
                })

        # sql = "select u.id, u.name, u.student_vip_expire, sum(o.coupon_amount) as total_amount, count(uw.wechat_id) as total_wechat " \
        #       "from sigma_account_re_groupuser as gu " \
        #       "join sigma_account_us_user as u " \
        #       "on u.available = 1 and gu.available = 1 and u.role_id = 2  and gu.user_id = u.id " \
        #       "left join sigma_pay_ob_order as o " \
        #       "on o.available = 1 and o.status = 3 and u.id = o.user_id " \
        #       "left join sigma_account_re_userwechat as uw  " \
        #       "on uw.available = 1 and uw.user_id = o.user_id " \
        #       "where gu.group_id = %s  " \
        #       "group by u.id,uw.user_id;" % (group_id)
        #
        # print(sql)
        # async with request.app['mysql'].acquire() as conn:
        #     async with conn.cursor(DictCursor) as cur:
        #         await cur.execute(sql)
        #         clazz = await cur.fetchall()
        # class_data = []
        #
        # # print(json.dumps(clazz, indent=4))
        #
        # for cla in clazz:
        #     current_timestamp = time.time()
        #     class_data.append({
        #         "name": cla["name"],
        #         "student_id": cla['id'],
        #         "is_bind": 1 if cla['total_wechat'] is not None and cla['total_wechat'] > 0 else 0,
        #         "is_paid": 1 if cla['total_amount'] is not None and cla['total_amount'] > 0 else 0,
        #         "pay_amount": cla['total_amount'] if cla['total_amount'] is not None else 0,
        #         "duration": 0 if current_timestamp > cla['student_vip_expire'] else (datetime.fromtimestamp(cla['student_vip_expire']) - datetime.fromtimestamp(current_timestamp)).days
        #     })

        return self.reply_ok({"clazz_info": class_data})

class MarketDetail(QueryMixin, DataExcludeMixin):
    """
    市场详情

    """
    def __init__(self):
        super(MarketDetail, self).__init__()
        self.db = "sales"
        self.grade_coll = "grade"
        self.instance_coll = 'instance'
        self.default = {
                "total_teacher_number": 0,
                "total_student_number": 0,
                "total_guardian_number": 0,
                "total_pay_number": 0,
                "total_pay_amount": 0,
                # "contest_coverage_ratio": 0,  # 测评覆盖率
                # "contest_average_per_person": 0,  # 人均测评数/月
                "total_valid_exercise_number": 0,
                "total_valid_reading_number": 0,  # 有效阅读
                "total_valid_word_number": 0,
                "total_exercise_image_number": 0,
                "total_word_image_number": 0,
                "contest_coverage_ratio": 0,
                "contest_average_per_person": 0,
                "pay_ratio": 0,
                "bind_ratio": 0,
            }

    @validate_permission(data_validation=True)
    async def school_list(self, request: Request):
        """
        学校列表
         {
            "page": 0
            "user_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        page = int(request_param.get("page", 1)) - 1
        per_page = 10
        channel_old_id = ''
        area_id = ''
        channel_id = ''
        user_id = request_param.get("user_id")
        if request['user_info']['instance_role_id'] == Roles.MARKET.value: #市场
            user_id = request['user_info']['user_id']
        else:

            one_market = await request.app['mongodb'][self.db][self.instance_coll]\
                .find_one({"user_id": int(user_id),
                           "role": Roles.SCHOOL.value,
                           "status": 1})
            if one_market:
                one_channel = await request.app['mongodb'][self.db][self.instance_coll]\
                    .find_one({"_id": ObjectId(one_market.get("parent_id")),
                               "role": Roles.CHANNEL.value,
                               'status': 1})
                if one_channel:
                    channel_old_id = one_channel.get('old_id','')
                    area_id = one_channel.get("parent_id", '')
                    channel_id = str(one_channel.get("_id" , ''))
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            include_channel = request['data_permission']['include_channel']
            if include_channel and channel_old_id not in include_channel:
                raise DataPermissionError("you have no right to access data")
            exist = await request.app['mongodb'][self.db][self.instance_coll]\
                .find_one({"old_id": channel_old_id,
                           "role": Roles.CHANNEL.value,
                           'status': 1})
            if not exist:
                return self.reply_ok(
                    {"school_list": [], "extra": {"total": 0, "number_per_page": per_page, "curr_page": page + 1}})
        elif request['user_info']['instance_role_id'] == Roles.AREA.value:
            if request['user_info']['area_id'] != area_id:
                raise DataPermissionError("you have no right to access data")
        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value:
            if request['user_info']['channel_id'] != channel_id:
                raise DataPermissionError("you have no right to access data")


        total_counts = await request.app['mongodb'][self.db][self.instance_coll].count_documents({"user_id": int(user_id),
                                                                            "role": Roles.SCHOOL.value,
                                                                        "status": 1})
        if total_counts <= 0:
            return self.reply_ok({"school_list": [], "extra": {"total": 0, "number_per_page": per_page, "curr_page": page + 1}})
        # channel_info = await request.app['mongodb'][self.db][self.instance_coll].find_one({"user_id": user_id,
        #                                                                                    "role": Roles.MARKET.value,
        #                                                                                    "status": 1})
        schools = request.app['mongodb'][self.db][self.instance_coll].find({"user_id": int(user_id),
                                                                            "role": Roles.SCHOOL.value,
                                                                            "status": 1}).skip(page*per_page).limit(per_page)
        schools = await schools.to_list(10000)
        if not schools :
            return self.reply_ok({"school_list": [],"extra": {"total": 0, "number_per_page": per_page, "curr_page": page + 1}})
        school_ids = [item['school_id'] for item in schools]

        school_sql = "select id, full_name, time_create " \
                     "from sigma_account_ob_school " \
                     "where available = 1 and id in (%s)" % (",".join([str(id) for id in school_ids]))
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(school_sql)
                real_school = await cur.fetchall()

        real_school_map = {}
        for school in real_school:
            real_school_map[school['id']] = school

        school_ids = [item['id'] for item in real_school]


        school_items = await self._list_school(request, school_ids)
        grade_items = await self._list_school_grade(request, school_ids)


        school_items_map = {}
        for item in school_items:
            item["contest_coverage_ratio"] = 0
            item["contest_average_per_person"] = 0
            school_items_map[item["_id"]] = item

        school_grade_mongo = request.app['mongodb'][self.db][self.grade_coll].find({"school_id": {"$in": school_ids}})
        school_grade_mongo = await school_grade_mongo.to_list(10000)
        school_stage_mongo_map = defaultdict(list)
        school_grade_stage_map = {}
        for s_g_m in school_grade_mongo:
            school_stage_mongo_map[s_g_m['school_id']].append(s_g_m['stage'])
            school_grade_stage_map[str(s_g_m['school_id'])+'@'+s_g_m['grade']] = s_g_m['stage']
        grade_of_school_sql = "select grade, school_id, time_create from sigma_account_ob_group where available = 1 " \
                              "and school_id in (%s) group by school_id, grade" % (",".join([str(id) for id in school_ids]))
        async with request.app['mysql'].acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(grade_of_school_sql)
                school_grade = await cur.fetchall()
        real_school_stage_defaultdict = defaultdict(list)
        grade_info_map = {}
        for s_g in school_grade:
            grade_info_map[str(s_g['school_id'])+"@"+s_g['grade']] = s_g
            real_school_stage_defaultdict[s_g['school_id']] + (school_stage_mongo_map.get(s_g['school_id'],[StageEnum.Register.value]))

        grade_items_defaultdict = defaultdict(list)
        grade_item_schoo_grade_id_map = {}

        for item in grade_items:
            item["contest_coverage_ratio"] = 0
            item["contest_average_per_person"] = 0
            grade_item_schoo_grade_id_map[str(item["_id"]['school_id'])+"@"+item["_id"]['grade']] = item


        for school_grade_id, data in grade_info_map.items():

            item = grade_item_schoo_grade_id_map.get(school_grade_id, self.default)
            item['stage'] = school_grade_stage_map.get(school_grade_id, StageEnum.Register.value)
            item['school_id'] = int(school_grade_id.split('@')[0]),
            item['grade'] = school_grade_id.split('@')[1]
            item['time_create'] = grade_info_map.get(school_grade_id, {}).get("time_create", 0)
            grade_items_defaultdict[int(school_grade_id.split('@')[0])]\
                .append(item)


        for school in schools:
            school['school_id'] = real_school_map.get(school['school_id']).get("id", "")
            school['name'] = real_school_map.get(school['school_id']).get("full_name", "")
            school['stage'] = min(real_school_stage_defaultdict.get(school['school_id'])) \
                if real_school_stage_defaultdict.get(school['school_id']) else StageEnum.Register.value
            school['school_stat'] = school_items_map.get(school['school_id'], self.default)
            school['grade_stat'] = grade_items_defaultdict.get(school['school_id'], [])
            # print(school['school_id'])

        # print(json.dumps(school_items_map, indent=4))

        return self.reply_ok({"school_list": schools, "extra": {"total": total_counts, "number_per_page": per_page, "curr_page": page + 1}})






