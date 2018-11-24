#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 某大区 某渠道 某市场 的数据
"""

from datetime import datetime, timedelta
import asyncio
import json
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL, ucAppKey, ucAppSecret, permissionAppKey
import aiohttp
import ujson
from utils import get_json, get_params, validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError, ChannelNotExist
from menu.menu import Menu
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId
from enumconstant import Roles, PermissionRole


class QueryMixin(BaseHandler):

    def __init__(self):
        self.db = 'sales'
        self.user_coll = 'sale_user'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'

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
                    "$match": {"channel": {"$in": channle_ids}}
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
                    "$match": {"channel": {"$in": channle_ids}}
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


    async def _list(self, request: Request, channel_ids: list):
        """
        学校数
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
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$channel",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
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
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_number", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

    async def _list_channel(self, request: Request, school_ids: list):
        """
        学校数
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
                        "school_id":1,
                        "channel": 1,
                        "school_number": 1,
                        "teacher_number": 1,
                        "student_number": 1,
                        "guardian_count": 1,
                        "pay_number": 1,
                        "pay_amount": 1,
                        "valid_exercise_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_exercise_count", 0]},
                        "e_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$e_image_c", 0]},
                        "valid_word_count": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$valid_word_count", 0]},
                        "w_image_c": {"$cond": [{"$and": [{"$lt": ["$day", yesterday_str]}, {
                            "$gte": ["$day", yesterday_before_30day_str]}]}, "$w_image_c", 0]},

                        "day": 1
                    }
                },

                {"$group": {"_id": "$school_id",
                            "total_school_number": {"$sum": "$school_number"},
                            "total_teacher_number": {"$sum": "$teacher_number"},
                            "total_student_number": {"$sum": "$student_number"},
                            "total_guardian_number": {"$sum": "$guardian_count"},
                            "total_pay_number": {"$sum": "$pay_number"},
                            "total_pay_amount": {"$sum": "$pay_amount"},
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
                        "total_pay_number": 1,
                        "total_pay_amount": 1,
                        "total_valid_exercise_number": 1,
                        "total_valid_word_number": 1,
                        "total_exercise_image_number": 1,
                        "total_word_image_number": 1,
                        "pay_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0, {"$divide": ["$total_pay_number", "$total_student_number"]}]},
                        "bind_ratio": {"$cond": [{"$eq": ["$total_student_number", 0]}, 0,
                                                {"$divide": ["$total_guardian_number", "$total_student_number"]}]},
                    }

                }

            ])

        async for item in item_count:
            items.append(item)

        return items

class AreaDetail(QueryMixin):
    """
    大区详情
    """
    def __init__(self):
        super(AreaDetail, self).__init__()


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

        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1})
        channels = await channels.to_list(10000)

        old_ids = [item['old_id'] for item in channels]

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
                              "guardian_last_week_new_number": guardian_last_week_new_number
                              })

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
        page = int(request_param.get("page", 0))
        per_page = 100
        if not area_id:
            return self.reply_ok([])

        channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": area_id,
                                                                             "role": Roles.CHANNEL.value,
                                                                             "status": 1}).skip(page*per_page).limit(per_page)
        channels = await channels.to_list(10000)

        old_ids = [item['old_id'] for item in channels]


        items = []
        if old_ids:
            sql = "select id, name from sigma_account_us_user where available = 1 and id in (%s) " % \
                  ','.join([str(id) for id in old_ids])
            async with request.app['mysql'].acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(sql)
                    real_channels = await cur.fetchall()

            items = await self._list(request, old_ids)
            channel_id_map = {}
            for channel in real_channels:
                channel_id_map[channel['id']] = channel

            for item in items:
                item['contest_coverage_ratio'] = 0
                item['contest_average_per_person'] = 0
                item['area_info'] = channel_id_map.get(item['_id'], {})

        return self.reply_ok(items)


class ChannelDetail(QueryMixin):
    """
    渠道详情
    """
    def __init__(self):
        super(ChannelDetail, self).__init__()

    async def overview(self, request: Request):
        """
        渠道详情总览
        {
            "channel_id": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_id = request_param.get("channel_id", "")
        if not channel_id:
            return self.reply_ok([])
        channel = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id), "status": 1})
        if channel:
            channel_old_id = [channel.get("old_id", 0)]
            schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id, "role": Roles.SCHOOL.value, "status": 1})
            schools = await schools.to_list(10000)
            school_ids = [item['school_id'] for item in schools]
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

            print ()

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
                                  "guardian_last_week_new_number": guardian_last_week_new_number
                                  })
        # raise ChannelNotExist("Channel not exist")
        return self.reply_ok([])

    async def market_list(self, request: Request):
        """
        市场列表
        {
            "area_id": ""
            "page": ""
        }
        :param request:
        :return:
        """
        request_param = await get_params(request)
        channel_id = request_param.get("channel_id", "")
        page = int(request_param.get("page", 0))
        per_page = 100
        if not channel_id:
            return self.reply_ok([])
        schools = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": channel_id, "role": Roles.SCHOOL.value, "status": 1})
        schools = await schools.to_list(100000)
        schools_ids = list(set([item['school_id'] for item in schools]))
        market_users = request.app['mongodb'][self.db][self.user_coll].find({"channel_id": channel_id, "instance_role_id": Roles.MARKET.value, "status": 1})
        market_users = await market_users.to_list(10000)
        market_users_user_ids = [item['user_id'] for item in market_users]
        users = request.app['mongodb'][self.db][self.user_coll].find({"user_id": {"$in": market_users_user_ids}}).skip(page*per_page).limit(per_page)
        users = await users.to_list(10000)

        market_users_map = {}
        for market in users:
            market_users_map[market['user_id']] = market

        school_market_map = {}
        for school in schools:
            school_market_map[school['school_id']] = market_users_map.get(str(school['user_id']))
        items = await self._list_channel(request, schools_ids)

        from collections import defaultdict
        channel_campact_data = defaultdict(dict)
        for item in items:
            item['contest_coverage_ratio'] = 0
            item['contest_average_per_person'] = 0
            channel_campact_data.setdefault(school_market_map.get(item['_id'], {}).get("user_id", ""), {}).setdefault(
                'total_school_number', []).append(item['total_school_number'])

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

        from utils import CustomEncoder
        print(json.dumps(items , indent=4, cls=CustomEncoder))


        return self.reply_ok(items)