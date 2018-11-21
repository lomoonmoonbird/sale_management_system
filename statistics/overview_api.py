#! python3.6
# --*-- coding: utf-8 --*--

"""
数据统计 ： 数据总览
"""

from datetime import datetime, timedelta
import asyncio
import json
import time
from aiohttp.web import Request
from configs import UC_SYSTEM_API_ADMIN_URL, THEMIS_SYSTEM_ADMIN_URL, ucAppKey, ucAppSecret, permissionAppKey
import aiohttp
import ujson
from utils import get_json, get_params
from demo.utils import validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError
from menu.menu import Menu
from auth.utils import insert_area
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId
from enumconstant import Roles, PermissionRole


class Overview(BaseHandler):
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'

    @validate_permission()
    async def overview(self, request: Request):
        """
        总部数据总览
        :param request:
        :return:
        """
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number(request)

            pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount(request)

            total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number(request)

            teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number(request)
            student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number(request)

            image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number(request)

            guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number(request)

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
        elif request['user_info']['instance_role_id'] == Roles.AREA.value:

            channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": request['user_info']['area_id'],
                                                                                       "role": Roles.AREA.value,
                                                                                       "status": 1})

            channels = await channels.to_list(10000)
            old_ids = [item['old_id'] for item in channels]

            pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number_area(request, old_ids)

            pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount_area(request, old_ids)

            total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number_area(
                request, old_ids)

            teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number_area(
                request, old_ids)
            student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number_area(
                request, old_ids)

            image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number_area(request, old_ids)

            guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number_area(
                request, old_ids)

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


    async def _guardian_number(self, request:Request):
        """
        家长数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        total_guardian_count_list = []
        current_week_new_guardian_count_list = []
        last_week_new_guardian_count_list = []
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            total_guardian_count = coll.aggregate(
                    [
                        {
                            "$project": {
                                "guardian_count": 1,
                                "day": 1
                            }
                        },

                        {"$group": {"_id": None,
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
                                              "$lte": current_week[6]}
                        }
                        },
                        {
                            "$project": {
                                "guardian_count": 1,
                                "day": 1
                            }
                        },

                        {"$group": {"_id": None,
                                    "total": {"$sum": "$guardian_count"},
                                    # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                    }
                         },


                    ])

            last_week_new_guardian_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": last_week[0],
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "guardian_count": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$guardian_count"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
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

    async def _images_number(self, request: Request):
        """
        图片数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        total_image_count_list = []
        current_week_new_image_count_list = []
        last_week_new_image_count_list = []
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            total_image_count = coll.aggregate(
                [
                    {
                        "$project": {
                            "total": {"$sum":   [ "$e_image_c", "$w_image_c" ] }
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$total"}
                                }
                     },

                ])

            current_week_new_image_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": current_week[0],
                                    "$lte": current_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "total": {"$sum":   [ "$e_image_c", "$w_image_c" ] }
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$total"}
                                }
                     },

                ])

            last_week_new_image_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": last_week[0],
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "total": {"$sum":   [ "$e_image_c", "$w_image_c" ] }
                        }
                    },

                    {"$group": {"_id": "",
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

    async def _teacher_number(self, request: Request):
        """
        老师数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.grade_per_day_coll]
        total_teacher_count_list = []
        current_week_new_teacher_count_list = []
        last_week_new_teacher_count_list = []
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            total_teacher_count = coll.aggregate(
                [
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
            current_week_new_teacher_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": current_week[0],
                                    "$lte": current_week[6]}
                        }
                    },
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

            last_week_new_teacher_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": last_week[0],
                                    "$lte": last_week[6]}
                        }
                    },
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

    async def _student_number(self, request: Request):
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
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            total_student_count = coll.aggregate(
                [
                    {
                        "$project": {
                            "student_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$student_number"}
                                }
                     },

                ])
            current_week_new_student_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": current_week[0],
                                    "$lte": current_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "student_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$student_number"}
                                }
                     },

                ])

            last_week_new_student_count = coll.aggregate(
                [
                    {
                        "$match": {
                            "day": {"$gte": last_week[0],
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "student_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
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

    async def _school_number(self, request: Request):
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
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            total_school_count = coll.aggregate(
                [
                    {
                        "$project": {
                            "school_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
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
                                    "$lte": current_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "school_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
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
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "school_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
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

    async def _pay_number(self, request: Request):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        total_pay_count_list = []
        current_week_new_pay_count_list = []
        last_week_new_pay_count_list = []
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            current_week_new_pay_number =  coll.aggregate(
                    [
                        {
                            "$match": {
                                    "day":  {"$gte": current_week[0],
                                              "$lte": current_week[6]}
                        }
                        },
                        {
                            "$project": {
                                "pay_number": 1,
                                "day": 1
                            }
                        },

                        {"$group": {"_id": "",
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
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "pay_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": "",
                                "total": {"$sum": "$pay_number"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                }
                     },

                ])

            total_pay_number = coll.aggregate(
                [
                    {
                        "$project": {
                            "pay_number": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": "",
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

    async def _pay_amount(self, request: Request):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
        total_pay_amount_list = []
        current_week_new_pay_amount_list = []
        last_week_new_pay_amount_list = []
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            current_week = self.current_week()
            last_week = self.last_week()
            current_week_new_pay_amount =  coll.aggregate(
                    [
                        {
                            "$match": {
                                    "day":  {"$gte": current_week[0],
                                              "$lte": current_week[6]}
                        }
                        },
                        {
                            "$project": {
                                "pay_amount": 1,
                                "day": 1
                            }
                        },

                        {"$group": {"_id": "",
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
                                    "$lte": last_week[6]}
                        }
                    },
                    {
                        "$project": {
                            "pay_amount": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": "",
                                "total": {"$sum": "$pay_amount"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
                                }
                     },

                ])

            total_pay_amount = coll.aggregate(
                [
                    {
                        "$project": {
                            "pay_amount": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": "",
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


    async def _guardian_number_area(self, request:Request, channle_ids=[]):
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

                    {"$group": {"_id": None,
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

                    {"$group": {"_id": None,
                                "total": {"$sum": "$guardian_count"},
                                # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
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

                {"$group": {"_id": None,
                            "total": {"$sum": "$guardian_count"},
                            # "pp": {"$push": {"$cond": [{"$gte": ["$day",current_week[0]]}, {"aaaa": "$class_pay_amount" },0 ] }}
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

    async def _images_number_area(self, request: Request, channle_ids=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": "",
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

    async def _teacher_number_area(self, request: Request, channle_ids=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

    async def _student_number_area(self, request: Request, channle_ids=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

    async def _school_number_area(self, request: Request, channle_ids=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

    async def _pay_number_area(self, request: Request, channle_ids=[]):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
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

                    {"$group": {"_id": "",
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

                {"$group": {"_id": "",
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

                {"$group": {"_id": "",
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

    async def _pay_amount_area(self, request: Request, channle_ids=[]):
        """
        付费数
        :param coll:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.class_per_day_coll]
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

                    {"$group": {"_id": "",
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

                {"$group": {"_id": "",
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

                {"$group": {"_id": "",
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


