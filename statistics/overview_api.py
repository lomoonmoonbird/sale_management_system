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
from utils import get_json, get_params, validate_permission
from basehandler import BaseHandler
from exceptions import InternalError, UserExistError, CreateUserError, DELETEERROR, RequestError
from menu.menu import Menu
from motor.core import Collection
from enum import Enum
from aiomysql.cursors import DictCursor
from pymongo import UpdateOne, DeleteMany
from bson import ObjectId
from enumconstant import Roles, PermissionRole
from mixins import DataExcludeMixin


class Overview(BaseHandler, DataExcludeMixin):
    def __init__(self):
        self.db = 'sales'
        self.instance_coll = 'instance'
        self.class_per_day_coll = 'class_per_day'
        self.grade_per_day_coll = 'grade_per_day'
        self.channel_per_day_coll = 'channel_per_day'
        self.school_per_day_coll = 'school_per_day'

    @validate_permission(data_validation=True)
    async def overview(self, request: Request):
        """
        总部数据总览
        :param request:
        :return:
        """
        exclude_channels = await self.exclude_channel(request.app['mysql'])
        if request['user_info']['instance_role_id'] == Roles.GLOBAL.value:
            exclude_channels += request['data_permission']['exclude_channel']
            exclude_area = request['data_permission']['exclude_area']
            channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": {"$nin": exclude_area},
                                                                                    "role": Roles.CHANNEL.value,
                                                                                    "status": 1},
                                                                                   {"old_id": 1})
            channels = await channels.to_list(None)
            channel_ids = [item['old_id'] for item in channels if item['old_id'] not in exclude_channels]
            pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number(request, channel_ids)

            pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount(request, channel_ids)

            total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number(request, channel_ids)

            teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number(request, channel_ids)
            student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number(request, channel_ids)

            image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number(request, channel_ids)

            guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number(request, channel_ids)

            contest_total, contest_curr_week_new_number, contest_last_week_new_number = await self._valid_contest_number(request, channel_ids)

            exercise_total, exercise_curr_week_new_number, exercise_last_week_new_number = await self._valid_exercise_number(
                request, channel_ids)

            word_total, word_curr_week_new_number, word_last_week_new_number = await self._valid_word_number(
                request, channel_ids)

            e_image_total, e_image_curr_week_new_number, e_image_last_week_new_number = await self._exercise_images_number(request,
                                                                                                            channel_ids)

            w_image_total, w_image_curr_week_new_number, w_image_last_week_new_number = await self._word_images_number(request,
                                                                                                            channel_ids)

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

                                  "e_image_total": e_image_total,
                                  "e_image_curr_week_new_number": e_image_curr_week_new_number,
                                  "e_image_last_week_new_number": e_image_last_week_new_number,

                                  "w_image_total": w_image_total,
                                  "w_image_curr_week_new_number": w_image_curr_week_new_number,
                                  "w_image_last_week_new_number": w_image_last_week_new_number,

                                  "guardian_total": guardian_total,
                                  "guardian_curr_week_new_number": guardian_curr_week_new_number,
                                  "guardian_last_week_new_number": guardian_last_week_new_number,
                                  "contest_total": contest_total,
                                  "contest_curr_week_new_number": contest_curr_week_new_number,
                                  "contest_last_week_new_number": contest_last_week_new_number,

                                  "exercise_total": exercise_total,
                                  "exercise_curr_week_new_number": exercise_curr_week_new_number,
                                  "exercise_last_week_new_number": exercise_last_week_new_number,

                                  "word_total": word_total,
                                  "word_curr_week_new_number": word_curr_week_new_number,
                                  "word_last_week_new_number": word_last_week_new_number,

                                  })
        elif request['user_info']['instance_role_id'] == Roles.AREA.value:
            # exclude_channels = await self.exclude_channel(request.app['mysql'])
            channels = request.app['mongodb'][self.db][self.instance_coll].find({"parent_id": request['user_info']['area_id'],
                                                                                       "role": Roles.CHANNEL.value,
                                                                                       "status": 1})

            channels = await channels.to_list(10000)
            old_ids = [item['old_id'] for item in channels]
            # old_ids = list(set(old_ids).difference(set(exclude_channels)))
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

            contest_total, contest_curr_week_new_number, contest_last_week_new_number = await self._valid_contest_number_area(
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
                                  "guardian_last_week_new_number": guardian_last_week_new_number,
                                  "contest_total": contest_total,
                                  "contest_curr_week_new_number": contest_curr_week_new_number,
                                  "contest_last_week_new_number": contest_last_week_new_number
                                  })

        elif request['user_info']['instance_role_id'] == Roles.CHANNEL.value: #渠道
            channel_id = request['user_info']['channel_id']
            # exclude_channels = await self.exclude_channel(request.app['mysql'])

            channel = await request.app['mongodb'][self.db][self.instance_coll].find_one({"_id": ObjectId(channel_id), 'status': 1})
            old_ids = [channel.get('old_id', -2) if channel else {}]
            # old_ids = list(set(old_ids).difference(set(exclude_channels)))
            pay_total, pay_curr_week_new_number, pay_last_week_new_number = await self._pay_number_channel(request,
                                                                                                        old_ids)

            pay_amount, pay_curr_week_new_amount, pay_last_week_new_amount = await self._pay_amount_channel(request,
                                                                                                         old_ids)

            total_school_number, curr_week_new_school_number, last_week_new_school_number = await self._school_number_channel(
                request, old_ids)

            teacher_total, teacher_curr_week_new_number, teacher_last_week_new_number = await self._teacher_number_channel(
                request, old_ids)
            student_total, student_curr_week_new_number, student_last_week_new_number = await self._student_number_channel(
                request, old_ids)

            image_total, image_curr_week_new_number, image_last_week_new_number = await self._images_number_channel(
                request, old_ids)

            guardian_total, guardian_curr_week_new_number, guardian_last_week_new_number = await self._guardian_number_channel(
                request, old_ids)

            contest_total, contest_curr_week_new_number, contest_last_week_new_number = await self._valid_contest_number_channel(
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
                                  "guardian_last_week_new_number": guardian_last_week_new_number,
                                  "contest_total": contest_total,
                                  "contest_curr_week_new_number": contest_curr_week_new_number,
                                  "contest_last_week_new_number": contest_last_week_new_number
                                  })
        else:
            return self.reply_ok({})

    async def _valid_contest_number_channel(self, request:Request, channle_ids=[], exclude_channels=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

    async def _guardian_number_channel(self, request:Request, channle_ids=[], exclude_channels=[]):
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

    async def _images_number_channel(self, request: Request, channle_ids=[], exclude_channels=[]):
        """
        图片数
        :param request:
        :return:
        """
        coll = request.app['mongodb'][self.db][self.channel_per_day_coll]
        total_image_count_list = []
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

    async def _teacher_number_channel(self, request: Request, channle_ids=[], exclude_channels = []):
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

    async def _student_number_channel(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _school_number_channel(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _pay_number_channel(self, request: Request, channle_ids=[], exclude_channels=[]):
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

                    {"$group": {"_id": "",
                                "total": {"$sum": "$pay_number"},
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

    async def _pay_amount_channel(self, request: Request, channle_ids=[], exclude_channels=[]):
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

                    {"$group": {"_id": "",
                                "total": {"$sum": "$pay_amount"},
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


    async def _valid_word_number(self, request: Request, channel_ids=[]):
        """
        有效单词数
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_word_count"}
                    }
                },

                {"$group": {"_id": None,
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_word_count"}
                    }
                },

                {"$group": {"_id": None,
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_word_count"}
                    }
                },

                {"$group": {"_id": None,
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

    async def _valid_exercise_number(self, request: Request, channel_ids=[]):
        """
        有效考试数
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_exercise_count"}
                    }
                },

                {"$group": {"_id": None,
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_exercise_count"}
                    }
                },

                {"$group": {"_id": None,
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$valid_exercise_count"}
                    }
                },

                {"$group": {"_id": None,
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

    async def _valid_contest_number(self, request:Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "total_exercise": {"$sum": "$valid_exercise_count"},
                        "total_word": {"$sum": "$valid_word_count"},
                        "total": {"$sum": ["$valid_exercise_count", "$valid_word_count", "$valid_reading_count"]}
                    }
                },

                {"$group": {"_id": None,
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
                        "channel": {"$in": channel_ids}
                    }
                },
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

        last_week_new_guardian_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channel_ids}
                    }
                },
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

    async def _guardian_number(self, request:Request, channel_ids=[]):
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
                        "$match": {"channel": {"$in": channel_ids}}
                    },
                    {
                        "$project": {
                            "guardian_count": 1,
                            "day": 1
                        }
                    },

                    {"$group": {"_id": None,
                                "total": {"$sum": "$guardian_count"},
                                }
                     },


                ])
        current_week_new_guardian_count =  coll.aggregate(
                [
                    {
                        "$match": {
                                "day":  {"$gte": current_week[0],
                                          "$lte": current_week[6]},
                            "channel": {"$in": channel_ids}
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
                                }
                     },


                ])

        last_week_new_guardian_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},

                        "channel": {"$in": channel_ids}
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

    async def _images_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
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
                        "channel": {"$in": channel_ids}
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
                        "channel": {"$in": channel_ids}
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

    async def _exercise_images_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": "$e_image_c" }
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$e_image_c" }
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$e_image_c" }
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

    async def _word_images_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "total": {"$sum": "$w_image_c" }
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$w_image_c" }
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
                        "channel": {"$in": channel_ids}
                    }
                },
                {
                    "$project": {
                        "total": {"$sum": "$w_image_c" }
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

    async def _teacher_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
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
                        "channel": {"$in": channel_ids}
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
                        "channel": {"$in": channel_ids}
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

    async def _student_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
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
                        "channel": {"$in": channel_ids}
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
                        "channel": {"$in": channel_ids}
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

    async def _school_number(self, request: Request, channel_ids=[]):
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
                    "$match": {"channel": {"$in": channel_ids}}
                },
                {
                    "$project": {
                        "school_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": None,
                            "total": {"$sum": "$school_number"}
                            }
                 },

            ])
        current_week_new_school_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": current_week[0],
                                "$lte": current_week[6]},
                        "channel": {"$in": channel_ids}
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
                            }
                 },

            ])

        last_week_new_school_count = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channel_ids}
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

    async def _pay_number(self, request: Request,  channel_ids=[]):
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
                            "channel": {"$in": channel_ids}
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
                                }
                     },


                ])

        last_week_new_pay_number = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channel_ids}
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
                            }
                 },

            ])

        total_pay_number = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channel_ids},
                               "day": {"$gte": request['data_permission']['pay_stat_start_time']}}
                },
                {
                    "$project": {
                        "pay_number": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": "",
                            "total": {"$sum": "$pay_number"},
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

    async def _pay_amount(self, request: Request, channel_ids=[]):
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
                            "channel": {"$in": channel_ids}
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
                                }
                     },


                ])

        last_week_new_pay_amount = coll.aggregate(
            [
                {
                    "$match": {
                        "day": {"$gte": last_week[0],
                                "$lte": last_week[6]},
                        "channel": {"$in": channel_ids}
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
                            }
                 },

            ])

        total_pay_amount = coll.aggregate(
            [
                {
                    "$match": {"channel": {"$in": channel_ids},
                               "day": {"$gte": request['data_permission']['pay_stat_start_time']}}
                },
                {
                    "$project": {
                        "pay_amount": 1,
                        "day": 1
                    }
                },

                {"$group": {"_id": "",
                            "total": {"$sum": "$pay_amount"},
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



    async def _valid_contest_number_area(self, request:Request, channle_ids=[], exclude_channels=[]):
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

                {"$group": {"_id": None,
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

    async def _guardian_number_area(self, request:Request, channle_ids=[], exclude_channels=[]):
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

    async def _images_number_area(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _teacher_number_area(self, request: Request, channle_ids=[], exclude_channels = []):
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

    async def _student_number_area(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _school_number_area(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _pay_number_area(self, request: Request, channle_ids=[], exclude_channels=[]):
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

    async def _pay_amount_area(self, request: Request, channle_ids=[], exclude_channels=[]):
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

                    {"$group": {"_id": "",
                                "total": {"$sum": "$pay_amount"},
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


