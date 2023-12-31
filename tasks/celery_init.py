#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""
import os
os.sys.path.insert(0, '/app/hexin/Sales-management-system')
from celery import Celery
from kombu import Queue, Exchange
from celery.schedules import crontab
import configs
from tasks.celery_per_day_task import  PerDayTask, PerDaySubTask_IMAGES,PerDaySubTask_GUARDIAN,\
    PerDaySubTask_PAYMENTS, PerDaySubTask_USERS, PerDayTask_SCHOOL, PerDayTask_VALIDCONTEST, PerDayTask_VALIDREADING


sales_celery = Celery("Sales")

class Config():
    broker_url = configs.BROKER_URL
    result_backend = configs.RESULT_BACKEND
    loglevel = 'DEBUG'
    timezone='Asia/Shanghai'
    enable_utc=True
    installed_apps  = ('tasks')
    beat_schedule= {
        # "per_day_task": {
        #     "task": "tasks.celery_per_day_task.PerDayTask",
        #     "schedule": crontab(hour=15,minute=35)
        # },
        "per_day_pay": {
            "task": "tasks.celery_per_day_task.PerDaySubTask_PAYMENTS",
            "schedule": crontab(hour=0)
        },
        "per_day_image": {
            "task": "tasks.celery_per_day_task.PerDaySubTask_IMAGES",
            "schedule": crontab(hour=0)
        },
        "per_day_guardian": {
            "task": "tasks.celery_per_day_task.PerDaySubTask_GUARDIAN",
            "schedule": crontab(hour=0)
        },
        "per_day_users": {
            "task": "tasks.celery_per_day_task.PerDaySubTask_USERS",
            "schedule": crontab(hour=0)
        },
        "per_day_school": {
            "task": "tasks.celery_per_day_task.PerDayTask_SCHOOL",
            "schedule": crontab(hour=0)
        },
        "per_day_validcontest": {
            "task": "tasks.celery_per_day_task.PerDayTask_VALIDCONTEST",
            "schedule": crontab(hour=0)
        },
        "per_day_validreading": {
            "task": "tasks.celery_per_day_task.PerDayTask_VALIDREADING",
            "schedule": crontab(hour=0)
        },


    }
    task_default_queue = 'sale_defult_queue'
    task_queues = (
        Queue('user_per_day', Exchange('user_per_day'), routing_key="user_per_day"),
        Queue('per_day_pay', Exchange('per_day_pay'), routing_key="per_day_pay"),
        Queue('per_day_images', Exchange('per_day_images'), routing_key="per_day_images"),
        Queue('per_day_guardian', Exchange('per_day_guardian'), routing_key="per_day_guardian"),
        Queue('per_day_user', Exchange('per_day_user'), routing_key="per_day_user"),
        Queue('per_day_school', Exchange('per_day_school'), routing_key="per_day_school"),
        Queue('per_day_validcontest', Exchange('per_day_validcontest'), routing_key="per_day_validcontest"),
        Queue('per_day_validreading', Exchange('per_day_validreading'), routing_key="per_day_validreading"),

    )

    task_routes = {
        'tasks.celery_per_day_task.PerDayTask': {
            'queue': 'user_per_day',
            'routing_key': 'user_per_day'
        },
        'tasks.celery_per_day_task.PerDaySubTask_PAYMENTS': {
            'queue': 'per_day_pay',
            'routing_key': 'per_day_pay'
        },
        'tasks.celery_per_day_task.PerDaySubTask_IMAGES': {
            'queue': 'per_day_images',
            'routing_key': 'per_day_images'
        },
        'tasks.celery_per_day_task.PerDaySubTask_GUARDIAN': {
            'queue': 'per_day_guardian',
            'routing_key': 'per_day_guardian'
        },
        'tasks.celery_per_day_task.PerDaySubTask_USERS': {
            'queue': 'per_day_user',
            'routing_key': 'per_day_user'
        },
        'tasks.celery_per_day_task.PerDayTask_SCHOOL': {
            'queue': 'per_day_school',
            'routing_key': 'per_day_school'
        },
        'tasks.celery_per_day_task.PerDayTask_VALIDCONTEST': {
            'queue': 'per_day_validcontest',
            'routing_key': 'per_day_validcontest'
        },
        'tasks.celery_per_day_task.PerDayTask_VALIDREADING': {
            'queue': 'per_day_validreading',
            'routing_key': 'per_day_validreading'
        },

    }

    # celery_queues = (
    #     # Queue('summary', Exchange('summary') ,routing_key="summary"),
    #     Queue('school', Exchange('school') ,routing_key="school"),
    #     Queue('contest', Exchange('contest'), routing_key="contest"),
    #     # Queue('grade', Exchange('grade'), routing_key='grade'),
    #     # Queue('student', Exchange('student'), routing_key='student'),
    #     # Queue('test', Exchange('test'), routing_key = 'test'),
    #     Queue('user', Exchange('user'), routing_key='user'),
    #     Queue('user_per_day', Exchange('user_per_day'), routing_key = 'user_per_day')
    #      # Queue('math', Exchange('math'), routing_key='math')
    # )
    # celery_routes = {
    #     # 'tasks.celery_task.SummaryTask' : {'queue': 'summary', 'routing_key':'summary'},
    #     'tasks.celery_task.SchoolTask' : {'queue': 'school', 'routing_key':'school'},
    #     'tasks.celery_task.ContestTask': {'queue': 'contest', 'routing_key': 'contest'},
    #     # 'tasks.celery_task.GradeTask': {'queue': 'grade', 'routing_key': 'grade'},
    #     # 'tasks.celery_task.StudentTask': {'queue': 'student', 'routing_key': 'student'},
    #     # 'tasks.celery_test.TestTask': {'queue': 'test', 'routing_key': 'test'},
    #     'tasks.celery_test.UserTask': {'queue': 'user', 'routing_key': 'user'},
    #     'tasks.celery_test.UserPerDayTask': {'queue': 'user_per_day', 'routing_key': 'user_per_day'},
    #     # 'tasks.celery_test.Math': {'queue': 'math', 'routing_key': 'math'}
    # }
    #


sales_celery.config_from_object(Config)

#user task
per_day_task = PerDayTask()
sales_celery.register_task(per_day_task)
# sales_celery.send_task('tasks.celery_per_day_task.PerDayTask')


per_day_exercise_images = PerDaySubTask_IMAGES()
sales_celery.register_task(per_day_exercise_images)
# sales_celery.send_task('tasks.celery_per_day_task.PerDaySubTask_IMAGES')

per_day_guardian = PerDaySubTask_GUARDIAN()
sales_celery.register_task(per_day_guardian)
sales_celery.send_task('tasks.celery_per_day_task.PerDaySubTask_GUARDIAN')

per_day_payments = PerDaySubTask_PAYMENTS()
sales_celery.register_task(per_day_payments)
# sales_celery.send_task('tasks.celery_per_day_task.PerDaySubTask_PAYMENTS')

per_day_schools = PerDayTask_SCHOOL()
sales_celery.register_task(per_day_schools)
# sales_celery.send_task('tasks.celery_per_day_task.PerDayTask_SCHOOL')

per_day_users_number = PerDaySubTask_USERS()
sales_celery.register_task(per_day_users_number)
# sales_celery.send_task('tasks.celery_per_day_task.PerDaySubTask_USERS')

per_day_valid_exercise_word = PerDayTask_VALIDCONTEST()
sales_celery.register_task(per_day_valid_exercise_word)
# sales_celery.send_task('tasks.celery_per_day_task.PerDayTask_VALIDCONTEST')

per_day_valid_reading = PerDayTask_VALIDREADING()
sales_celery.register_task(per_day_valid_reading)
# sales_celery.send_task('tasks.celery_per_day_task.PerDayTask_VALIDREADING')

