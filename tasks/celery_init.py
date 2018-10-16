#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""

from celery import Celery
from celery.schedules import crontab
import configs
from tasks.celery_task import SchoolTask


sales_celery = Celery("Sales")

class Config():
    broker_url = configs.BROKER_URL
    result_backend = configs.RESULT_BACKEND
    loglevel = 'DEBUG'
    # timezone='Asia/Shanghai',
    enable_utc=True,
    installed_apps  = ('tasks')
    beat_schedule={
        "morning_msg_1": {
            "task": "tasks.celery_task.SchoolTask",
            "schedule": 10,
        },
    }


sales_celery.config_from_object(Config)

school_task = SchoolTask()
school_task = sales_celery.register_task(school_task)
