#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""

from celery import Celery
from kombu import Queue, Exchange
from celery.schedules import crontab
import configs
from tasks.celery_task import SummaryTask, Math


sales_celery = Celery("Sales")

class Config():
    broker_url = configs.BROKER_URL
    result_backend = configs.RESULT_BACKEND
    loglevel = 'DEBUG'
    timezone='Asia/Shanghai'
    enable_utc=True
    installed_apps  = ('tasks')
    beat_schedule= {
        "summary": {
            "task": "tasks.celery_task.SummaryTask",
            "schedule": 10,
        },
    }
    task_default_queue = 'summary'
    # task_queues = (
    #     Queue('summary', Exchange('summary') ,routing_key="summary"),

    # )
    # task_routes = {
    #         'tasks.celery_task.SummaryTask': {
    #             'queue': 'summary',
    #             'routing_key': 'summary'
    #         }
    #     }

    celery_queues = (
         Queue('summary', Exchange('summary') ,routing_key="summary"),
         Queue('test2', Exchange('test2') ,routing_key="test2"),
    )   
    celery_routes = {
        'tasks.celery_task.SummaryTask' : {'queue': 'summary', 'routing_key':'summary'},
        'tasks.celery_task.Math' : {'queue': 'test2', 'routing_key':'test2'}
    }



sales_celery.config_from_object(Config)

school_task = SummaryTask()
m = Math()
school_task = sales_celery.register_task(school_task)
m = sales_celery.register_task(m)