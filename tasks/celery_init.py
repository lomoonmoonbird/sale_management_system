#! python3.6
# --*-- coding: utf-8 --*--

"""
celery任务队列初始化
"""

from celery import Celery
from kombu import Queue, Exchange
from celery.schedules import crontab
import configs
from tasks.celery_task_grade import  GradeTask
from tasks.celery_task_summary import SummaryTask
from tasks.celery_task_school import SchoolTask
from tasks.celery_task_student import StudentTask

sales_celery = Celery("Sales")

class Config():
    broker_url = configs.BROKER_URL
    result_backend = configs.RESULT_BACKEND
    loglevel = 'DEBUG'
    timezone='Asia/Shanghai'
    enable_utc=True
    installed_apps  = ('tasks')
    # beat_schedule= {
    #     # "summary": {
    #     #     "task": "tasks.celery_task.SummaryTask",
    #     #     "schedule": 10,
    #     # },
    #     #  "school": {
    #     #     "task": "tasks.celery_task.SchoolTask",
    #     #     "schedule": 20,
    #     # },
    #     # 'grade': {
    #     #     "task": "tasks.celery_task.GradeTask",
    #     #     "schedule": 30
    #     # }
    #     # "student": {
    #     #     "task": "tasks.celery_task.StudentTask",
    #     #     "schedule": 10,
    #     # }
    # }
    # task_default_queue = 'summary'
    # # task_queues = (
    # #     Queue('summary', Exchange('summary') ,routing_key="summary"),
    #
    # # )
    # # task_routes = {
    # #         'tasks.celery_task.SummaryTask': {
    # #             'queue': 'summary',
    # #             'routing_key': 'summary'
    # #         }
    # #     }
    #
    # celery_queues = (
    #      Queue('summary', Exchange('summary') ,routing_key="summary"),
    #      Queue('school', Exchange('school') ,routing_key="school"),
    #      Queue('grade', Exchange('grade'), routing_key='grade'),
    #      Queue('student', Exchange('student'), routing_key='student')
    # )
    # celery_routes = {
    #     'tasks.celery_task.SummaryTask' : {'queue': 'summary', 'routing_key':'summary'},
    #     'tasks.celery_task.SchoolTask' : {'queue': 'school', 'routing_key':'school'},
    #     'tasks.celery_task.GradeTask': {'queue': 'grade', 'routing_key': 'grade'},
    #     'tasks.celery_task.StudentTask': {'queue': 'student', 'routing_key': 'student'}
    # }



sales_celery.config_from_object(Config)

summary_task = SummaryTask()
school_task = SchoolTask()
# grade_task = GradeTask()
# student_task = StudentTask()
summary_task = sales_celery.register_task(summary_task)
school_task = sales_celery.register_task(school_task)
# grade_task = sales_celery.register_task(grade_task)
# student_task = sales_celery.register_task(student_task)


# sales_celery.send_task('tasks.celery_task.SummaryTask')
sales_celery.send_task('tasks.celery_task.SchoolTask')