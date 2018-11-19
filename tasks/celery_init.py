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
from tasks.celery_task_school_backup import SchoolTask
from tasks.celery_task_student import StudentTask
from tasks.celery_task_user import  PerDayTask, PerDaySubTask_IMAGES,PerDaySubTask_GUARDIAN, \
    PerDaySubTask_PAYMENTS, PerDaySubTask_PAYMENTS, PerDaySubTask_USERS, PerDayTask_SCHOOL, PerDayTask_VALIADCONTEST
from tasks.celery_test import TestTask
from tasks.celery_task_school import PerDayTask_SCHOOL_NUMBER

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
    task_default_queue = 'sale_defult_queue'
    task_queues = (
        # Queue('user', Exchange('user') ,routing_key="user"),
        Queue('user_per_day', Exchange('user_per_day'), routing_key="user_per_day"),
        # Queue('contest', Exchange('contest'), routing_key="contest"),
        # Queue('school', Exchange('school'), routing_key="school"),

    )
    task_routes = {
        # 'tasks.celery_task_user.UserTask': {
        #     'queue': 'user',
        #     'routing_key': 'user'
        # },
        'tasks.celery_task_user.PerDayTask': {
            'queue': 'user_per_day',
            'routing_key': 'user_per_day'
        },
        # 'tasks.celery_task_user.PerDaySubTask_IMAGES': {
        #     'queue': 'user_per_day_exercise_image',
        #     'routing_key': 'user_per_day_exercise_image'
        # },
        # 'tasks.celery_task_contest.ContestTask': {
        #     'queue': 'contest',
        #     'routing_key': 'contest'
        # },
        # 'tasks.celery_task_school.SchoolTask': {
        #     'queue': 'school',
        #     'routing_key': 'school'
        # }

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
sales_celery.send_task('tasks.celery_task_user.PerDayTask')
#
per_day_exercise_images = PerDaySubTask_IMAGES()
sales_celery.register_task(per_day_exercise_images)

per_data_guardian = PerDaySubTask_GUARDIAN()
sales_celery.register_task(per_data_guardian)

per_day_payments = PerDaySubTask_PAYMENTS()
sales_celery.register_task(per_day_payments)

per_day_schools = PerDayTask_SCHOOL()
sales_celery.register_task(per_day_schools)

per_day_users_number = PerDaySubTask_USERS()
sales_celery.register_task(per_day_users_number)

per_day_valid_exercise_word = PerDayTask_VALIADCONTEST()
sales_celery.register_task(per_day_valid_exercise_word)

#school
# school_number_per_day_task = PerDayTask_SCHOOL_NUMBER()
# sales_celery.register_task(school_number_per_day_task)
# sales_celery.send_task('tasks.celery_task_school.PerDayTask_SCHOOL_NUMBER')







#user per day task
# user_per_day_task = UserPerDayTask()
# sales_celery.register_task(user_per_day_task)
# sales_celery.send_task("tasks.celery_task_user.UserPerDayTask")
# # #contest task
# contest_task = ContestTask()
# sales_celery.register_task(contest_task)
# sales_celery.send_task('tasks.celery_task_contest.ContestTask')
# # # #school
# school_task = SchoolTask()
# sales_celery.register_task(school_task)
# sales_celery.send_task("tasks.celery_task_school.SchoolTask")


# summary_task = SummaryTask()
# school_task = SchoolTask()
# grade_task = GradeTask()
# student_task = StudentTask()
# summary_task = sales_celery.register_task(summary_task)
# school_task = sales_celery.register_task(school_task)
# grade_task = sales_celery.register_task(grade_task)
# student_task = sales_celery.register_task(student_task)


# sales_celery.send_task('tasks.celery_task.SummaryTask')
# sales_celery.send_task('tasks.celery_task.SchoolTask')

#test
# test_task = TestTask()
# test_task = sales_celery.register_task(test_task)
# test_task.apply_async()
# sales_celery.send_task('tasks.celery_test.TestTask')


# from tasks.celery_test import SubTestTask
# subtesttask = SubTestTask()
# sales_celery.register_task(subtesttask)
