#! python3.6
# --*-- coding: utf-8 --*--

"""
一些涉及统计的MYSQL数据表
"""

from sqlalchemy import Table, Column, Integer, String, MetaData, \
ForeignKey, Boolean, DateTime, SmallInteger, select
from sqlalchemy.dialects import mysql

metadata = MetaData()

#Hermes 考试上传的答题卡图片
as_hermes = Table('sigma_pool_as_hermes', metadata, 
                Column('uid', String(128), unique=True), #图片uid
                Column('exercise_id', Integer), #考试id
                Column('student_id', default=0, index=True), #学生id
                Column('status', Integer), #图片状态
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )


#school 学校
ob_school = Table('sigma_account_ob_school', metadata, 
                Column('uid', String(128), unique=True), #学校uid
                Column('full_name', String(64)), #学校全名
                Column('short_name', String(32)), #学校缩略名
                Column('admin_id', Integer), #管理员id
                Column('owner_id', Integer, default=0), #渠道账号id
                Column('expire', DateTime), #过期时间
                Column('vip_level', Integer, default=0), #客户重要程度 0 普通用户
                Column('pay_mode', SmallInteger, default=0), #0 按学校付费 1个人付费
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#group 年级 班级
ob_group = Table('sigma_account_ob_group', metadata, 
                Column('uid', String(64), unique=True), #年级uid
                Column('name', String(64)), #班级
                Column('grade', String(8)), #入学年份  当前时间与此字段计算几年级
                Column('school_id', Integer), #学校id
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#groupuser 用户和年级班级关系

ob_groupuser = Table('sigma_account_ob_groupuser', metadata, 
                Column('group_id', String), #年级id
                Column('user_id', String, index=True), #用户id
                Column('role_id', String), #角色
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#exercise 考试
ob_exercise = Table('sigma_exercise_ob_exercise', metadata, 
                Column('uid', String(128), unique=True), #考试uid
                Column('user_id', String(64)), #出卷人
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#wechat 微信绑定关系
ob_wechat = Table('sigma_account_ob_wechat', metadata, 
                Column('unionid', String(32), unique=True), #TODO 含义
                Column('openid', Integer, index=True, nullable=True), #微信openid
                Column('role_id', default=2, nullable=False), #角色分类
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )


#user 用户表 所有用户
us_user = Table('sigma_account_us_user', metadata, 
                Column('name', String(32)), #名称
                Column('uid', Integer(128), unique=True, index=True, nullable=True), #用户展示uid
                Column('openid', Integer(128), nullable=True), #微信openid
                Column('unionid', Integer(128), unique=True, index=True), #todo 含义
                Column('school_id', Integer, default=0, nullable=True), #学校id
                Column('role_id', SmallInteger, default=0), #角色id
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#wechat 用户和绑定微信关系
re_userwechat = Table('sigma_account_re_userwechat', metadata, 
                Column('user_id'), #用户id
                Column('wechat_id', Integer, index=True), #sigma_account_ob_wechat id
                Column('relationship', default=0, nullable=False), #该user_id和wechat_id的关系
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

#order 账单订单
ob_order = Table('sigma_pay_ob_order', metadata, 
                Column('uid', String(128), unique=True, index=True, nullable=True), #账单id
                Column('user_id', Integer, index=True, nullable=True), #购买者用户id
                Column('origin_amount', default=2, nullable=False), #订单原始金额
                Column('current_amount', Boolean, default=1), #订单当前金额
                Column('coupon_amount', DateTime), #使用优惠券之后价格，实际支付价格
                Column('status', default=2, nullable=False), #订单状态
                Column('available', Boolean, default=1), #是否可用
                Column('time_create', DateTime) #创建时间
                )

# u = as_hermes.insert().values(uid='mmb', exercise_id='moonmoonbird')
# f = as_hermes.select().where(as_hermes.c.uid > 1)
# # print (str(u))
# print (f.compile().statement.params)
# # print (f.compile(dialect=mysql.dialect()))
# # print(repr(as_hermes.c.uid == 'ed'))


# s = select([as_hermes]).where(as_hermes.c.uid > 1)
# s = as_hermes.select().where(as_hermes.c.uid  > 1)
# print (s.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}))