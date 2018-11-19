#! python3.6
# --*-- coding: utf-8 --*--

"""
枚举类型
"""

from enum import Enum

class Roles(Enum):
    """
    销管对应的实例角色
    """
    SUPER = 0 #超级管理员
    GLOBAL = 1 #总部
    AREA = 2  #大区
    CHANNEL = 3 #渠道
    MARKET = 4 #市场
    SCHOOL = 5 #学校

class PermissionRole(Enum):
    """
    对应themis角色的id
    """
    SUPER = 37 #超级管理员id
    GLOBAL= 35 #总部id
    AREA = 33 #大区id
    CHANNEL = 34 #渠道id
    MARKET = 36 #市场id