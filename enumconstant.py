#! python3.6
# --*-- coding: utf-8 --*--

"""
枚举类型
"""

from enum import Enum
from configs import  PERMISSION_SUPER ,PERMISSION_GLOBAL, PERMISSION_AREA, PERMISSION_CHANNEL, PERMISSION_MARKET
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
    SUPER = PERMISSION_SUPER #超级管理员id
    GLOBAL= PERMISSION_GLOBAL #总部id
    AREA = PERMISSION_AREA #大区id
    CHANNEL = PERMISSION_CHANNEL #渠道id
    MARKET = PERMISSION_MARKET #市场id