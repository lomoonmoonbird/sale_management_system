# --*-- coding: utf-8 --*--

from enum import Enum, unique

@unique
class ErrorCode(Enum):
    OK = 0
    USEREXIST = 3001
    CREATEUSERFAIL = 3002
    USERBANNED = 3003
    DELETEERROR = 3004
    CHANNELNOTEXIST = 3005
    InstanceExist = 3006



    HTTP200 = 200
    PARAMETERERROR = 3900
    UNAUTHENTICATED = 3901
    INTERNALERROR = 3902
    UNAUTHORIZED = 3903
    DATAFORBIDDEN = 3904