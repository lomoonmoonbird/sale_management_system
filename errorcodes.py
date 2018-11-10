# --*-- coding: utf-8 --*--

from enum import Enum, unique

@unique
class ErrorCode(Enum):
    OK = 0
    USEREXIST = 1
    CREATEUSERFAIL = 2
    USERBANNED = 3
    DELETEERROR = 4


    HTTP200 = 200
    PARAMETERERROR = 400
    UNAUTHENTICATED = 401
    INTERNALERROR = 500
    UNAUTHORIZED = 403