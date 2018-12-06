"""
aapi.exceptions
~~~~~~~~~~~~~~~
"""

from errorcodes import ErrorCode

class InternalError(Exception):
    """Internal exception."""
    status_code = ErrorCode.INTERNALERROR


class RequestError(Exception):
    """Invalid request."""
    status_code = ErrorCode.PARAMETERERROR

class PermissionError(Exception):
    status_code = ErrorCode.UNAUTHORIZED


class LoginError(RequestError):
    """Invalid login info."""
    status_code = ErrorCode.UNAUTHENTICATED


class UserExistError(RequestError):
    """
    用户已经存在
    """
    status_code = ErrorCode.USEREXIST

class InstanceExistError(RequestError):
    """
    实例已经存在
    """
    status_code = ErrorCode.InstanceExist

class CreateUserError(RequestError):
    status_code = ErrorCode.CREATEUSERFAIL

class UserBannedError(RequestError):
    status_code = ErrorCode.USERBANNED

class DELETEERROR(RequestError):
    status_code = ErrorCode.DELETEERROR

class ChannelNotExist(RequestError):
    status_code = ErrorCode.CHANNELNOTEXIST