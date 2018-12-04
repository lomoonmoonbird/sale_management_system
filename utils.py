"""
aapi.utils
~~~~~~~~~~
This module provides utility functions that are used for handle http request.
"""
import asyncio
import re
from functools import wraps

import schema

import ujson
from aiohttp import web
from aiohttp.helpers import sentinel

from configs import UC_SYSTEM_API_URL, THEMIS_SYSTEM_OPEN_URL, permissionAppKey, permissionAppSecret
from exceptions import LoginError, PermissionError
from loggings import logger
import json
import datetime
import functools
from bson import ObjectId

__all__ = ('json_response', 'text_response', 'get_json', 'get_params', 'get_cookie')

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime)):
            return str(obj)
        if isinstance(obj, (ObjectId)):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

json_dumps = functools.partial(json.dumps,cls=CustomEncoder)
def json_response(data=sentinel, *, text=None, body=None, status=200,
                  reason=None, headers=None, content_type='application/json',
                  dumps=json_dumps):
    if data is not sentinel:
        if text or body:
            raise ValueError(
                "only one of data, text, or body should be specified"
            )
        else:
            text = dumps(data)
    return web.Response(text=text, body=body, status=status, reason=reason,
                        headers=headers, content_type=content_type)


def text_response(text: str, *, status=200,
                  reason=None, headers=None, content_type='text/plain'):
    return web.Response(text=text, status=status, reason=reason,
                        headers=headers, content_type=content_type)


async def get_json(_request: web.Request, *, schema_format=None, ignore_extra_keys=True) -> dict:
    """Get json body if possible, return a dict which loads from body."""
    if _request.method != 'POST' or not _request.can_read_body:
        params = dict()
    else:
        try:
            params = await _request.json(loads=ujson.loads)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f'Invalid json parsed from {_request.remote} {e}')
            params = dict()

    if schema_format is not None:
        return schema.Schema(schema_format, ignore_extra_keys=ignore_extra_keys).validate(params)
    else:
        return params


async def get_params(_request, *, schema_format=None, ignore_extra_keys=True, unique=True) -> dict:
    """Get url parameters if possible."""
    if unique:
        params = dict(_request.query)
    else:
        params = dict()
        for k, v in _request.query.items():
            if k not in params:
                params[k] = [v]
            else:
                params[k].append(v)

    if schema_format is not None:
        return schema.Schema(schema_format, ignore_extra_keys=ignore_extra_keys).validate(params)
    else:
        return params


def get_cookie(_request, *, cookie_header='Cookie') -> dict:
    cookie = _request.headers.get(cookie_header)
    try:
        cookie_as_dict = _DecodeCookie(cookie).get_result()
    except Exception as e:
        logger.warning(f'Invalid cookie parsed from {_request.remote} {e}')
        cookie_as_dict = dict()
    return cookie_as_dict


def convert_value(_value, _type=None):
    """Convert str or bytes to (int,float,bool)"""
    return _ConvertValue(_value, _type=_type).result


class _ConvertValue:
    IS_INTEGER_PATTERN_BYTES = re.compile(b'[0-9]+')
    IS_INTEGER_PATTERN_STR = re.compile('[0-9]+')
    IS_FLOAT_PATTERN_BYTES = re.compile(b'[0-9]+\.[0-9]+')
    IS_FLOAT_PATTERN_STR = re.compile('[0-9]+\.[0-9]+')
    BOOL_TRUE_COLLECTIONS_STR = ('True', 'true')
    BOOL_TRUE_COLLECTIONS_BYTES = (b'True', b'true')
    BOOL_FALSE_COLLECTIONS_STR = ('False', 'false')
    BOOL_FALSE_COLLECTIONS_BYTES = (b'False', b'false')

    def __init__(self, _value, _type=None):
        self._type = type(_value) if _type is None else _type
        self._value = _value
        self.result = None

        if self.is_integer():
            self.result = int(self._value)
        elif self.is_float():
            self.result = float(self._value)
        elif self.is_bool():
            # Here means result already generate from self.is_bool()
            pass
        else:
            self.result = self._value

    def is_integer(self) -> bool:
        if self._type is str:
            return self.IS_INTEGER_PATTERN_STR.fullmatch(self._value) is not None
        elif self._type is bytes:
            return self.IS_INTEGER_PATTERN_BYTES.fullmatch(self._value) is not None

        return False

    def is_float(self) -> bool:
        if self._type is str:
            return self.IS_FLOAT_PATTERN_STR.fullmatch(self._value) is not None
        elif self._type is bytes:
            return self.IS_FLOAT_PATTERN_BYTES.fullmatch(self._value) is not None

        return False

    def is_bool(self) -> bool:
        if self._type is str:
            if self._value in self.BOOL_TRUE_COLLECTIONS_STR:
                self.result = True
                return True
            elif self._value in self.BOOL_FALSE_COLLECTIONS_STR:
                self.result = False
                return True

        elif self._type is bytes:
            if self._value in self.BOOL_TRUE_COLLECTIONS_BYTES:
                self.result = True
                return True
            elif self._value in self.BOOL_FALSE_COLLECTIONS_BYTES:
                self.result = False
                return True
        return False


class _DecodeCookie:
    COOKIE_PATTERN = re.compile(r'\s?([A-Za-z_0-9]+)\s?=\s?([A-za-z_0-9]+)\s?;?\s?')

    def __init__(self, cookie: str):
        self.cookie = cookie

    def get_result(self) -> dict:
        if not isinstance(self.cookie, str):
            return dict()
        return {i[0]: convert_value(i[1], _type=str)
                for i in self.COOKIE_PATTERN.findall(self.cookie)}


def catch_unknown_error(error, error_info):
    def _wrapper(func):
        async def _inner_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not hasattr(e, 'status_code'):
                    raise error(error_info + str(e))
                else:
                    raise

        return _inner_wrapper

    return _wrapper


def validate_permission():
    def outer_wrapper(func):
        @wraps(func)
        @catch_unknown_error(LoginError, 'Login error:')
        async def inner_wrapper(*args, **kwargs):

            async def get_user_id(_session, _cookie) -> int:
                async with _session.get(UC_SYSTEM_API_URL + '/getLoginStatus', headers={'Cookie': _cookie}) as resp:
                    resp_as_json = await resp.json()
                    success = schema.Schema({
                        'status': 0,
                        'data':
                            {
                                'isLogin': True,
                                'userId': int
                            }
                    }, ignore_extra_keys=True).is_valid(resp_as_json)
                    if not success:
                        raise LoginError('Invalid cookie info.')
                    return resp_as_json['data']['userId']

            async def _validate_permission(_session, _user_id: int, permission: str) -> bool:
                async with _session.post(THEMIS_SYSTEM_OPEN_URL + '/grant/validateByUserId',
                                         json={'appKey': permissionAppKey,
                                               'appSecret': permissionAppSecret,
                                               'userId': _user_id,
                                               'name': permission}, ssl=False) as resp:
                    resp_as_json = await resp.json()
                return schema.Schema({
                    'status': 0,
                    'data': {
                        'hasGranted': True
                    }
                }, ignore_extra_keys=True).is_valid(resp_as_json)
            request = args[0] if hasattr(args[0], 'app') else args[1]  # type: aiohttp.web.Request
            http_session = request.app['http_session']
            cookie = request.headers.get('Cookie')

            if cookie is None:
                raise LoginError('Can not find header "Cookie".')

            user_id = await get_user_id(http_session, cookie)
            u = await request.app['mongodb']['sales']['sale_user'].find_one({"user_id": str(user_id)})
            if u:
                if u.get('status', 0) != 1:
                    raise PermissionError("User Banned")
            else:
                raise PermissionError("Not Authorized")

            request_uri = ' '.join((request.method, request.path))
            if not await _validate_permission(http_session, user_id, request_uri):
                raise PermissionError('User can not access this API!')


            if request.get('user_info') is None:
                request['user_info'] = {
                    'user_id': user_id
                }
            else:
                request['user_info']['user_id'] = user_id
            request['user_info']['area_id'] = u.get("area_id", '')
            request['user_info']['channel_id'] = u.get("channel_id", '')
            request['user_info']['global_id'] = u.get("global_id", '')
            request['user_info']['market_id'] = u.get("market_id", '')
            request['user_info']['school_id'] = u.get("school_id", '')
            request['user_info']['role_id'] = int(u.get("role_id", -1))
            request['user_info']['instance_role_id'] = int(u.get("instance_role_id", -1))
            print (request['user_info'])
            return await func(*args, **kwargs)

        return inner_wrapper

    return outer_wrapper