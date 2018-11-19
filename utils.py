"""
aapi.utils
~~~~~~~~~~
This module provides utility functions that are used for handle http request.
"""
import asyncio
import re

import schema

import ujson
from aiohttp import web
from aiohttp.helpers import sentinel
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
