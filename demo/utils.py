import asyncio
from exceptions import LoginError, PermissionError, UserBannedError
from functools import wraps

import schema

import aiohttp.web
from configs import (THEMIS_SYSTEM_OPEN_URL, UC_SYSTEM_API_URL, permissionAppKey,
                     permissionAppSecret)


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
                    print (resp_as_json)
                    success = schema.Schema({
                        'status': 0,
                        'data':
                            {
                                'isLogin': True,
                                'userId': int
                            }
                    }, ignore_extra_keys=True).is_valid(resp_as_json)
                    print (_cookie)
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
                    raise UserBannedError("User Banned")
            else:
                raise LoginError("User not exists")

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
