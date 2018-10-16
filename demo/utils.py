import asyncio
from exceptions import LoginError

import schema

import aiohttp.web
from configs import (THEMIS_SYSTEM_URL, UC_SYSTEM_URL, permissionAppKey,
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


@catch_unknown_error(LoginError, 'Unknown error:')
async def get_permission(request: aiohttp.web.Request):
    """Get permission from UC system."""

    async def get_user_base_info(_session, _cookie):
        async with _session.get(UC_SYSTEM_URL + '/getLoginStatus', headers={'Cookie': _cookie}) as resp:
            resp_as_json = await resp.json()
            success_resp_as_json = schema.Schema({
                'status': 0,
                'data':
                    {
                        'isLogin': bool,
                        'userId': int
                    }
            }, ignore_extra_keys=True).validate(resp_as_json)

            return success_resp_as_json

    async def get_all_permission_info(_session, _cookie, _user_id: int):
        async with _session.get(THEMIS_SYSTEM_URL + '/permission/getListByUserId',
                                headers={'Cookie': _cookie}, params=(('appKey', permissionAppKey),
                                                                     ('appSecret', permissionAppSecret),
                                                                     ('userId', str(_user_id)))) as resp:
            resp_as_json = await resp.json()
            success_resp_as_json = schema.Schema({
                'status': 0,
                'data': list
            }, ignore_extra_keys=True).validate(resp_as_json)

            return success_resp_as_json

    http_session = request.app['http_session']
    cookie = request.headers.get('Cookie')
    if cookie is None:
        raise LoginError('Can not find header "Cookie".')

    success_uc_resp_as_json = await get_user_base_info(http_session, cookie)
    user_id = success_uc_resp_as_json['data']['userId']
    success_themis_resp_as_json = await get_all_permission_info(http_session, cookie, user_id)

    if request.get('user_info') is None:
        request['user_info'] = {
            'used_id': success_uc_resp_as_json['data']['userId'],
            'is_login': success_uc_resp_as_json['data']['isLogin'],
            'permissions': success_themis_resp_as_json['data']
        }
    else:
        request['user_info']['user_id'] = success_uc_resp_as_json['data']['userId']
        request['user_info']['is_login'] = success_uc_resp_as_json['data']['isLogin']
        request['user_info']['permissions'] = success_themis_resp_as_json['data']
