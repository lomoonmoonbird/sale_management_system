"""
aapi.middlewares
~~~~~~~~~~~~~~~~
This module provides middleware that are used for handle error, log access, etc.
"""
import asyncio

from aiohttp.web import middleware
from loggings import logger
from utils import json_response
from errorcodes import ErrorCode

@middleware
async def error_handle_middleware(request, handler):
    """This middleware will catch all exceptions and return this exception's info."""
    try:
        resp = await handler(request)
    except asyncio.CancelledError:
        raise

    except Exception as e:
        import traceback
        traceback.print_exc()
        error_name = e.__class__.__name__
        error_info = "'" + error_name + "'" + ' : ' + str(e)
        # TODO ADD LOG HERE.

        if hasattr(e, 'status_code'):
            logger.info(error_info)
            code = e.status_code
            http_code = 200

            if hasattr(e, 'http_code'):
                http_code = e.http_code.value
            resp = json_response({'code': code.value, 'message': error_info, 'data': {}}, status=http_code)
        else:
            logger.warning(error_info)
            resp = json_response(
                {'code': ErrorCode.INTERNALERROR.value, 'message': 'An unexpected exception ' + error_info, 'data': {}}, status=200)
    return resp
