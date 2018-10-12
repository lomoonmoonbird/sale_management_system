"""
aapi.middlewares
~~~~~~~~~~~~~~~~
This module provides middleware that are used for handle error, log access, etc.
"""
import asyncio

from aiohttp.web import middleware
from loggings import logger
from utils import json_response


@middleware
async def error_handle_middleware(request, handler):
    """This middleware will catch all exceptions and return this exception's info."""
    try:
        resp = await handler(request)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        error_name = e.__class__.__name__
        error_info = "'" + error_name + "'" + ' : ' + str(e)
        # TODO ADD LOG HERE.
        if hasattr(e, 'status_code'):
            logger.info(error_info)
            code = e.status_code
            resp = json_response({'code': code, 'data': error_info}, status=code)
        else:
            logger.warning(error_info)
            resp = json_response(
                {'code': 500, 'data': 'An unexpected exception ' + error_info}, status=500)
    return resp
