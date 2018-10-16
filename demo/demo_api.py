import asyncio

from aiohttp.web import Request
from demo.utils import get_permission
from utils import *


async def index(req: Request):
    await get_permission(req)
    print(await get_params(req))
    print(await get_params(req, unique=False))
    print(await get_json(req))
    print(get_cookie(req))
    return text_response('666')
