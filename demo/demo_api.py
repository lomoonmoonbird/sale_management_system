import asyncio
from utils import *
from aiohttp.web import Request


async def index(req: Request):
    print(await get_params(req))
    print(await get_params(req, unique=False))
    print(await get_json(req))
    print(get_cookie(req))
    return text_response('666')
