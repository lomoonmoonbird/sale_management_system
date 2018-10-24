from aiohttp.web import Request
from demo.utils import validate_permission
from utils import get_cookie, get_json, get_params, text_response


@validate_permission()
async def index(req: Request):
    # print(await get_params(req))
    # print(await get_params(req, unique=False))
    # print(await get_json(req))
    # print(get_cookie(req))
    # print(req.path)
    # print(req.method)
    # print(' '.join((req.method, req.path)))

    return text_response('666')
