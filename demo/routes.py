import demo.demo_api
import demo.model
from aiohttp.web import UrlDispatcher


def register(router: UrlDispatcher):
    router.add_get('/demo/', demo.demo_api.index)
    router.add_post('/demo/', demo.demo_api.index)
