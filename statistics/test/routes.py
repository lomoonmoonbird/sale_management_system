import statistics.test.api
from aiohttp.web import UrlDispatcher


def register(router: UrlDispatcher):
    router.add_get('/api/sta', statistics.test.api.index)
