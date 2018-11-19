import statistics.school.api
from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList

overview = Overview()
arealist = AreaList()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/global/overview', overview.overview)
    router.add_get('/api/stat/area/list', arealist.area_list)
