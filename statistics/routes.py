import statistics.school.api
from aiohttp.web import UrlDispatcher

from statistics.global_api import Overview
from statistics.area_api import AreaOverview

overview = Overview()
areaoverview = AreaOverview()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/global/overview', overview.overview)
    router.add_get('/api/stat/area/overview', areaoverview.overview)
