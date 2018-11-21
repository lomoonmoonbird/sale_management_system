import statistics.school.api
from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.export.month_week_export_api import ExportReport


export = ExportReport()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/report/month', export.month)
    router.add_get('/api/stat/report/week', export.week)
