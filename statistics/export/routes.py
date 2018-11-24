from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.export.global_month_week_export_api import GlobalExportReport
from statistics.export.area_month_week_export_api import AreaExportReport
from statistics.export.channel_month_week_export_api import ChannelExportReport

globalexport = GlobalExportReport()
area_export = AreaExportReport()
channel_export = ChannelExportReport()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/report/month', globalexport.month)
    router.add_get('/api/stat/report/week', globalexport.week)
    router.add_get('/api/stat/report/area/month', area_export.month)
    router.add_get('/api/stat/report/area/week', area_export.week)
    router.add_get('/api/stat/report/channel/month', channel_export.month)
    router.add_get('/api/stat/report/channel/week', channel_export.week)
