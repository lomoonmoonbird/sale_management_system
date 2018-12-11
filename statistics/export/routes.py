from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.export.global_month_week_export_api import GlobalExportReport
from statistics.export.area_month_week_export_api import AreaExportReport
from statistics.export.channel_month_week_export_api import ChannelExportReport
from statistics.export.clazz_export_api import ClazzExportReport
from statistics.export.school_export_api import SchoolExportReport

globalexport = GlobalExportReport()
area_export = AreaExportReport()
channel_export = ChannelExportReport()
clazz_export = ClazzExportReport()
school_export = SchoolExportReport()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/report/month', globalexport.month)
    router.add_get('/api/stat/report/week', globalexport.week)
    router.add_get('/api/stat/report/area/month', area_export.month)
    router.add_get('/api/stat/report/area/week', area_export.week)
    router.add_get('/api/stat/report/channel/month', channel_export.month)
    router.add_get('/api/stat/report/channel/week', channel_export.week)
    router.add_get('/api/stat/report/clazz/pay', clazz_export.clazz_pay_export)
    router.add_get('/api/stat/report/school/clazz/contest', school_export.contest_related)
