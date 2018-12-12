from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.export.global_month_week_export_api import GlobalExportReport
from statistics.export.area_month_week_export_api import AreaExportReport
from statistics.export.channel_month_week_export_api import ChannelExportReport
from statistics.export.clazz_export_api import ClazzExportReport
from statistics.export.school_export_api import SchoolExportReport
from statistics.export.grade_export_api import GradeExportReport
from statistics.export.date_range_export_api import DateRangeExport

globalexport = GlobalExportReport()
area_export = AreaExportReport()
channel_export = ChannelExportReport()
clazz_export = ClazzExportReport()
school_export = SchoolExportReport()
grade_export = GradeExportReport()
daterange_export = DateRangeExport()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/report/month', globalexport.month)
    router.add_get('/api/stat/report/week', globalexport.week)
    router.add_get('/api/stat/report/area/month', area_export.month)
    router.add_get('/api/stat/report/area/week', area_export.week)
    router.add_get('/api/stat/report/channel/month', channel_export.month)
    router.add_get('/api/stat/report/channel/week', channel_export.week)
    router.add_get('/api/stat/report/clazz/pay', clazz_export.clazz_pay_export)
    router.add_get('/api/stat/report/school/clazz/contest', school_export.contest_related)
    router.add_get('/api/stat/report/grade/clazz/contest', grade_export.contest_related)
    router.add_get('/api/stat/report/school/clazz/guardian', school_export.guardian_related)
    router.add_get('/api/stat/report/grade/clazz/guardian', grade_export.guardian_related)
    router.add_get('/api/stat/report/daterange/global/arealist', daterange_export.area_list_export)
    router.add_get('/api/stat/report/daterange/area/channelist', daterange_export.channel_list_export)
    router.add_get('/api/stat/report/daterange/channel/marketlist', daterange_export.market_list_export)
