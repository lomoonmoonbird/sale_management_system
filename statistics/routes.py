from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.channel_list_api import ChannelList
from statistics.detail import AreaDetail, ChannelDetail, GradeDetail, ClazzDetail, SchoolDetail

overview = Overview()
arealist = AreaList()
channellist = ChannelList()
areadetail = AreaDetail()
channeldetail = ChannelDetail()
gradedetail = GradeDetail()
clazzdetail = ClazzDetail()
schooldetail = SchoolDetail()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/global/overview', overview.overview)
    router.add_get('/api/stat/area/overview', overview.overview)
    router.add_get('/api/stat/area/list', arealist.area_list)
    router.add_get('/api/stat/channel/list', channellist.channel_list)

    router.add_get('/api/stat/detail/area/overview', areadetail.overview)
    router.add_get('/api/stat/detail/area/channelist', areadetail.channel_list)
    router.add_get('/api/stat/detail/channel/overview', channeldetail.overview)
    router.add_get('/api/stat/detail/channel/marketlist', channeldetail.market_list)

    router.add_get('/api/stat/detail/grade/gradelist', gradedetail.grade_list)
    router.add_get('/api/stat/detail/clazz/clazzlist', clazzdetail.clazz_list)
    router.add_get('/api/stat/detail/school/clazzlist', schooldetail.clazz_list)
