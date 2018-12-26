from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.channel_list_api import ChannelList
from statistics.market_list import MarketList
from statistics.detail import AreaDetail, ChannelDetail, GradeDetail, ClazzDetail, SchoolDetail, MarketDetail

overview = Overview()
arealist = AreaList()
channellist = ChannelList()
areadetail = AreaDetail()
channeldetail = ChannelDetail()
marketlist = MarketList()
gradedetail = GradeDetail()
clazzdetail = ClazzDetail()
schooldetail = SchoolDetail()
marketdetail = MarketDetail()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/global/overview', overview.overview) #统计 总部总览
    router.add_get('/api/stat/area/overview', overview.overview) #统计 大区总览
    router.add_get('/api/stat/channel/overview', overview.overview) #统计 渠道总览

    router.add_get('/api/stat/area/list', arealist.area_list) #统计 总部大区列表
    router.add_get('/api/stat/channel/list', channellist.channel_list) #统计 大区渠道列表
    router.add_get('/api/stat/market/list', marketlist.market_list) #统计 渠道市场列表

    router.add_get('/api/stat/detail/area/overview', areadetail.overview) #统计 某大区总览
    router.add_get('/api/stat/detail/area/channelist', areadetail.channel_list) #统计 某大区渠道列表
    router.add_get('/api/stat/detail/channel/overview', channeldetail.overview) #统计 某渠道总览
    router.add_get('/api/stat/detail/channel/marketlist', channeldetail.market_list) #统计 某渠道市场列表
    router.add_get('/api/stat/detail/channel/allschool', channeldetail.school_list_detail)  # 统计 某渠道所有学校统计列表
    router.add_get('/api/stat/detail/market/schoolist', marketdetail.school_list) #统计 某市场学校列表
    router.add_get('/api/stat/detail/area/schoolist', areadetail.school_list)  # 统计 大区学校列表
    router.add_get('/api/stat/detail/channel/schoolist', channeldetail.school_list)  # 统计 渠道学校列表

    router.add_get('/api/stat/detail/grade/gradelist', gradedetail.grade_list) #统计 年级详情
    router.add_get('/api/wap/stat/detail/grade/gradelist', gradedetail.wap_grade_list)  # 移动端 统计 年级详情
    router.add_get('/api/stat/detail/clazz/clazzlist', clazzdetail.clazz_list) #统计 班级详情
    router.add_get('/api/stat/detail/school/clazzlist', schooldetail.clazz_list) #统计 学校班级详情
