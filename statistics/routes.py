import statistics.school.api
from aiohttp.web import UrlDispatcher

from statistics.overview_api import Overview
from statistics.area_list_api import AreaList
from statistics.channel_list_api import ChannelList

overview = Overview()
arealist = AreaList()
channellist = ChannelList()

def register(router: UrlDispatcher):
    router.add_get('/api/stat/global/overview', overview.overview)
    router.add_get('/api/stat/area/overview', overview.overview)
    router.add_get('/api/stat/area/list', arealist.area_list)
    router.add_get('/api/stat/channel/list', channellist.channel_list)
