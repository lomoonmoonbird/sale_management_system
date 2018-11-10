import demo.demo_api
import demo.model
from aiohttp.web import UrlDispatcher

from user.api import User
user = User()

def register(router: UrlDispatcher):
    prefix= '/api'
    router.add_post(prefix + '/user', user.create_account)
    router.add_get(prefix + '/user/profile', user.profile)
    router.add_post(prefix + '/instance/area', user.add_area)
    router.add_post(prefix + '/instance/area/user', user.add_area_user)
    router.add_get(prefix + '/instance/area/channel', user.get_one_area_channels)
    router.add_post(prefix + '/instance/area/channel', user.add_area_channel)
    router.add_post(prefix + '/instance/channel/user', user.add_channel_user)
    router.add_get(prefix + '/instance/channels', user.get_channels)
    router.add_get(prefix + '/instance/areas', user.get_areas)
    router.add_post(prefix + '/instance/area/channel/delete', user.del_area) #删除大区渠道
    router.add_post(prefix + '/instance/area/delete', user.del_area)  # 删除大区
    router.add_post(prefix + '/instance/channel/user', user.add_channel_user)#创建渠道账号
    router.add_post(prefix + '/instance/market/user', user.add_market_user)  # 创建市场账号
    router.add_get(prefix + '/instance/area/user/channel', user.get_area_user_channels)#获取用户所在大区渠道
