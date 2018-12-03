
from aiohttp.web import UrlDispatcher

from user.api import User
user = User()

def register(router: UrlDispatcher):
    prefix= '/api'
    router.add_get(prefix + '/user/profile', user.profile)#用户资料
    router.add_post(prefix + '/instance/area', user.add_area) #创建大区
    router.add_post(prefix + '/instance/area/user', user.add_area_user) #创建大区用户
    router.add_post(prefix + '/instance/area/user/delete', user.del_area_user) #删除大区用户
    router.add_get(prefix + '/instance/area/channel', user.get_one_area_channels) #获取某一个大区渠道
    router.add_post(prefix + '/instance/area/channel', user.add_area_channel) #添加大区渠道
    router.add_post(prefix + '/instance/channel/user', user.add_channel_user) #创建大区账号
    router.add_post(prefix + '/instance/channel/user/delete', user.del_channel_user)  # 删除渠道账号
    router.add_get(prefix + '/instance/channels', user.get_channels)#获取渠道
    router.add_get(prefix + '/instance/areas', user.get_areas) #获取大区
    router.add_post(prefix + '/instance/area/channel/delete', user.del_area) #删除大区渠道
    router.add_post(prefix + '/instance/area/delete', user.del_area)  # 删除大区
    router.add_post(prefix + '/instance/channel/user', user.add_channel_user)#创建渠道账号
    router.add_post(prefix + '/instance/market/user', user.add_market_user)  # 创建市场账号
    router.add_post(prefix + '/instance/market/user/update', user.update_marker_user)  # 更新市场账号信息
    router.add_post(prefix + '/instance/market/user/delete', user.del_market_user)  # 删除市场账号
    router.add_get(prefix + '/instance/area/user/channel', user.get_area_user_channels)#获取用户所在大区渠道
    router.add_get(prefix + '/instance/market/users', user.get_market_user)  # 获取市场账号
