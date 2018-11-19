import demo.demo_api
import demo.model
from aiohttp.web import UrlDispatcher

from school.api import School
school = School()

def register(router: UrlDispatcher):
    prefix= '/api'
    router.add_post(prefix + '/instance/market/school', school.add_school_market) #绑定学校市场
    router.add_post(prefix + '/instance/market/school/delete', school.del_market_school) #解绑学校市场
    router.add_get(prefix + '/instance/market/schools', school.get_market_school)  # 获取学校和市场绑定关系
    router.add_get(prefix + '/instance/market/school/spareusers', school.get_spare_market_user_for_school)  # 获取可分配学校的市场用户
