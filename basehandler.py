# --*-- coding: utf-8 --*--

"""
请求处理基类
"""


from utils import json_response
from aiohttp import ClientSession
from aiohttp.web import Response, StreamResponse
import ujson
import mimetypes
from datetime import datetime, timedelta, date

class BaseHandler():

    async def json_post_request(self, http_session:ClientSession, url:str, data={}, cookie='')->dict:
        """
        json post请求,并返回json对象
        :param http_session:
        :param url:
        :param data:
        :return:
        """
        header = {}
        header.update({"Content-Type": "application/json"})
        if cookie:
            header.update({"Cookie": cookie})
        resp = await http_session.post(url, data=data, headers=header)
        return await resp.json()


    def reply_ok(self, response_data={}, extra={}):
        data = {"code": 0, "message": "ok", "data": response_data}
        return json_response(data=data)

    async def replay_stream(self, byte_data,filename, request):
        resp = StreamResponse()
        resp.content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        disposition = 'filename="{}"'.format(filename+'.xlsx')
        if 'text' not in resp.content_type:
            disposition = 'attachment; ' + disposition

        resp.headers['CONTENT-DISPOSITION'] = disposition

        resp.content_length = len(byte_data)
        await resp.prepare(request)

        await resp.write(byte_data)
        await resp.write_eof()
        return resp


    def rounding(self, number):
        """
        四舍五入
        :param number:
        :return:
        """
        return "%.2f" % number

    def percentage(self, number):
        """
        转换成百分比
        :param number:
        :return:
        """
        return "{0:0.2f}%".format(number*100)


    def current_week(self):
        """
        本周
        :return:
        """
        #list(self._get_week(datetime.now().date()))
        return [d.isoformat() for d in self._get_week(datetime.now().date())]

    def last_week(self):
        """
        上周
        :return:
        """
        return [d.isoformat() for d in self._get_week((datetime.now()-timedelta(7)).date())]

    def last_last_week(self):
        """
        上上周
        :return:
        """
        return [d.isoformat() for d in self._get_week((datetime.now() - timedelta(14)).date())]

    def last_week_from_7_to_6(self):
        """
        上周 从星期日到星期六为一周期 日 一 二 三 四 五 六
        :return:
        """
        return [d.isoformat() for d in self._get_week_from_7_to_6((datetime).date())]

    def last_last_week_from_7_to_6(self):
        """
        上上周 从星期日到星期六为一周 日 一 二 三 四 五 六
        :return:
        """
        return [d.isoformat() for d in self._get_week_from_7_to_6((datetime(2018, 12, 25) -timedelta(7) ).date())]


    def _get_week_from_7_to_6(self, date):
        one_day = timedelta(days=1)
        day_idx = (date.weekday()+1) % 7
        sunday = date - timedelta(days=7+day_idx)
        date = sunday
        for n in range(1,8):
            yield date
            date += one_day

    def _get_week(self, date):
        one_day = timedelta(days=1)
        day_idx = (date.weekday()) % 7
        sunday = date - timedelta(days=day_idx)
        date = sunday
        for n in range(7):
            yield date
            date += one_day



    def _curr_and_last_and_last_last_month(self):
        """
        当月和上月分别开始和结束日期
        :return:
        """
        today = date.today()
        first_day_of_curr_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_curr_month - timedelta(days=1)

        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        last_day_of_last_last_month =first_day_of_last_month - timedelta(days=1)
        first_day_of_last_last_month = last_day_of_last_last_month.replace(day=1)
        return first_day_of_last_last_month.strftime("%Y-%m-%d"), last_day_of_last_last_month.strftime("%Y-%m-%d"), \
               first_day_of_last_month.strftime("%Y-%m-%d"), last_day_of_last_month.strftime("%Y-%m-%d"), \
               first_day_of_curr_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
