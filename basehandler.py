# --*-- coding: utf-8 --*--

"""
请求处理基类
"""


from utils import json_response
from aiohttp import ClientSession
import ujson

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


    def reply_ok(self, response_data={}):
        data = {"code": 0, "msg": "ok", "data": response_data}
        return json_response(data=data)

