# --*-- coding: utf-8 --*--

import treelib
import ujson
from loggings import logger
"""
菜单管理
"""


class Data(object):
    def __init__(self, roles=[]):
        self.roles = roles


    def to_json(self):
        return {
            "menu_id": self.menu_id,
            "menu_name": self.menu_name,
            "menu_roles": self.menu_roles,


        }


    def __call__(self, menu_id, menu_name, menu_roles):
        self.menu_id = menu_id
        self.menu_name = menu_name
        self.menu_roles = menu_roles
        return self.to_json()

class Menu:

    def __init__(self):
        self.menu = treelib.Tree()

        self.data = Data()
        self.menus()

    def menus(self):
        self.menu.create_node("Root", 'root', data=self.data("root", "dashboard", ["super"]))
        self.menu.create_node("账号管理", 'account', parent='root', data=self.data("account", "账号管理", ["global", "area", "channel"]))
        self.menu.create_node("大区账号管理", 'area_account', parent='account', data=self.data("area_account", "大区账号管理", ["global"]))
        self.menu.create_node("渠道账号管理", 'channel_account', parent='account',
                              data=self.data("channel_account", "渠道账号管理", ['area']))
        self.menu.create_node("市场账号管理", 'market_account', parent='account',
                              data=self.data("market_account", "市场账号管理", ['channel']))


    def show(self):
        logger.debug(ujson.dumps(self.menu.to_dict(with_data=True), indent=4))
        return self.menu.to_dict(with_data=True)


