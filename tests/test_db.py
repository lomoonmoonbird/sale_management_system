import asyncio
import unittest
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
import json
from configs import THEMIS_SYSTEM_OPEN_URL, permissionAppKey, THEMIS_SYSTEM_ADMIN_URL
from configs import MONGODB_CONN_URL, REDIS_CONFIG, MYSQL_NAME, MYSQL_USER, \
    MYSQL_PASSWORD, MYSQL_HOST, MYSQL_HOST_WRITE, MYSQL_HOST_READ, MYSQL_PORT, \
    MYSQL_POOL_MINSIZE, MYSQL_POOL_MAXSIZE


class TestThemis(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.mongodb_engine = AsyncIOMotorClient(MONGODB_CONN_URL, io_loop=self.loop)
    def test_app_detail(self):
        async def go():
            all = self.mongodb_engine['sales']['instance'].find({})
            all = await all.to_list(10000)
            for a in all:
                await self.mongodb_engine['sales']['instance'].update_one({"_id": a['_id']}, {"$set": {"parent_id": str(a['parent_id'])}})

        self.loop.run_until_complete(go())




