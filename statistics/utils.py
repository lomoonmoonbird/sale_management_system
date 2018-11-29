
from aiomysql.cursors import DictCursor

class DataExcludeMixin():

    def __init__(self):
        self.sql = "select * from sigma_account_us_user WHERE id in (select channel_id from sigma_account_re_channel_group where group_id = 9);"

    async def exclude_channel(self, mysql_client):
        async with mysql_client.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(self.sql)
                channels = await cur.fetchall()
        return [item['id'] for item in channels]