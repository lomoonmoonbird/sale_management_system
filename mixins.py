from aiomysql import DictCursor


class DataExcludeMixin():
    exclude_channel_sql = "select id from sigma_account_us_user WHERE id in (select channel_id from sigma_account_re_channel_group where group_id = 9);"
    exclude_school_sql = "select id from sigma_account_ob_school where owner_id in (%s)"
    async def exclude_channel(self, mysql_client):
        """
        测试渠道
        :param mysql_client:
        :return:
        """
        async with mysql_client.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(self.exclude_channel_sql)
                channels = await cur.fetchall()
        return [item['id'] for item in channels] + [8, 81]

    async def exclude_schools(self, mysql_client):
        """
        测试学校
        :param mysql_client:
        :return:
        """
        exclude_channel = await self.exclude_channel(mysql_client)
        async with mysql_client.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(self.exclude_school_sql % (",".join([str(id) for id in exclude_channel])))
                schools = await cur.fetchall()
        return [item['id'] for item in schools if item['id'] ]