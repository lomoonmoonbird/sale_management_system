from aiomysql import DictCursor


class DataExcludeMixin():
    exclude_channel_sql = "select id from sigma_account_us_user WHERE id in (select channel_id from sigma_account_re_channel_group where group_id = 9);"
    exclude_school_sql = "select id from sigma_account_ob_school where owner_id in" \
                         " ( select id from sigma_account_us_user WHERE id in (select channel_id " \
                         "from sigma_account_re_channel_group where group_id = 9) )"
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
        return [item['id'] for item in channels]

    async def exclude_schools(self, mysql_client):
        """
        测试学校
        :param mysql_client:
        :return:
        """
        async with mysql_client.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(self.exclude_school_sql)
                schools = await cur.fetchall()
        return [item['id'] for item in schools]