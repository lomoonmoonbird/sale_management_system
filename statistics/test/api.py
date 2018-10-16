import asyncio

from aiohttp.web import Request
from utils import *
# from tasks.models.tables import as_hermes
from sqlalchemy.dialects import mysql
from tasks.celery_init import school_task
async def index(req: Request):
    # print(await get_params(req))
    # print(await get_params(req, unique=False))
    # print(await get_json(req))
    # print(get_cookie(req))
    
    # u = as_hermes.select().where(as_hermes.c.uid > "1")
    # print (u.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string, type(u.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string))
    # pool = req.app['mysql']
    # async with pool.acquire() as conn:
    #     async with conn.cursor() as cursor:
    #         ret = await cursor.execute(u.compile(dialect=mysql.dialect(),compile_kwargs= {"literal_binds": True}).string)
    #         ret = await cursor.fetchall()
    # print (ret)
    # print (u.compile().statement)
    # print (u.compile(dialect=mysql.dialect()))
    school_task.delay()
    return text_response('666')
