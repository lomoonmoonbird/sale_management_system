from aioredis import create_redis_pool
from motor.motor_asyncio import AsyncIOMotorClient
import aiomysql

from configs import MONGODB_CONN_URL, REDIS_CONFIG, MYSQL_NAME, MYSQL_USER, \
    MYSQL_PASSWORD, MYSQL_HOST, MYSQL_HOST_WRITE, MYSQL_HOST_READ, MYSQL_PORT, \
    MYSQL_POOL_MINSIZE, MYSQL_POOL_MAXSIZE

from loggings import logger


async def init_db(app):
    logger.debug('Init database.')

    # Initiate redis client.
    redis_engine = await create_redis_pool(**REDIS_CONFIG)
    app['redis'] = redis_engine
    logger.debug('Redis client ready.')

    # Initiate mongodb client.
    mongodb_engine = AsyncIOMotorClient(MONGODB_CONN_URL)
    app['mongodb'] = mongodb_engine
    logger.debug('mongodb client ready.')

    #Initiate mysql connection pool
    mysql_pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT,
                                      user=MYSQL_USER, password=MYSQL_PASSWORD,
                                      db=MYSQL_NAME)

    app['mysql'] = mysql_pool
    logger.debug('mysql connection pool ready.')

async def close_db(app):
    logger.debug('Close application.')
    app['redis'].close()
    await app['redis'].wait_closed()
