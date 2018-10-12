from aioredis import create_redis_pool
from motor.motor_asyncio import AsyncIOMotorClient

from configs import MONGODB_CONN_URL, REDIS_CONFIG
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


async def close_db(app):
    logger.debug('Close application.')
    app['redis'].close()
    await app['redis'].wait_closed()
