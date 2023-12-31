"""
aapi.main
~~~~~~~~~
Entry-point for aapi.
"""
import asyncio

import uvloop

import aiohttp
import loggings
from aiohttp import web
from basemodel import close_db, init_db
from middlewares import error_handle_middleware
from routes import setup_routes
from configs import PORT

# Get logger
logger = loggings.logger

# Install the uvloop event loop policy.
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def init_app(app: web.Application):
    """Initiate web application settings after app is instantiated."""
    logger.debug('Init application.')

    setup_routes(app)

    await init_db(app)
    app['http_session'] = aiohttp.ClientSession()


async def close_app(app: web.Application):
    """Close web application gracefully."""
    await close_db(app)
    await app['http_session'].close()


APP = web.Application(middlewares=[error_handle_middleware], debug=False, logger=None)

# Setup on_startup and on_cleanup.
APP.on_startup.append(init_app)
APP.on_cleanup.append(close_app)

host, port = '0.0.0.0', PORT

web.run_app(APP, host=host, port=port, access_log=None, print=logger.debug)
