"""
aapi.routes
~~~~~~~~~~~
Include all routes here.
"""
import demo.routes
import statistics.routes
import user.routes
import school.routes
from aiohttp.web import Application
from loggings import logger


def setup_routes(app: Application):
    """Set routes in application routing system.

    How to setup new route:
    1. import module.routes
    2. module.routes.register(app.router)
    3. add a logger
    """
    logger.debug('Setup routes.')
    demo.routes.register(app.router)  # Add routes. Step-2
    logger.debug('Success register demo.routes')  # Add logger. Step-3
    # statistics.school.routes.register(app.router)
    # logger.debug('Success register stastictis.school.routes')
    user.routes.register(app.router)
    logger.debug('Success register user.routes')
    school.routes.register(app.router)
    logger.debug('Success register school.routes')
    statistics.routes.register(app.router)
    logger.debug('Success register statistics.routes')

