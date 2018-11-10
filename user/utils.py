import asyncio
from exceptions import LoginError
from functools import wraps

import schema

import aiohttp.web
from configs import (THEMIS_SYSTEM_OPEN_URL, UC_SYSTEM_API_URL, permissionAppKey,
                     permissionAppSecret)