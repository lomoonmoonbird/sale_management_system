"""
aapi.configs
~~~~~~~~~~~~
Include all configs that are used within aapi.
"""
import os
import socket

here = os.path.abspath(os.path.dirname(__file__))
env = 'stable' if socket.gethostname().startswith('ali') else 'dev'

if env == 'stable':
    DEBUG = False
    PORT = 8080
    """MySQL config"""
    MYSQL_NAME = 'sigma_centauri_new'
    MYSQL_USER = 'sigma'
    MYSQL_PASSWORD = 'sigmaLOVE2017'
    MYSQL_HOST = 'rm-uf68040g28501oyn1rw.mysql.rds.aliyuncs.com'
    MYSQL_HOST_WRITE = 'rm-uf68040g28501oyn1.mysql.rds.aliyuncs.com'  # Not used now.
    MYSQL_HOST_READ = 'rr-uf6247jo85269bp6e.mysql.rds.aliyuncs.com'  # Not used now.
    MYSQL_PORT = 3306
    MYSQL_POOL_MINSIZE = 5
    MYSQL_POOL_MAXSIZE = 10

    """Redis configs"""
    REDIS_CONFIG = {
        'address': 'redis://redis.ali.hexin.im:6379',
        'db': 2,
        'minsize': 5,
        'maxsize': 10
    }

    """Mongodb configs"""
    MONGODB_CONN_URL = 'mongodb://root:sigmaLOVE2017@dds-uf6fcc4e461ee5a41.mongodb.rds.aliyuncs.com:3717,' \
                       'dds-uf6fcc4e461ee5a42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-7558683'

    """Logging configs"""
    LOG_FILE_PATH = '/data/logs/Sales-management-system/index.log'
    if not os.path.exists('/data/logs/Sales-management-system'):
        os.mkdir('/data/logs/Sales-management-system')
    DEV_LOGGING_LEVEL = ...  # Not used in stable environment.

    """Account system URL"""
    STUDENT_ACCOUNT_SYS_URL = 'http://passport.ali.hexin.im/api'

    """Content-bank URL"""
    CONTENT_BANK_URL = 'http://mark.content.ali.hexin.im/api'

    """Celery task"""
    RESULT_BACKEND = 'redis://redis.ali.hexin.im/21'
    BROKER_URL = 'redis://redis.ali.hexin.im/21'

    """Centauri URL"""
    CENTAURI_URL = 'http://zuoye.ali.hexin.im/api'

    """For permission"""
    UC_SYSTEM_API_URL = 'http://uc.ali.hexin.im/api'
    UC_SYSTEM_API_ADMIN_URL = 'http://uc.ali.hexin.im/api/admin'
    THEMIS_SYSTEM_OPEN_URL = 'http://themis.ali.hexin.im/api/open'
    THEMIS_SYSTEM_ADMIN_URL = 'http://themis.ali.hexin.im/api/admin'
    permissionAppKey = 'themise8aacb6b63'
    permissionAppSecret = '1f08aee118e0167277b1f21fb6aa577c'
    ucAppKey = '0d8cf2c3b25588ca'
    ucAppSecret = '956bc2120d8cf2c3b25588ca8b3e116b'

    """权限themis角色"""
    PERMISSION_SUPER = 36  # 超级管理员id
    PERMISSION_GLOBAL = 32  # 总部id
    PERMISSION_AREA = 33  # 大区id
    PERMISSION_CHANNEL = 34  # 渠道id
    PERMISSION_MARKET = 35  # 市场id

else:
    DEBUG = True
    PORT = 8080
    """MySQL configs"""
    MYSQL_NAME = 'sigma_centauri_new'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'sigmalove'
    MYSQL_HOST = 'mysql.hexin.im'
    MYSQL_HOST_WRITE = 'mysql.hexin.im'  # Not used now.
    MYSQL_HOST_READ = 'mysql.hexin.im'  # Not used now.
    MYSQL_PORT = 3306
    MYSQL_POOL_MINSIZE = 1
    MYSQL_POOL_MAXSIZE = 1

    """Redis configs"""
    REDIS_CONFIG = {
        'address': 'redis://redis.hexin.im:6379',
        'db': 20,
        'minsize': 5,
        'maxsize': 10,
        'encoding': 'utf-8'
    }

    """Mongodb configs"""
    MONGODB_CONN_URL = 'mongodb://127.0.0.1'

    """Logging configs"""
    LOG_FILE_PATH = os.path.join(here, 'logs/index.log')
    if not os.path.exists(os.path.join(here, 'logs')):
        os.mkdir(os.path.join(here, 'logs'))
    DEV_LOGGING_LEVEL = 'DEBUG'

    """Account system URL"""
    STUDENT_ACCOUNT_SYS_URL = 'http://passport.hexin.im/api'

    """Content-bank URL"""
    CONTENT_BANK_URL = 'http://mark.content.hexin.im/api'

    """Celery task"""

    RESULT_BACKEND = 'redis://127.0.0.1'
    BROKER_URL = 'redis://127.0.0.1'

    """Centauri URL"""
    CENTAURI_URL = 'http://zuoye.hexin.im/api'

    """For permission"""
    UC_SYSTEM_API_URL = 'http://uc.hexin.im/api'
    UC_SYSTEM_API_ADMIN_URL = 'http://uc.hexin.im/api/admin'
    THEMIS_SYSTEM_OPEN_URL = 'http://themis.hexin.im/api/open'
    THEMIS_SYSTEM_ADMIN_URL = 'http://themis.hexin.im/api/admin'
    permissionAppKey = 'themisb560b78819'
    permissionAppSecret = '1fab6d0efbd554e4daab74232774c5c0'
    ucAppKey = '0d8cf2c3b25588ca'
    ucAppSecret = '956bc2120d8cf2c3b25588ca8b3e116b'

    """权限themis角色"""
    PERMISSION_SUPER = 37  # 超级管理员id
    PERMISSION_GLOBAL = 35  # 总部id
    PERMISSION_AREA = 33  # 大区id
    PERMISSION_CHANNEL = 34  # 渠道id
    PERMISSION_MARKET = 36  # 市场id
