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
    LOG_FILE_PATH = '/data/logs/aio-sigma-tornado/index.log'
    if not os.path.exists('/data/logs/aio-sigma-tornado'):
        os.mkdir('/data/logs/aio-sigma-tornado')
    DEV_LOGGING_LEVEL = ...  # Not used in stable environment.

    """Account system URL"""
    STUDENT_ACCOUNT_SYS_URL = 'http://passport.ali.hexin.im/api'

    """Content-bank URL"""
    CONTENT_BANK_URL = 'http://mark.content.ali.hexin.im/api'

    """Celery task"""
    BROKER_URL = 'redis://redis.ali.hexin.im/19'

    """Centauri URL"""
    CENTAURI_URL = 'http://zuoye.ali.hexin.im/api'

else:
    DEBUG = True

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
        'db': 19,
        'minsize': 5,
        'maxsize': 10,
        'encoding': 'utf-8'
    }

    """Mongodb configs"""
    MONGODB_CONN_URL = 'mongodb://mongo.hexin.im'

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
    BROKER_URL = 'redis://redis.hexin.im/19'

    """Centauri URL"""
    CENTAURI_URL = 'http://zuoye.hexin.im/api'
