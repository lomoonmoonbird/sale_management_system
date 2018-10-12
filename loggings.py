"""
aapi.loggings
~~~~~~~~~~~~~
Initialize logger here.
"""
import logging.config

from configs import DEV_LOGGING_LEVEL, LOG_FILE_PATH, env

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [pid:%(process)d] [%(levelname)s] %(message)s',
        },
        'precise': {
            'format': '%(levelname)-8s \x1b[6;30;42m \x1b[0m %(asctime)-s \x1b[6;30;42m \x1b[0m '
                      '%(module)-15s -> %(funcName)-15s \x1b[6;30;42m \x1b[0m '
                      '%(message)s\n'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_FILE_PATH,
            'maxBytes': 1024 * 1024 * 1024 * 1024,  # No limit.
            'backupCount': 1,  # Only one.
            'formatter': 'standard',  # Use which formatter.
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'precise'
        },
    },
    'loggers': {
        'aapi': {
            'handlers': ['default'] if env == 'stable' else ['console'],
            'level': 'DEBUG' if env == 'stable' else DEV_LOGGING_LEVEL,
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING)

logger = logging.getLogger('aapi')
