import os
import logging
import logging.config
import datetime
import yaml

'''
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': 0,

    'root': {
        'handlers': ['consoleHandler', 'fileHandler'],
        'level': 'INFO'
        },

    'handlers': {
        'consoleHandler': {
            'level': 'INFO',
            'formatter': 'consoleFormatter',
            '()': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
            },

        'fileHandler': {
            'formatter': 'fileFormatter',
            '()': 'logging.FileHandler',
            'level': 'INFO',
            'filename': ''
            }

        },

    'formatters': {
        'consoleFormatter': {
            '()': 'lib.ExecutionLogger.CustomFormatter',
            'format_out': '%(asctime)s :: %(levelname)s :: [%(threadName)s | %(filename)s | %(lineno)d] :: %(message)s'
            },
        'fileFormatter': {
            'format': '%(asctime)s :: %(levelname)s :: [%(threadName)s | %(filename)s | %(lineno)d] :: %(message)s'
            }
     }
}
'''

class Logger(object):
    logger = None
    logs_file_loc = None

    @classmethod
    def init_logger(cls, file_name):
        cls.file_name = file_name
        cls.logs_file_loc = os.path.join('.', 'logs', f'{file_name}.log')
        os.makedirs(os.path.join('.', 'logs'), exist_ok=True)
        log_conf_loc = os.path.join('.', 'config', 'log.yml')
        with open(log_conf_loc, "r") as stream:
            logging_config = yaml.safe_load(stream)
        logging_config['handlers']['fileHandler']['filename'] = cls.logs_file_loc
        logging.config.dictConfig(logging_config)
        cls.logger = logging.getLogger(__name__)
        cls.logger.info(f'start time : {datetime.datetime.now().strftime("%Y/%m/%d %I:%M:%S %p")}')


class CustomFormatter(logging.Formatter):

    def __init__(self, format_out):
        logging.Formatter.__init__(self, fmt=format_out)
        blue = '\033[34m'
        green = '\033[1;36m'
        yellow = '\033[33m'
        red = '\033[91m'
        dark_red = '\033[0m'
        reset = '\033[0m'
        self.FORMATS = {
            logging.DEBUG: blue + format_out + reset,
            logging.INFO: green + format_out + reset,
            logging.WARNING: yellow + format_out + reset,
            logging.ERROR: red + format_out + reset,
            logging.CRITICAL: dark_red + format_out + reset
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
