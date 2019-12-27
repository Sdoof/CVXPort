
import logging
from datetime import datetime
from pytz import timezone
import pathlib

from cvxport import Config
from cvxport import utils


class Logger:
    def __init__(self, name):
        self.name = name

        # set up directory
        today = datetime.strftime(datetime.now(timezone('EST')), '%Y-%m-%d')
        path = pathlib.Path(Config['log_path']) / today
        filename = utils.get_next_filename(pathname=path, file_prefix=name)

        # configure log output
        print(filename)
        handler = logging.FileHandler(filename=filename)
        handler.setLevel(Config['log_level'])
        handler.setFormatter(logging.Formatter(fmt=Config['log_format'], datefmt=Config['log_date_format']))

        self.logger = logging.getLogger(name)
        self.logger.setLevel(Config['log_level'])
        self.logger.addHandler(handler)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def exception(self, msg):
        self.logger.exception(msg)


if __name__ == '__main__':
    logger = Logger('test')

    try:
        raise ValueError('dummy')
    except:
        logger.error('tested')
        logger.exception('tested')