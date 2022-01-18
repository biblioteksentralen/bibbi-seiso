import os
import re
from pathlib import Path
from typing import Optional

import yaml
import logging.config
import logging
import time
from datetime import timedelta

from dotenv import load_dotenv


class ElapsedFilter(logging.Filter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()

    def filter(self, record):
        elapsed_seconds = record.created - self.start_time
        record.elapsed = str(timedelta(seconds=elapsed_seconds)).split('.')[0]
        return True


class ElapsedFormatter(logging.Formatter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()

    def formatTime(self, record, datefmt=None):
        elapsed_seconds = record.created - self.start_time
        return str(timedelta(seconds=elapsed_seconds)).split('.')[0]


def setup_logging(path: Optional[Path] = None, level=logging.INFO):
    load_dotenv()
    if path is None:
        path = Path('logging.yml')

    if not path.exists():
        logging.basicConfig(level=level)
        print('Failed to load configuration file. Using default configs')
        return

    with path.open('r') as f:
        try:
            log_dir = Path(os.getenv('STORAGE_PATH')).joinpath(os.getenv('LOG_PATH'))
            if not log_dir.exists():
                log_dir.mkdir(parents=True)
            config = yaml.safe_load(re.sub(r'\{\{\s*log-path\s*\}\}', str(log_dir), f.read()))
            logging.config.dictConfig(config)
        except Exception as e:
            print(e)
            print('Error in Logging Configuration. Using default configs')
            logging.basicConfig(level=level)

    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    logger = logging.getLogger()
    # handler = logging.StreamHandler()
    # handler.setFormatter(ElapsedFormatter('%(asctime)s %(levelname)-8s %(message)s'))
    # logger.addHandler(handler)
    return logger


