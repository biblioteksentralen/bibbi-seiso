import logging
import time
from datetime import timedelta


class ElapsedFormatter(logging.Formatter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()

    def formatTime(self, record, datefmt=None):
        elapsed_seconds = record.created - self.start_time
        return str(timedelta(seconds=elapsed_seconds)).split('.')[0]


logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(ElapsedFormatter('%(asctime)s %(levelname)-8s %(message)s'))
logger.addHandler(handler)
