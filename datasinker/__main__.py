import os
import logging
from .config import DB, CONSUMER
from .sinker import Sinker

if __name__ == '__main__':
    logging.info('datasiker started at pid {}'.format(os.getpid()))
    s = Sinker(DB, CONSUMER)
    s.run()