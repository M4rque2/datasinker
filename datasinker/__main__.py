import logging
from .config import logger, DB, CONSUMER
from .sinker import Sinker

if __name__ == '__main__':

    s = Sinker(DB, CONSUMER, logger)
    s.run()