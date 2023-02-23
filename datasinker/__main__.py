import logging
from .config import args, DB, CONSUMER
from .sinker import Sinker

if __name__ == '__main__':
    print('datasinker is running')

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler("{}/datasinker_{}.log".format(args.logpath, args.topic), mode = 'a')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(args)
    s = Sinker(DB, CONSUMER, logger)
    s.run()