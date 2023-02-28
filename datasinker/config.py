'''
This module collects configs from cmd arguments, and generate a Kafka consumer object, a dataset db object and a logger
'''
import dataset
# There are many kafka module, we use kafka-python here
from kafka import KafkaConsumer
import argparse
import logging

parser = argparse.ArgumentParser(
        prog = 'Data Sinker',
        description = 'Transfer data from kafka to database',
        epilog = ''
)

# kafka configs
parser.add_argument('-t', '--topic', help = 'Kafka topic', default = 'test')
parser.add_argument('-b', '--bootstrap', help = r'Kafka bootstrap servers in format {ip}:{port}', default='localhost:9092')
parser.add_argument('-o', '--offset', choices = ['earliest', 'latest'], default = 'latest')

# database configs
parser.add_argument('-d', '--database', help = r'database server in format {protocol}://{username}:{pwd}@{ip}:{port}/{schema}', default='sqlite://')

# log path
parser.add_argument('-l', '--logpath', default = '.')

args = parser.parse_args()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("{}/datasinker_{}.log".format(args.logpath, args.topic), mode = 'a')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.debug('datasinker runs with config : {}'.format(args))

CONSUMER = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, auto_offset_reset=args.offset)
# ensure_shcema 一定要设置为False，不然dataset会修改数据库的数据结构，切记切记！！
DB = dataset.connect(args.database, ensure_schema=False)
