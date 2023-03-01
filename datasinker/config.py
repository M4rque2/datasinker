'''
This module collects configs from cmd arguments, and generate a Kafka consumer object, a dataset db object and determine log config
'''
# https://github.com/pudo/dataset/issues/411
# dataset不兼容sqlalchemy2.0 作者也不想跟进了，这里迟早需要优化
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
parser.add_argument('-l', '--logpath', default = '')

args = parser.parse_args()
# 这个方法只能调用一次，调用第二次无效，在整个module中设置一次即可，其他logger会自动得到这个设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                    filename = args.logpath
                    )
logger = logging.getLogger(__name__)
logger.info('DataSinker runs with config : {}'.format(args))

CONSUMER = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, auto_offset_reset=args.offset)
# ensure_shcema 一定要设置为False，不然dataset会修改数据库的数据结构，切记切记！！
DB = dataset.connect(args.database, ensure_schema=False)
