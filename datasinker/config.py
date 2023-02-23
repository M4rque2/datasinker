'''
This module collects configs from cmd arguments, and generate a Kafka consumer object and a dataset db
'''
# import dataset
# from kafka import KafkaConsumer
import argparse

parser = argparse.ArgumentParser(
        prog = 'Data Sinker',
        description = 'Transfer data from kafka to database',
        epilog = ''
)

# kafka configs
parser.add_argument('-t', '--topic', help = 'Kafka topic', required=True)
parser.add_argument('-b', '--bootstrap', help = r'Kafka bootstrap servers in format {ip}:{port}', required=True)
parser.add_argument('-o', '--offset', choices = ['earliest', 'latest'], default = 'latest')

# database configs
parser.add_argument('-d', '--database', help = r'database server in format {protocol}://{username}:{pwd}@{ip}:{port}/{schema}', required=True)

args = parser.parse_args()

# CONSUMER = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, auto_offset_reset=args.offset)
# Connect to the database
# DB = dataset.connect(args.database)
