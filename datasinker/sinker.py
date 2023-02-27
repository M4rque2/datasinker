# 启动方法：
# nohup python3 TiDB_sinker_pro.py >> /chj/logs/sinker_pro.log 2>&1 &

# 遇到没见过的表程序会重新加载表名配置，加载出来还是没有，直接用kafka key作为表名
# 同理，关闭程序时也不建议直接关，会丢失数据，使用 kill -SIGTERM 安全结束程序

import os
import logging
from json import loads
from json.decoder import JSONDecodeError

class Sinker():
    '''从kafka读取数据, 入库TiDB'''
    def __init__(self, db, consumer, logger=None):
        # 传入pymysql的connector对象，
        # 传入KafkaConsumer
        # 数据库有MySQL和TiDB，TiDB有2套环境，kafka有多个topic，以上每一种组合，起一个独立进程。
        self.db = db
        self.consumer = consumer
        if not logger:
            self.logger = logging.getLogger(__name__)
        else:  
            self.logger = logger
        pid = os.getpid()
        self.logger.info("TiDB sinker pro started at pid {}".format(pid))

    def run(self):
        for msg in self.consumer:
            if not msg.key:
                # Kafka 消息不带key就完了，不知道这条消息往哪张表里插入，只能丢弃了
                self.logger.error(f"A message without key {msg}")
                continue
            table_name = msg.key.decode('utf-8')
            table = self.db[table_name]
            if table is None:
                error_text = "table doesn't exist {}".format(table_name)
                self.logger.error(error_text)
                continue

            try:
                msg_json = loads(msg.value.decode('utf-8'))
            except JSONDecodeError:
                self.logger.error("json decode error, msg: {}".format(msg.value))
                continue

            try:
                # 如果ensure为True或者None，当dict和数据库表里的字段对不上时。会修改表结构，强行插入。这里设为False，不允许修改表结构。
                # dataset有一个bug还没修，如果表结构和数据不匹配，会插入一条空记录，自增id加一条。
                table.insert(msg_json)
            except Exception as e:
                self.logger.error(e)
