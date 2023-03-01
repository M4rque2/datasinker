import unittest
import dataset

from datasinker.sinker import Sinker
from .mock_kafka import MockConsumer
from .mock_data import BASIC_DATA

class SqliteTestCase(unittest.TestCase):
    
    def setUp(self):
        CONSUMER = MockConsumer()
        DB = dataset.connect('sqlite:///test.db', ensure_schema = False)
        # 如果不想保留文件，用这个方法启动一个内存数据库
        # DB = dataset.connect('sqlite://', ensure_schema=False)
        table = DB.create_table(BASIC_DATA['kafka_key'], primary_id='id', primary_type=DB.types.integer, primary_increment=True)
        for key, value in BASIC_DATA['kafka_value'].items():
            if isinstance(value, str):
                data_type = DB.types.text
            elif isinstance(value, int):
                data_type = DB.types.integer
            elif isinstance(value, dict):
                data_type = DB.types.json
            else:
                raise TypeError('Unknown data type {}'.format(type(value)))
            table.create_column(key, data_type)
        s = Sinker(DB, CONSUMER)
        s.run()
        self.DB = DB
        self.table = table
    
    def tearDown(self):
        # clean up test table
        self.table.drop()

    def test_basic(self):
        # get the latest inserted data
        data = self.table.find_one(order_by=['-id'])
        assert isinstance(data['text_data'], str)
        assert isinstance(data['json_data'], dict)
        len(self.table) == 100
    
    def test_log(self):
        # Pytest won't show logs if all test passes, if you want to show logs assert False
        assert True

if __name__ == '__main__':
    unittest.main()
