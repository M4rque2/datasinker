import unittest
import dataset

from datasinker.sinker import Sinker
from .fake_kafka import FakeConsumer
from .fake_data import BASIC_DATA



class SqliteTestCase(unittest.TestCase):
    def setUp(self):
        CONSUMER = FakeConsumer()
        # DB = dataset.connect('sqlite:///test.db', ensure_schema = False)
        DB = dataset.connect('sqlite://', ensure_schema=False)
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

    def test_basic(self):
        # get the latest inserted data
        data = self.table.find_one(order_by=['-id'])
        self.assertTrue(data['text_data'], str)
        self.assertTrue(data['json_data'], dict)
        assert len(self.table) == 1

if __name__ == '__main__':
    unittest.main()
