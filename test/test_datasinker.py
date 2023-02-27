import unittest
import dataset

from datasinker.sinker import Sinker
from .fake_kafka import FakeConsumer
from .fake_data import BASIC_DATA



class SqliteTestCase(unittest.TestCase):
    def setUp(self):
        CONSUMER = FakeConsumer()
        DB = dataset.connect('sqlite:///test.db')
        s = Sinker(DB, CONSUMER)
        s.run()
        self.DB = DB
        self.table = DB['test']

    def test_basic(self):
        data = self.table.find_one()
        assert isinstance(data['text_data'], str)
        assert isinstance(data['json_data'], dict)

if __name__ == '__main__':
    unittest.main()
