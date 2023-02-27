from collections.abc import Iterator
from json import dumps
from .fake_data import BASIC_DATA

class FakeMessage():
    '''A fake kafka message class'''
    def __init__(self, key, value):
        if not isinstance(key, bytes):
            raise TypeError('Fake kafka key must be bytes type')
        if not isinstance(value, bytes):
            raise TypeError('Fake kafka value must by bytes type')
        self._key = key
        self._value = value
    
    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

class FakeConsumer():
    '''a fake kafka consumer class'''
    def __init__(self, msg_gen=None):
        if not msg_gen:
            self.msg_gen = self.basic_msg_gen()
        elif not isinstance(msg_gen(), Iterator):
            raise TypeError('msg_gen() must be a generator, use yield in function def')
        else:
            self.msg_gen = self.msg_gen()

    @staticmethod
    def basic_msg_gen(value = BASIC_DATA):
        '''
        basic message generator
        这里约定：
        通过kafka key指定table name
        kafka value必须是JSON格式，json key指定数据表column name，json value强烈建议使用text格式
        Python sql客户端会自动转换数据类型，根据胡一刀原则，多一刀不如少一刀。
        '''
        fake_key = 'test'
        fake_value = value
        fk = FakeMessage(fake_key.encode(), dumps(fake_value).encode())
        yield fk

    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            return next(self.msg_gen)
        except IndexError:
            raise StopIteration
