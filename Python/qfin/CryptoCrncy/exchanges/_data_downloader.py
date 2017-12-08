from time import time, sleep
from collections import OrderedDict

from abc import ABCMeta, abstractmethod

from ._product import _Product


class BaseDataDownloader(metaclass=ABCMeta):
    """DataEngine manages all market data. 

    DataEngine is responsible for:
    1. Pull Data from the Website
    2. Keep the most recent data in memory
    3. Calculate aggregate bar data (1min, 5min, 10min)
    """

    def __init__(self):
        self._products    = OrderedDict()
        self._product_cls = None

        self._min_rtime   = 0.34   # minimum running time

    def add_product(self, *args, **kwargs):
        assert isinstance(self._product_cls, _Product)
        prod = self._product_cls(*args, **kwargs)
        self._products[prod.name] = prod

    @abstractmethod
    def download_trades(self):
        for name, prod in self._products.items():
            stime = time()
            try:
                prod.download_trades()
            except Exception as err:
                pass
            finally:
                wait = self._min_rtime - (time() - stime)
                if wait > 0:
                    sleep(wait)

