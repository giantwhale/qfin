from abc import ABCMeta, abstractmethod
from collections import OrderedDict

class BaseStrategy(metaclass=ABCMeta):

    def __init__(self):
        self._exchanges = OrderedDict()

    def update(self):
        for name, exch in self._exchanges.items():
            exch.update()

    def print_stats(self):
        for name, exch in self._exchanges.items():
            print("Exchange [%s]", name)
            exchange.print_stats()
