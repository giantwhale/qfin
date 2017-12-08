import os
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict


class BaseOrder(metaclass=ABCMeta):

    def __init__(self, oid, base_ccy, quote_ccy, side, price, size):
        pass

    @abstractproperty
    def pairname(self):
        pass

    @abstractproperty
    def side(self):
        pass

    @abstractproperty
    def price(self):
        pass

    @abstractproperty
    def size(self):
        pass

    @abstractproperty
    def exchange_name(self):
        pass

    @abstractproperty
    def order_id(self):
        pass



class BaseExecution(metaclass=ABCMeta):
    """
    Prototype for execution engine:
        - cancel order(s)
        - send orders
        - get fill(s) (optional)
        - get current position
    """
    def __init__(self):
        # @ToDo: do we need these two? I thought all orders are 
        # managed by OrderBook
        self._products = OrderedDict()
        self._orders   = OrderedDict()  # hashed by order_id

    def __str__(self):
        return ''

    @abstractmethod
    def cancel_order(self, client, order_id):
        pass

    @abstractmethod
    def cancel_all_orders(self, client):
        pass

    @abstractmethod
    def send_order(self, client, order):
        pass

    @abstractmethod
    def get_fill(self, client, order_id):
        pass

    @abstractmethod
    def get_all_fills(self, client):
        pass
