import os
from abc import ABCMeta, abstractmethod, abstractproperty

import logging 

from copy import copy
from collections import OrderedDict

from ._product import BaseProduct

logger = logging.getLogger(__file__)
WARN   = logger.warn


class Exchange(metaclass=ABCMeta):

    def __init__(self):
        self._products = OrderedDict()  # ccy pairs 
        self._ccys     = []             # ccy

    @abstractproperty
    def product_cls(self):
        pass

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def available_usd(self):
        pass

    @abstractproperty
    def positions(self):
        pass

    @abstractproperty
    def best_bid_ask(self):
        pass

    def initialize(self):
        """For derived exchange instances, if there's need to re-implement this object, 
        the method must call super(Drived, self).initialize()
        """
        ccys = []
        for _, prd in self._products.items():
            ccys.append(prd.base_ccy)
            ccys.append(prd.quote_ccy)
        ccys = set(ccys)

        if 'USD' not in ccys:
            raise KeyError("USD must be present to allow proper trading.")
        self._ccys = sorted(list(ccys))

    @property
    def ccy_list(self):
        return copy(self._ccys)

    @property
    def num_ccy(self):
        return len(self._ccys)

    @abstractmethod
    def update(self):
        """Exchnange specific, as different exch may have 
        different API to update trade & quote data
        """
        pass

    def add_product(self, base_ccy, quote_ccy):
        prd = self.product_cls(base_ccy, quote_ccy)
        self._products[prd.name] = prd
        return self

    @abstractmethod
    def get_trade_ticks(self):
        """Used in DataEngine to get a list of all trade tick blocks"""
        pass

    @abstractmethod
    def get_quote_ticks(self):
        """Used in DataEngine to get a list of all quote tick blocks"""
        pass

    @abstractmethod
    def load_current_positions(self):
        """Used by TradeEngine"""
        pass

    @abstractmethod
    def load_current_quotes(self):
        """Used by TradeEngine"""
        pass


    def print_stats(self):
        print("this is exchange [%s]" % self.name)

    # Order API
    @abstractmethod
    def cancel_all_orders(self):
        pass

    @abstractmethod
    def send_order(self, *args, **kwargs):
        pass

    # Auxiliary Functions
    # ---------------------------------------------
    def __str__(self):
        items = ['Exchange [%s]' % self.name]
        for _, prd in self._products.items():
            items.extend(['  | ' + x for x in str(prd).split(os.linesep)])
        return os.linesep.join(items)
    
    def __repr__(self):
        return self.__str__()
