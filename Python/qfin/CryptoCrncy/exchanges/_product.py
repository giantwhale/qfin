import os
from abc import ABCMeta, abstractmethod, abstractproperty

from collections import deque


class BaseProduct(metaclass=ABCMeta):
    # This is a Prototype. Implementation isn't efficient. 

    def __init__(self, base_ccy, quote_ccy):
        self._base_ccy  = str(base_ccy )
        self._quote_ccy = str(quote_ccy)

        # We need to decide how data are stored
        # For prototyping, we start with the simplest 
        # implemetation
        # ===========================================

        ntick = 4096
        n5min = 100

        # Trades
        self._max_tid = 0
        self._trades  = deque(maxlen=ntick)
        self._sides   = deque(maxlen=ntick)
        self._bars_5m = deque(maxlen=n5min)

    # Common interfaces
    @property
    def base_ccy(self):
        return self._base_ccy

    @property
    def quote_ccy(self):
        return self._quote_ccy

    @abstractproperty
    def name(self):
        # A Unique Identifier that the Exchange API 
        # uses to identify the ccy pair
        pass 

    # @deprecated: we no longer
    # @abstractmethod
    # def get_bar_data(self, n=100, freq=5):
    #     raise NotImplementedError("")

    # Auxiliary function
    def __str__(self):
        items = [
              "Class <%s>" % self.__class__.__name__
            , "==============================="
            , " Base ccy: %s" % self._base_ccy
            , "Quote ccy: %s" % self._quote_ccy
        ]
        return os.linesep.join(items)
