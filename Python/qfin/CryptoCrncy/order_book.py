import logging

import numpy as np
import pandas as pd
from pandas import DataFrame, Series, concat
from qfin.utils import data_parser
from os.path import join as path_join


logger = logging.getLogger(__file__)
WARN   = logger.warn
temp_path = 'CryptoCrncy/templates'

# @ToDo: For post-trade analysis, we need fill bars as well 


class OrderBook(object):


    def __init__(self):
        self._active_orders   = None
        self._inactive_orders = None

    def initialize(self):
        self._active_orders   = data_parser.parse_data_from(path_join(temp_path, 'orderbook_temp.csv'),empty=1)
        self._inactive_orders = data_parser.parse_data_from(path_join(temp_path, 'orderbook_temp.csv'),empty=1)

    def update(self, exchanges):
        # update active order book
        assert isinstance(exchanges, dict)

    def log_orders(self, directory):
        # moive completed orders to 
        pass

    def cancel_all_orders(self, exchanges):
        for name, exch in exchanges.items():
            exch.cancel_all_orders()

    def send_order(self, trades):
        WARN("send_order: not implemented yet")

