import logging

import os
from os import makedirs
from os.path import join as path_join
from time import sleep
from datetime import datetime, timedelta
utcnow = datetime.utcnow

from collections import OrderedDict

from pandas import DataFrame, concat

from .exchanges import Exchange

from .mongo_helper import MongoHelper
from .taq_helper import TAQHelper
from ..utils.serialize import serialize
from ..utils.dt_utils import floor_dt, ceil_dt

from .. import settings


logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn



class DataEngine(object):

    def __init__(self, exchanges=None):
        self._exchanges = OrderedDict()
        self._mongo     = MongoHelper()

        if exchanges:
            self.add_exchanges(exchanges)

        self._taq_helper = TAQHelper()

    def initialize(self):
        for name, exch in self._exchanges.items():
            exch.initialize()

    def add_exchanges(self, exchanges):
        if not isinstance(exchanges, list):
            exchanges = [exchanges]
        for exch in exchanges:
            assert isinstance(exch, Exchange), "%s is not an Exchange instance" % str(exch)
            self._exchanges[exch.name] = exch
        return self

    def publish_taq_data(self, bar_time_start):
        # prepare data
        v_trades = []
        v_quotes = []
        for _, exch in self._exchanges.items():
            v_trades.append( exch.get_trade_ticks() )
            v_quotes.append( exch.get_quote_ticks() )
        trades = concat(v_trades, axis=0, ignore_index=True)
        quotes = concat(v_quotes, axis=0, ignore_index=True)
        
        INFO("Publish Data {} trades, {} quotes at bar {}".format(
            trades.shape[0], quotes.shape[0], bar_time_start.strftime('%H:%M')))

        if settings.run_type == 'PROD':
            self._taq_helper.save_taq_data_prod(bar_time_start, trades, quotes)

        # then save data to disk
        self._taq_helper.save_taq_data(bar_time_start, trades, quotes)

    def publish_current_quotes(self):
        v = []
        for name, exch in self._exchanges.items():
            q = exch.load_current_quotes()
            v.append(q)
        quotes = concat(v, axis=0, ignore_index=True)
        doc = serialize(quotes)
        self._mongo.append('current_quote', utcnow(), doc)

    def start_server(self):
        self.initialize()

        one_min = timedelta(minutes=1)
        bartime = floor_dt(utcnow(), minutes=1)
        while 1:
            # Update market data
            cnt = 0
            quotes = []
            for name, exch in self._exchanges.items():
                DEBUG('updating exchange [%s]' % name)
                cnt += exch.update() or 0

            INFO("DataEngine Server Loop @ {:s} ... loaded {:d} records".format(
                utcnow().strftime('%Y-%m-%d %H:%M:%S'), cnt))

            INFO("Publish current quotes")
            self.publish_current_quotes()

            # Dump data to disk
            if utcnow() >= bartime + one_min:
                self.publish_taq_data(bartime)
                bartime = bartime + one_min

    def print_debug_info(self):
        for name, exch in self._exchanges.items():
            print(exch)
