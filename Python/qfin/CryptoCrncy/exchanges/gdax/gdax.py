import logging

import os
from time import sleep
from collections import OrderedDict
from numpy import float_
from pandas import DataFrame, concat
import pandas as pd
from datetime import datetime, timedelta

from io import StringIO

from .._exchange import Exchange
from .product import Product
from .execution import GdaxExecution
import gdax  # gdax-python package
from .extensions import AuthenticatedClientEnriched

from ...fileutils import load_signal
from .... import settings


logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info


def vconcat(items):
    return concat(items, axis=0, ignore_index=True)


class GDAX(Exchange):

    def __init__(self):
        super(GDAX, self).__init__()
        self._client    = None
        self._positions = None
        self._USD       = -1e16
        self._quotes    = None

        self._exec_engine = GdaxExecution()

    @property
    def client(self):
        # @FixMe: is there active connection between client and server
        # do we have to reconnect when disconnected?
        return self._client

    @property
    def product_cls(self):
        return Product

    @property
    def name(self):
        return 'GDAX'

    @property
    def available_usd(self):
        if self._USD < -1e12: 
            raise RuntimeError("USD Position hasn't been updated.")
        return self._USD

    @property
    def positions(self):
        if self._positions is None:
            raise RuntimeError("Position hasn't been loaded yet")
        return self._positions

    @property
    def best_bid_ask(self):
        if self._positions is None:
            raise RuntimeError("Position hasn't been loaded yet")
        return self._quotes        

    def initialize(self):
        DEBUG('GDAX::initialize ...')
        super(GDAX, self).initialize()
        GDAX_Config = settings.GDAX_Config
        self._client = AuthenticatedClientEnriched(GDAX_Config.key, GDAX_Config.seckey, GDAX_Config.passwd)

    # Update functions: pull data from the Exchange 
    # -------------------------------------------------
    
    def update(self, asof=None):
        assert self._client is not None, 'Initialize Exch GDAX first.'
        if asof is not None:
            NotImplementedError("SIM mode hasn't been implemented")

        cnt = 0
        for name, prd in self._products.items():
            DEBUG('GDAX::updating product [%s]' % name)
            cnt += prd.update(self._client)
        return cnt

    # Order API
    # -------------------------------------------------
    
    def cancel_all_orders(self):
        self._exec_engine.cancel_all_orders(self.client)

    def send_order(self, **kwargs):
        # Function has too many args, must be keyed
        return self._exec_engine.send_order(self.client, **kwargs)

    # Data API
    # -------------------------------------------------
    def get_trade_ticks(self, columns=None):
        hld = vconcat([prd.get_trade_ticks() for key, prd in self._products.items()])
        if len(hld) == 0:
            INFO('No trades data found')
        hld = hld.sort_values('TradeTime').reset_index()
        if columns:
            if type(columns) == str:
                columns = columns.split(',')
            return hld.ix[:, columns].copy()
        return hld

    def get_quote_ticks(self, columns=None):
        hld = vconcat([prd.get_quote_ticks() for key, prd in self._products.items()])
        if len(hld) == 0:
            INFO('No quotes data found')
        hld = hld.sort_values('DataTime').reset_index()
        if columns:
            if type(columns) == str:
                columns = columns.split(',')            
            return hld.ix[:, columns].copy()
        return hld


    def load_current_positions(self):
        accounts = self._client.get_accounts()
        items = []

        for acct in accounts:
            items.append(OrderedDict([
                    ( 'Exchange', 'GDAX'                          ),
                    ( 'Ccy',      acct.get('currency', '_NA_')    ),
                    ( 'PosCurr',  float(acct.get('balance',   0)) ),
                    ( 'PosAvail', float(acct.get('available', 0)) ),
                    ( 'Hold',     float(acct.get('hold',      0)) ),
                ]))
        pos = DataFrame(items)
        return pos

    def load_current_quotes(self):
        bases, quotes, bids, asks = ['USD'], ['USD'], [1.0], [1.0]
        for name, prd in self._products.items():
            bid, ask = prd.bid_ask
            bases.append(prd.base_ccy)
            quotes.append(prd.quote_ccy)
            bids.append(bid)
            asks.append(ask)

        df = DataFrame({
                'Exchange': 'GDAX',
                'BaseCcy':  bases,
                'QuoteCcy': quotes,
                'Bid': bids,
                'Ask': asks,
            }, columns=['Exchange', 'BaseCcy', 'QuoteCcy', 'Bid', 'Ask'])
        return df
