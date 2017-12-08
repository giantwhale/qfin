import logging

import os
from datetime import datetime
utcnow = datetime.utcnow

from collections import deque
import numpy as np
from numpy import nan_to_num as fix
from pandas import DataFrame, Series, concat

from .._product import BaseProduct
from ....utils.dt_utils import floor_dt
from .utils import parse_result
from time import sleep

logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn
ERROR  = logger.error

class Product(BaseProduct):

    _exchange = 'GDAX'

    def __init__(self, base_ccy, quote_ccy='USD'):
        super(Product, self).__init__(base_ccy, quote_ccy)
        self._max_trade_id    = -1
        self._trades = DataFrame()  # A very inefficient implemetation
        
        self._trade_ticks_hist = []
        self._quote_ticks_hist = []

        self._last_trade_id = -1

        self._bid_ask = (-1, -1)  # keep the current bid/ask

    @property
    def name(self):
        return self.base_ccy + '-' + self.quote_ccy

    @property
    def bid_ask(self):
        return self._bid_ask

    @property
    def trade_cols(self):
        return 'TradeTime,Exchange,BaseCcy,QuoteCcy,TradeId,Price,Size,Side'.split(',')

    @property
    def quote_cols(self):
        return 'DataTime,Exchange,BaseCcy,QuoteCcy,Bid,BidSize,BidNumOrders,Ask,AskSize,AskNumOrders'.split(',')

    def update(self, client):
        n1 = self._update_trades(client)
        sleep(0.2)
        n2 = self._update_quotes(client)
        sleep(0.2)
        return n1 + n2

    # Quote Tick
    # -----------------------------------------
    def _add_quote(self, datatime, bid, bid_size, n_bid_orders, ask, ask_size, n_ask_orders):

        bid,  ask = float(bid), float(ask)
        bid0, ask0 = self._bid_ask

        self._bid_ask = (bid if np.isfinite(bid) else bid0
                       , ask if np.isfinite(ask) else ask0)

        self._quote_ticks_hist.append({
                'DataTime'     : datatime
              , 'Exchange'     : 'GDAX'
              , 'BaseCcy'      : self.base_ccy
              , 'QuoteCcy'     : self.quote_ccy
              , 'Bid'          : bid
              , 'BidSize'      : float(bid_size)
              , 'BidNumOrders' : int(n_bid_orders)
              , 'Ask'          : ask
              , 'AskSize'      : float(ask_size)
              , 'AskNumOrders' : int(n_ask_orders)
            })

    def _update_quotes(self, client):
        quotes = client.get_product_order_book(self.pair_name, level=1)
        now    = utcnow()
        quotes = parse_result(quotes)

        if quotes is None:
            print("  No quote data found")
            return 0

        bids = quotes.get('bids')
        if bids is not None and len(bids) > 0:
            bid = bids[0]
        else:
            ERROR("Failed to load bids")
            bid = (np_nan, np_nan, np_nan)

        asks = quotes.get('asks')
        if asks is not None and len(asks) > 0:
            ask = asks[0]
        else:
            ERROR("Failed to load asks")
            ask = (np_nan, np_nan, np_nan)

        self._add_quote(now
                      , bid = bid[0], bid_size = bid[1], n_bid_orders = bid[2]
                      , ask = ask[0], ask_size = ask[1], n_ask_orders = ask[2])
        return 1

    def get_quote_ticks(self, level=1):
        if level == 1:
            if len(self._quote_ticks_hist) == 0:
                return DataFrame(columns=self.quote_cols)
            df = DataFrame(self._quote_ticks_hist, columns=self.quote_cols)
            self._quote_ticks_hist.clear()
            return df
        elif level == 2 or level == 3:
            raise NotImplementedError('level 2/3 data are not implemented yet')
        else:
            raise ValueError('Invalid level [%s], expecting 1, 2, or 3' % str(level))

    # Trade Tick
    # -----------------------------------------
    def _add_trade(self, tradetime, trade_id, price, size, side):
        self._max_trade_id = max(self._max_trade_id, trade_id)
        self._trade_ticks_hist.append({
                      'TradeTime': tradetime
                    , 'Exchange': 'GDAX'
                    , 'BaseCcy' : self.base_ccy
                    , 'QuoteCcy': self.quote_ccy
                    , 'TradeId' : trade_id  # should already be an int
                    , 'Price'   : float(price)
                    , 'Size'    : float(size)
                    , 'Side'    : side
            })

    def _update_trades(self, client):
        try:
            self._update_quotes(client=client)
            result = client.get_product_trades(product_id=self.pair_name)
        except:
            # @Todo: fault tolerance
            result = None

        trades = parse_result(result)
        if trades is None:
            print("  No trades data found")
            return 0

        max_id = self._max_trade_id
        cnt = 0
        for trd in trades:
            trade_id  = int( trd.get('trade_id', -1) )
            if trade_id <= max_id:
                if trade_id < 0:
                    ERROR("failed to parse data.")
                continue

            # time
            tradetime = trd['time']  # what if no value is available?
            if '.' in tradetime:
                tradetime = datetime.strptime(tradetime, '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                tradetime = datetime.strptime(tradetime, '%Y-%m-%dT%H:%M:%SZ')

            # other info
            price = float( trd.get('price', -1) )
            size  = float( trd.get('size' , -1) )
            side  = trd.get('side', 'n')[0]

            # Sanity check
            if tradetime is None or trade_id<0 or price<0 or size<0:
                ERROR("failed to parse data.")
                continue

            self._add_trade(tradetime, trade_id, price, size, side)
            cnt += 1
        return cnt

    def get_trade_ticks(self):
        if len(self._trade_ticks_hist) == 0:
            return DataFrame(columns=self.trade_cols)
        df = DataFrame(self._trade_ticks_hist, columns=self.trade_cols)
        self._trade_ticks_hist.clear()
        return df

    # GDAX Specific Implementation
    # -----------------------------------------
    @property
    def pair_name(self):
        """The same as name"""
        return self.base_ccy + '-' + self.quote_ccy

    def __str__(self):
        return 'GDAX Product [%s]' % self.name


def dt2tx(x):
    return x.hour * 24 * 60 + x.minute * 60 + x.second + 0.001 * (x.microsecond // 1000)


def dt2ltx(x):
    """Seconds passed since 2000-01-01"""
    return (x - datetime(2000, 1, 1)).total_seconds()
