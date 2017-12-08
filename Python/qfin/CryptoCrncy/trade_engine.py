# encoding: utf-8
import logging

import os
from os.path import join as path_join

from time import time, sleep
from datetime import datetime, timedelta
utcnow = datetime.utcnow

from collections import OrderedDict
from pandas import DataFrame, concat, pivot_table, read_csv
import numpy as np
import pymongo
from numpy import inf, nan_to_num as fix, maximum, minimum

from .exchanges import Exchange
from .universe import Universe
from .order_book import OrderBook
from .signal import Signal
from .data_loader._base_data_loader import BaseDataLoader
from .optimizer import LinearOptimizer
from .algobot import Algobot
from .mongo_helper import MongoHelper
from .fileutils import save_signal
from ..utils.dt_utils import floor_dt, ceil_dt
from ..utils.np_utils import non_na
from ..utils.serialize import deserialize


logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info


def vconcat(items):
    return concat(items, axis=0, ignore_index=True)


class TradeEngine(object):

    def __init__(self, rebal_hor=60, offset=5):
        self._exchanges     = OrderedDict()

        # self._signal        = None  # @deprecated
        self._mongo         = MongoHelper()

        self._optimizer     = LinearOptimizer()
        self._algobot       = Algobot()

    # Setters
    def add_exch(self, exch):
        assert isinstance(exch, Exchange)
        self._exchanges[exch.name] = exch

    def initialize(self):
        for _, exch in self._exchanges.items():
            exch.initialize()

    # Orders
    def send_order(self, exch, base_ccy, quote_ccy, side, price, size, horizon=None):
        if isinstance(exch, str):
            exch = self._exchanges[exch]
        assert isinstance(exch, Exchange)
        order = exch.send_order(base_ccy=base_ccy, quote_ccy=quote_ccy, 
                    side=side, price=price, size=size, horizon=horizon)
        return order

    def cancel_all_orders(self, exchanges=None):
        exchanges = self._exchanges if exchanges is None else exchanges
        for name, exch in exchanges.items():
            exch.cancel_all_orders()

    def build_init_account(self, exchanges):
        items = []
        for name, exch in exchanges.items():
            u = DataFrame(OrderedDict([
                    ('Exchange', name     ),
                    ('Ccy',  exch.ccy_list),
                ]))
            items.append(u)
        account = vconcat(items)
        account.insert(0, 'Date',    utcnow().date())
        account.insert(1, 'BarTime', utcnow().strftime('%H%M'))

        account['Alpha'   ] = 0.0
        account['PosTrgt' ] = np.nan
        account['PosFinal'] = np.nan
        account['IsRestricted'] = 0

        return account

    def load_account(self, init_account, exchanges):
        account = init_account.copy()

        items = []
        for name, exch in exchanges.items():
            pos = exch.load_current_positions()
            pos['DataTime'] = utcnow()
            items.append(pos)
        pos = vconcat(items)

        keys = ['Exchange', 'Ccy']
        m = account[keys].merge(pos, how='left', on=keys)
    
        cols = ['PosCurr', 'PosAvail', 'Hold']
        for col in cols:
            account[col] = fix(m[col].values)

        account['PosTrgt'] = non_na(account['PosTrgt'].values, account['PosCurr'].values)
        return account

    def load_alpha(self, account, bartime, max_wait=20):
        stime   = time()
        sigtime = bartime - timedelta(minutes=5)
        account['Alpha'] = 0.0
        while 1:
            cursor = self._mongo.database['alpha'].find({
                        'BarTime': {'$gte': sigtime}
                    })
            if cursor.count() > 0:
                cursor = cursor.sort('BarTime', pymongo.ASCENDING).limit(1)
                df     = deserialize(cursor[0]['Data'])
                if 'Alpha' in df.columns:
                    univ   = self.build_univ(account)                
                    alpha  = univ.merge(df, how='left', on=univ.columns.tolist())
                    account['Alpha'] = fix(alpha)
                    break
            if time() - stime > max_wait:
                break
            else:
                print('.', end='')
                sleep(3)
        return account

    def build_univ(self, account):
        univ = DataFrame({
                'Exchange': account['Exchange'],
                'BaseCcy':  account['Ccy'],
            }, columns=['Exchange', 'BaseCcy'])
        return univ

    def add_quotes(self, account, exchanges):
        res = self._mongo.database['current_quote'].find_one({
                'BarTime': {'$gt': utcnow() - timedelta(seconds=60)}
            })
        if res is None:
            account['Bid'] = np.nan
            account['Ask'] = np.nan
        else:
            quotes = deserialize(res['Data'])
            quotes = quotes.ix[quotes['QuoteCcy'] == 'USD', :].copy()
            univ   = self.build_univ(account)
            q      = univ.merge(quotes, how='left', on=univ.columns.tolist())
            account['Bid'] = q['Bid']
            account['Ask'] = q['Ask']
        account['Mid'] = fix(account['Bid'].values + account['Ask'].values)
        return account

    def calculate_trades(self, account):
        trades   = fix(account['PosFinal'].values - account['PosCurr'].values)
        trades   = np.round(trades, 2)
        max_sell = maximum(0.0, account['PosAvail'].values)
        trades   = maximum(trades, -max_sell)
        return trades

    def save_curr_snap(self, now, account):
        save_signal(account, now.strftime('%Y%m%d_%H%M00'), 'account.account_hist')

    # Main Loop
    def start_server(self):
        INFO("Start TradeEngine Server")
        self.initialize()

        one_min    = timedelta(minutes=1)
        five_min   = timedelta(minutes=5)

        exchanges  = self._exchanges  # functional style

        account    = self.build_init_account(exchanges)

        endtime    = utcnow().replace(hour=23, minute=50, second=0, microsecond=0)
        next_min   = floor_dt(utcnow(), minutes=1) + one_min
        next_rebal = floor_dt(utcnow(), minutes=5) + five_min
        while utcnow() < endtime:
            now = utcnow()
            account['Date'   ] = now.date()
            account['BarTime'] = now.strftime('%H%M')

            INFO("===== Rebalance at {}, Next rebal {} =====".format(
                now.strftime('%H:%M'), next_rebal.strftime('%H:%M')))

            INFO("  ... cancel all orders")
            self.cancel_all_orders(exchanges)

            INFO("  ... updating current positions")
            account = self.load_account(account, exchanges)

            if now >= next_rebal:
                next_rebal = floor_dt(utcnow(), minutes=5) + five_min
                INFO("  ... loading alpha")
                account = self.load_alpha(account, next_rebal, max_wait=30)
                INFO("  ... load quotes")
                account = self.add_quotes(account, exchanges)
                account = self._optimizer.optimize(account)
            else:
                INFO("  ... intermediate step, skip loading alpha")
                INFO("  ... load quotes")
                account = self.add_quotes(account, exchanges)

            INFO("  ... calculating trades")
            account['Trade'] = self.calculate_trades(account)

            INFO("  ... sending orders")
            self._algobot.execute(account[['Exchange', 'Ccy', 'Trade', 'Bid', 'Ask']], exchanges)

            INFO("  ... saving account to disk")
            self.save_curr_snap(now, account)

            # sleep till the next minute
            t = (next_min - utcnow()).total_seconds()
            if t > 0.1:
                INFO("  ... ... sleep for %.2f seconds" % (t+0.1))
                sleep(t + 0.1)

            next_min += one_min

        # EOD Zone
        account = self.run_eod_process(now)



