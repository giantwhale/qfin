import logging

import os
from os import makedirs
from os.path import join as path_join
from time import sleep
from datetime import datetime
utcnow = datetime.utcnow

import pandas as pd
from pandas import DataFrame, concat

from .exchanges import Exchange

from .fileutils import save_signal, load_signal

from .mongo_helper import MongoHelper
from ..utils.serialize import serialize, deserialize
from ..utils.dt_utils import floor_dt


logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn


class TAQHelper(object):

    def __init__(self):
        self._daily_trades_1m = {}
        self._daily_quotes_1m = {}
        self._cache_date = datetime(1970, 1, 1)

        self._mongo = MongoHelper()

        self._trade_cols = 'TradeTime,Exchange,BaseCcy,QuoteCcy,TradeId,Price,Size,Side'.split(',')
        self._quote_cols = 'DataTime,Exchange,BaseCcy,QuoteCcy,Bid,BidSize,BidNumOrders,Ask,AskSize,AskNumOrders'.split(',')
        
    def get_taq_data_prod(self, asof=None):
        if asof is not None:
            asof = floor_dt(asof, minutes=1)
            res = self._mongo.database['taq1m'].find_one({
                'BarTime': {'$eq': asof}
            })
        else:
            raise NotImplementedError("What should be the logic here?")

        if res is None:
            return None, None

        data = res.get('Data', {})

        z = data.get('tradeticks')
        tradeticks = deserialize(z) if z is not None else None
    
        z = data.get('quoteticks')
        quoteticks = deserialize(z) if z is not None else None
        
        return tradeticks, quoteticks

    def save_taq_data_prod(self, asof=None, tradeticks=None, quoteticks=None):
        assert isinstance(asof, datetime), "asof must be an datetime"
        asof = floor_dt(asof, minutes=1)
        doc = {}
        if tradeticks is not None:
            tradeticks = tradeticks[self._trade_cols].copy()
            doc['tradeticks'] = serialize(tradeticks)
        if quoteticks is not None:
            quoteticks = quoteticks[self._quote_cols].copy()
            doc['quoteticks'] = serialize(quoteticks)
        self._mongo.append('taq1m', asof, doc)

    def get_taq_data(self, asof=None, horizon_m=1, trade_columns=None, quote_columns=None):
        """In simulation, only the same day is supported"""
        asof = asof.replace(second=0, microsecond=0)
        if self._cache_date != asof:
            self._make_cache(asof)

        if trade_columns is None and quote_columns is None:
            raise ValueError("at least one of trade_columns and quote_columns must be set")
        
        trades = self._load_data(asof, horizon_m, self._daily_trades_1m, trade_columns) if trade_columns is not None else None
        quotes = self._load_data(asof, horizon_m, self._daily_quotes_1m, quote_columns) if quote_columns is not None else None

        return trades, quotes

    def save_taq_data(self, asof, tradeticks=None, quoteticks=None):
        ymd_hms = asof.strftime('%Y%m%d_%H%M%S')
        save_signal(tradeticks[self._trade_cols], ymd_hms, 'tradeticks', 'rawdata')
        save_signal(quoteticks[self._quote_cols], ymd_hms, 'quoteticks', 'rawdata')

    def _load_data(self, asof, horizon_m, dct, columns=None):
        if horizon_m == 1:
            return dct.get(asof.strftime('%H%M'))

        hld     = []
        for n in range(horizon_m):
            asof = asof - timedelta(minutes=n)
            key  = asof.strftime('%H%M')
            df   = dct.get(key)
            if df is not None:
                hld.append(df)

        if len(hld) == 0:
            return None

        df = concat(hld, axis=0, ignore_index=True)
        if columns is not None:
            df = df[columns].copy()
        return df

    def _make_cache(self, asof):
        self._daily_trades_1m.clear()
        self._daily_quotes_1m.clear()

        ymd = asof.strftime('%Y%m%d')
        
        # process trade data
        df = load_signal(ymd, 'tradeticks', 'storage/rawdata')
        if df is not None:
            df['BarTimeStart'] = pd.to_datetime(df['TradeTime']).apply(lambda x: x.hour * 100 + x.minute)
            grouped = df.groupby('BarTimeStart')
            self._daily_trades_1m = {'%04d' % bartime : grp for bartime, grp in grouped}

        # process quote data
        df = load_signal(ymd, 'quoteticks', 'storage/rawdata')
        if df is not None:
            df['BarTimeStart'] = pd.to_datetime(df['DataTime']).apply(lambda x: x.hour * 100 + x.minute)
            grouped = df.groupby('BarTimeStart')
            self._daily_quotes_1m = {'%04d' % bartime : grp for bartime, grp in grouped}
