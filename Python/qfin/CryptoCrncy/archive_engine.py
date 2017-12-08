import os
import re
from os import makedirs, listdir, remove
from os.path import join as path_join, exists as path_exists
import fnmatch
from datetime import datetime, timedelta
utcnow = datetime.utcnow

from pandas import DataFrame, read_csv, concat

from .fileutils import data_dir_path, archive_dir_path
from .exchanges import Exchange
from .mongo_helper import MongoHelper 

import logging
logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn


class _DataItem(object):

    def __init__(self, name, datatype, exchanges, rebal_hor, check_completeness):
        self.name       = name
        self.datatype   = datatype
        self.check_completeness = bool(check_completeness)
        self.exchanges  = exchanges

    def src_dest_dir_pairs(self, asof):
        if self.exchanges is None:
            return [(data_dir_path(asof, self.name, self.datatype, exch=None),
                     archive_dir_path(self.name, self.datatype, exch=None), )]
        else:
            return [(data_dir_path(asof, self.name, self.datatype, exch=exch),
                     archive_dir_path(self.name, self.datatype, exch=exch)) for exch in self.exchanges]


class ArchiveEngine(object):
    """
    Archive data from 1, 2, 3 days ago. 
    """
    def __init__(self):
        self._data_items = [] 

        self._mongo = MongoHelper()
        self._mongo_cleaners = []


    def add_data_item(self, name, datatype, exchanges=None, rebal_hor=300, check_completeness=False):
        item = _DataItem(name, datatype, exchanges, rebal_hor, check_completeness)
        self._data_items.append(item)

    def add_mongo_cleaner(self, name, keep_min=1440):
        self._mongo_cleaners.append((name, keep_min))

    def run(self, asof=None, lookback=5):
        for name, keep_min in self._mongo_cleaners:
            self._mongo.delete_old(name, keep_min)

        asof = asof or utcnow()
        for idx in reversed(range(lookback)):
            d = asof - timedelta(days=idx)
            print('Processing {} '.format(d.strftime('%Y-%m-%d')))
            for item in self._data_items:
                self.aggregate_data(d, item)


    def aggregate_data(self, asof, item):
        ymd = asof.strftime('%Y%m%d')

        for src_dir, dest_dir in item.src_dest_dir_pairs(asof):
            os.makedirs(dest_dir, exist_ok=True)
            df = self._concat(src_dir)
            if df is not None:
                outfile = path_join(dest_dir, ymd + '.csv')
                df.to_csv(outfile, index=False, float_format='%g')
            print("  ... loaded {:6d} for {:s}".format(df.shape[0] if df is not None else 0, item.name))
            
    def _concat(self, dirname, file_list=None):
        if not path_exists(dirname):
            return None

        ptn = re.compile(r'^\d{8}\_\d{6}')
        if file_list is None:
            file_list = os.listdir(dirname)
            file_list = [f for f in file_list if ptn.search(f)]

        if len(file_list) == 0:
            return None

        hld = []
        for f in file_list:
            fname = path_join(dirname, f)
            if path_exists(fname):
                df = read_csv(fname)
                hld.append(df)

        if len(hld) == 0:
            return None
        else:
            # ! successful
            concated = concat(hld, axis=0, ignore_index=True)
            return concated

