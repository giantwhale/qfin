from abc import ABCMeta, abstractmethod, abstractproperty
from pandas import DataFrame
from io import BytesIO
from copy import copy
from datetime import datetime, timedelta
utcnow = datetime.utcnow
from pymongo import MongoClient
import pickle
import traceback
import logging

logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn



class MongoHelper(object):

    def __init__(self):
        self._client = MongoClient('localhost', 27017)
        self._db     = self._client.crypto_ccy

    @property
    def database(self):
        return self._db

    def append(self, name, bartime, doc, **kwargs):
        assert isinstance(bartime, datetime)
        INFO('[Mongo] append to {} at {}'.format(name, bartime.strftime('%Y-%m-%d %H:%M:%S')))
        record = { 'BarTime': bartime, 'Data': doc}

        for k, v in kwargs.items():
            record[k] = v

        collection  = self._db[name]
        try:
            inserted_id = collection.insert_one(record).inserted_id
        except:
            WARN("  Failed to insert record")
            traceback.print_exc()
            inserted_id = None
        return inserted_id

    def delete_old(self, name, keep_min = 1440):
        bartime = utcnow() - timedelta(minutes = max(0, keep_min))
        collection  = self._db[name]
        try:
            res = collection.delete_many({ 'BarTime': { '$lt': bartime } }) 
            INFO("Deleted {} docs from collection {}".format(res.deleted_count, name))
        except:
            WARN("  Failed to delete old records")
            traceback.print_exc()

    def fetch(self, name, asof, data, **kwargs):
        pass
