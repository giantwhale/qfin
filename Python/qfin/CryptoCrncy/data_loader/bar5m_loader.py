from os.path import isfile, join as path_join
from collections import deque

from datetime import datetime, timedelta
utcnow = datetime.utcnow

from pandas import read_csv, concat, DataFrame


from ._base_data_loader import BaseDataLoader
from ...utils.dt_utils import floor_dt
from ... import settings


class Bar5mLoader(BaseDataLoader):

    def __init__(self, maxlen=5, run_type='PROD'):
        self._run_type = run_type
        
        # elements: (time, data<DataFrame>)
        self._data     = deque(maxlen=maxlen)  # Newest first

    def get_bars(self, n=1):
        n = min(n, len(self._data))
        if n == 0:
            raise RuntimeError("No data loaded, @Todo: fault tolerance")
        hld = []
        for idx in range(n):
            _, df = self._data[idx]
            hld.append(df)
        df  = concat(hld, axis=0, ignore_index=True)
        return df

    @property
    def key(self):
        return 'Bar5mLoader'

    @property
    def maxlen(self):
        return self._data.maxlen

    def _load(self, asof=None, account=None):
        if asof is not None:
            raise NotImplementedError("SIM")
        else:
            self._load_prod(account=account)

    def _load_prod(self, account):
        dt   = timedelta(minutes=5)
        root = settings.workspace_cryptoccy

        T    = floor_dt(utcnow(), 5)
        t    = T - timedelta(minutes=5 * self.maxlen) 
        if len(self._data) > 0:
            t1, _ = self._data[0]
            t = max(t, t1) + dt
        
        vt = []
        while t <= T:
            vt.append(t)
            t += dt

        # Load data
        for t in vt:
            ymd = t.strftime('%Y%m%d')
            timestamp = t.strftime('%Y%m%d%H%M%S')
            fname = path_join(root, 'data', 'bar5m', ymd, '%s.csv' % timestamp)
            if not isfile(fname):
                raise RuntimeError("File not exist [%s], @ToDo: fault tolerance" % fname)
            df = read_csv(fname)
            self._data.appendleft((t, df))

    def update(self, loader):
        assert isinstance(loader, type(self))
        maxlen = max(self._maxlen, loader.maxlen)
        if maxlen > self._maxlen:
            self._data = deque(maxlen=maxlen)
