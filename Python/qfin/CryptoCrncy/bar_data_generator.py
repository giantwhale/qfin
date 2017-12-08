from datetime import datetime, timedelta
utcnow = datetime.utcnow

from numpy import sum as np_sum, max as np_max, min as np_min, maximum, minimum, nan as np_nan

from time import sleep
from pandas import concat, DataFrame

from .exchanges import Exchange
from .fileutils import save_signal
from .mongo_helper import MongoHelper
from .taq_helper import TAQHelper
from ..utils.serialize import serialize
from ..utils.dt_utils import floor_dt
from .. import settings


import logging
logger = logging.getLogger(__name__)
INFO = logger.info


ymd2     = lambda x: x.strftime('%Y-%m-%d')
ymd_hms2 = lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
hm       = lambda x: x.strftime('%H:%M')
hms      = lambda x: x.strftime('%H:%M:%S')

def BarDataGenerator(run_type=None):
    run_type = run_type or settings.run_type
    if run_type == 'SIM':
        obj = _BarDataGeneratorSIM()
    elif run_type == 'PROD':
        obj = _BarDataGeneratorPROD()
    else:
        raise ValueError('Invalid run_type: %s' % run_type)
    return obj


class _BarDataGenerator(object):

    def __init__(self):
        self._run_type = settings.run_type

        self._max_horizon_m  = 0
        self._taq_helper     = TAQHelper()

    def run(self, startdate=None, enddate=None):
        raise NotImplementedError("Not to be called in the Base Class")


class _BarDataGeneratorSIM(_BarDataGenerator):

    def __init__(self):
        super(_BarDataGeneratorSIM, self).__init__()
        self._bar_processors = []

    def add_frequency(self, minutes):
        minutes = int(minutes)
        assert minutes > 0
        self._bar_processors.append(_BarProcessorSIM(minutes))
        self._max_horizon_m = max(self._max_horizon_m, minutes)

    def run(self, startdate, enddate):
        assert startdate <= enddate
        asof = datetime(startdate.year, startdate.month, startdate.day)
        enddate = enddate.replace(hour=23, minute=59, second=0, microsecond=0)

        assert len(self._bar_processors) > 0
        trade_cols = ['Exchange', 'BaseCcy', 'QuoteCcy', 'Price', 'Side', 'Size']

        prev_hour = -1
        while asof <= enddate:
            if prev_hour != asof.hour:
                INFO("processing %s" % ymd_hms2(asof))
                prev_hour = asof.hour

            tradeticks, _ = self._taq_helper.get_taq_data(
                asof, horizon_m=1, trade_columns=trade_cols, quote_columns=None)

            for proc in self._bar_processors:
                proc.append(asof=asof, data=tradeticks)

            asof += timedelta(minutes=1)


class _BarDataGeneratorPROD(_BarDataGenerator):

    def __init__(self):
        super(_BarDataGeneratorPROD, self).__init__()
        self._bar_processor = _BarProcessorPROD(5)

    def add_frequency(self, minutes):
        raise RuntimeError("In PROD mode, we only allow 5min bars")

    def load_taq(self, asof):
        trades, _ = self._taq_helper.get_taq_data_prod(asof)
        return trades, None

    def run(self, *args, **kwargs):
        one_min  = timedelta(minutes=1)
        two_min  = timedelta(minutes=2)
        five_min = timedelta(minutes=5)
        
        bartime = floor_dt(utcnow(), minutes=5)
        T       = bartime
        while True:
            now = utcnow()
            if now >= T + two_min:
                # load historical bar, if not available, record None
                trades, _ = self.load_taq(T)
                nrows = trades.shape[0] if trades is not None else 0
                INFO("[{}] Search for BarTime {}: loaded 5min bar (hist, {:4d} rows)".format(hms(utcnow()), hm(T), nrows))
            elif now >= T + one_min:
                # load the current bar, if not available, try sleep a bit and load again
                trades, _ = self.load_taq(T)
                if trades is None:
                    sleep(1)
                    continue
                nrows = trades.shape[0] if trades is not None else 0
                INFO("[{}] Search for BarTime {}: loaded 5min bar (curr, {:4d} rows)".format(hms(utcnow()), hm(T), nrows))
            else:
                # wait till the next whole minute
                delta = max(0, (T + one_min - utcnow()).total_seconds())
                INFO("  Wait till the next whole minute for %.2f seconds" % (delta + 0.25))
                sleep(delta + 0.25) 
                continue
            
            self._bar_processor.append(trades)
            if utcnow() >= bartime + five_min:
                self._bar_processor.publish(bartime)
                bartime += five_min
            T = T + timedelta(minutes=1)



# Bar Processor 
# ===================================================

class _BarProcessor(object):
    
    def __init__(self, minutes):
        minutes = int(minutes)
        assert minutes > 0

        self._taq_helper   = TAQHelper()

        self._minutes      = minutes
        self._data         = []
        self._curr_bartime = None
        self._data_name    = 'Bar%dm' % minutes

    def _process(self):
        valid_data = [x for x in self._data if x is not None]
        self._data.clear()

        if len(valid_data) == 0:
            return 
        m = concat(valid_data, axis=0, ignore_index=True)
        grouped = m.groupby(['Exchange', 'BaseCcy', 'QuoteCcy'])

        v_exch    = []
        v_base    = []
        v_quote   = []
        v_vwap    = []
        v_volume  = []
        v_buyvol  = []
        v_sellvol = []
        v_ntrades = []
        v_low     = []
        v_high    = []
        v_open    = []
        v_close   = []

        for (exch, base, quote), grp in grouped:
            px      = grp['Price'].fillna(0.0).values
            good    = px > 0.0001
            good_px = px[good]
            side    = grp['Side'].fillna('').astype('S').values
            size    = grp['Size'].fillna(0.0).values

            valid   = np_sum(good) > 0

            v_exch   .append( exch  )
            v_base   .append( base  )
            v_quote  .append( quote )
            v_vwap   .append( np_sum(px * size) / maximum(0, np_sum(size * good)) )
            v_volume .append( np_sum(size)                         )
            v_buyvol .append( np_sum(size * (side == 'b'))         )
            v_sellvol.append( np_sum(size * (side == 's'))         )
            v_ntrades.append( grp.shape[0]                         )
            v_low    .append( np_min(good_px) if valid else np_nan )
            v_high   .append( np_max(good_px) if valid else np_nan )
            v_open   .append( good_px[0]  if valid else np_nan     )
            v_close  .append( good_px[-1] if valid else np_nan     )

        df = DataFrame({
            'Date': self._curr_bartime.date(),
            'BarTimeStart': self._curr_bartime.strftime('%H%M'),
            'Exch':     v_exch,
            'BaseCcy':  v_base,
            'QuoteCcy': v_quote,
            'Vwap':     v_vwap,
            'Volume':   v_volume,
            'BuyVol':   v_buyvol,
            'SellVol':  v_sellvol,
            'NTrades':  v_ntrades,
            'Low':      v_low,
            'High':     v_high,
            'Open':     v_open,
            'Close':    v_close,
            }, columns='Date,BarTimeStart,Exch,BaseCcy,QuoteCcy,Vwap,Volume,BuyVol,SellVol,NTrades,Low,High,Open,Close'.split(','))


        self.save_data(df)

    def save_data(self, df):
        raise NotImplementedError("base class shouldn't be called")


class _BarProcessorSIM(_BarProcessor):

    def __init__(self, minutes):
        super(_BarProcessorSIM, self).__init__(minutes)

    def append(self, asof, data):
        if len(self._data) == 0:
            self._curr_bartime = asof

        self._data.append(data)
        if len(self._data) == self._minutes:
            self._process()

    def save_data(self, df):
        ymd_hms = self._curr_bartime.strftime('%Y%m%d%H%M%S')
        save_signal(df, ymd_hms, self._data_name, 'storage/signals')

    def __repr__(self):
        return 'BarProcessorSIM(%d)' % self._minutes


class _BarProcessorPROD(_BarProcessor):
    # This implementation is a bit weird ...
    # It basically passes control to other object

    def __init__(self, minutes):
        super(_BarProcessorPROD, self).__init__(minutes)
        self._mongo   = MongoHelper()
        self._bartime = None

    def append(self, data):
        self._data.append(data)

    def publish(self, bartime):
        self._curr_bartime = bartime
        self._process()

    def save_data(self, df):
        doc = {
            'BarTime': self._curr_bartime,
            'Data': serialize(df)
        }
        self._mongo.append('bar5m', self._curr_bartime, doc)

    def __repr__(self):
        return 'BarProcessorPROD(%d)' % self._minutes

