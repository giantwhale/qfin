from datetime import datetime, timedelta, timezone
from time import sleep
from threading import Event

utcnow = datetime.utcnow

from pandas import DataFrame
from .._exchange import Exchange
from .pusherclient import *


logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn
ERROR  = logger.error


class Bitstamp_Data(Exchange):
    _exchange = 'Bitstamp'

    def __init__(self):

        self.pusher = Pusher('de504dc5763aeef9ff52')
        self._max_trade_id = -1
        self._trades_cols = [
            'TradeTime', 'Exchange', 'BaseCcy', 'QuoteCcy', 'TradeId', 'Side', 'Price', 'Size', 'BuyOrderID',
            'SellOrderID']
        self._trades = DataFrame(columns=self._trades_cols)
        self._trades_event = Event()
        self._trades_event.set()
        self._quotes_history_cols = ['Time', 'Exchange', 'BaseCcy', 'QuoteCcy', 'Side', 'Price', 'Size', 'Depth']
        self._quotes_history = DataFrame(columns=self._quotes_history_cols)
        self._quotes_history_event = Event()
        self._quotes_history_event.set()
        self._live_orders_cols = ['Time', 'Exchange', 'Action', 'BaseCcy', 'QuoteCcy', 'TradeId', 'Side', 'Price',
                                  'Size']
        self._live_orders = DataFrame(columns=self._live_orders_cols)
        self._live_orders_event = Event()
        self._live_orders_event.set()

        # specify 5min bar
        # this is not optimal, need to change the design @FixMe
        self._bar5m = None
        self._bar5m_min_len = 300  # about 1.05 days
        self._bar5m_max_len = 450

        self._rawtrades5m = DataFrame(columns=self._trades_cols)
        self._quotes_history5m = DataFrame(columns=self._quotes_history_cols)
        self._live_orders5m = DataFrame(columns=self._live_orders_cols)

        self._last_trade_id = -1
        # round up self._next_update_tx to the most recent whole 5min
        self._next_update_tx = (dt2ltx(utcnow()) + 299.999) // 300 * 300 + (datetime(2000, 1, 1) - timedelta(hours=1)).timestamp()

    @property
    def name(self):
        return 'Bitstamp'

    def initialize(self):

        currency_pairs = ['', 'btceur', 'eurusd', 'xrpusd', 'xrpeur', 'xrpbtc', 'ltcusd', 'ltceur', 'ltcbtc']

        def trade_handler(currency_pair):
            def parse_trade(trade):
                import ipdb; ipdb.set_trace()
                trade = eval(trade)
                pair = (currency_pair if currency_pair != '' else 'btcusd').upper()
                self._trades_event.wait()
                self._trades.loc[self._trades.shape[0]] = [
                                datetime.fromtimestamp(int(trade['timestamp'])) + timedelta(hours=4), 
                                'Bitstamp',
                                pair[3:], 
                                pair[0:3], 
                                trade['id'], 
                                'bs'[trade['type']],
                                trade['price'], 
                                trade['amount'], 
                                trade['buy_order_id'],
                                trade['sell_order_id']
                            ]
            return parse_trade

        def order_book_handler(currency_pair):
            def parse_order_book(order_book):
                max_level = 5
                order_book = eval(order_book)
                pair = (currency_pair if currency_pair != '' else 'btcusd').upper()
                order_book_time = utcnow()
                bids = [[order_book_time, 'Bitstamp', pair[3:], pair[0:3], 'b', float(each[0]), float(each[1]), idx + 1]
                        for idx, each in enumerate(order_book['bids'][0:5])]
                asks = [[order_book_time, 'Bitstamp', pair[3:], pair[0:3], 's', float(each[0]), float(each[1]), idx + 1]
                        for idx, each in enumerate(order_book['asks'][0:max_level])]
                self._quotes_history_event.wait()
                for each in bids + asks:
                    self._quotes_history.loc[self._quotes_history.shape[0]] = each
            return parse_order_book

        def live_order_handler(currency_pair, action):
            def parse_live_order(live_order):
                live_order = eval(live_order)
                pair = (currency_pair if currency_pair != '' else 'btcusd').upper()
                self._live_orders_event.wait()
                self._live_orders.loc[self._live_orders.shape[0]] = [datetime.fromtimestamp(int(live_order['datetime'])) + timedelta(hours=4),
                                                                     'Bitstamp', action, pair[3:], pair[0:3],
                                                                     live_order['id'], 'bs'[live_order['order_type']],
                                                                     live_order['price'], live_order['amount']]

            return parse_live_order

        def connect_handler(data):

            for each in currency_pairs:
                chan = self.pusher.subscribe('live_trades' + ('_' if each != '' else '') + each)
                chan.bind('trade', trade_handler(currency_pair=each))
                chan = self.pusher.subscribe('order_book' + ('_' if each != '' else '') + each)
                chan.bind('data', order_book_handler(currency_pair=each))
                # chan = self.pusher.subscribe('live_orders' + ('_' if each != '' else '') + each)
                # for each_action in ['order_created', 'order_changed', 'order_deleted']:
                #    chan.bind(each_action, live_order_handler(currency_pair=each, action=each_action))

        self.pusher.connection.bind('pusher:connection_established', connect_handler)
        self.pusher.connect()


    def get_quotes_history5m(self, level=1):

        if level == 1:
            return self._quotes_history5m
        elif level == 2 or level == 3:
            raise NotImplementedError('level 2/3 data are not implemented yet')
        else:
            raise ValueError('Invalid level [%s], expecting 1, 2, or 3' % str(level))

    def get_rawtrades5m(self):

        if len(self._rawtrades5m) == 0:
            return DataFrame(columns=self._trades_cols)

        return self._rawtrades5m

    def get_live_orders5m(self):

        return self._live_orders5m

    def get_bar_data(self, n=100, barsize=5):

        return None

    def update_trades(self):
        now = utcnow()
        tx = now.timestamp()
        #if tx < self._next_update_tx:
        #    sleep(20)
        #    return
        DEBUG("... Updating, current next_update_tx = %.3f" % self._next_update_tx)

        self._trades_event.clear()
        if self._trades.shape[0] != 0:
            self._rawtrades5m = self._trades.copy()
            max_index = max(self._rawtrades5m[self._rawtrades5m['TradeTime'] <= datetime.fromtimestamp(self._next_update_tx)].index)
            self._rawtrades5m = self._rawtrades5m.loc[0:max_index]
            self._trades = self._trades.loc[(max_index+1):]
            self._trades.index = range(0, self._trades.shape[0])
        self._trades_event.set()

        self._quotes_history_event.clear()
        if self._quotes_history.shape[0] != 0:
            self._quotes_history5m = self._quotes_history.copy()
            max_index = max(self._quotes_history5m[self._quotes_history5m['Time'] <= datetime.fromtimestamp(self._next_update_tx)].index)
            self._quotes_history5m = self._quotes_history5m.loc[0:max_index]
            self._quotes_history = self._quotes_history.loc[(max_index+1):]
            self._quotes_history.index = range(0, self._quotes_history.shape[0])
        self._quotes_history_event.set()

        self._live_orders_event.clear()
        if self._live_orders.shape[0] != 0:
            self._live_orders5m = self._live_orders.copy()
            max_index = max(self._live_orders5m[self._live_orders5m['Time'] <= datetime.fromtimestamp(self._next_update_tx)].index)
            self._live_orders5m = self._live_orders5m.loc[0:max_index]
            self._live_orders = self._live_orders.loc[(max_index+1):]
            self._live_orders.index = range(0, self._live_orders.shape[0])
        self._live_orders_event.set()

        self._next_update_tx += 300

    def __str__(self):

        return 'Bitstamp_Data'

    def update(self):
        pass

    def available_usd(self):
        pass

    def best_bid_ask(self):
        pass

    def cancel_all_orders(self):
        pass

    def positions(self):
        pass

    def product_cls(self):
        pass

    def send_order(self, *args, **kwargs):
        pass

    def get_trade_data(self, asof=None, horizon_m=1, columns=None):
        raise NotImplementedError('not implemented in production yet')

    def get_trade_ticks(self):
        """Used in DataEngine to get a list of all trade tick blocks"""
        pass

    def get_quote_ticks(self):
        """Used in DataEngine to get a list of all quote tick blocks"""
        pass


def dt2tx(x):
    return x.hour * 24 * 60 + x.minute * 60 + x.second + 0.001 * (x.microsecond // 1000)


def dt2ltx(x):
    """Seconds passed since 2000-01-01"""
    return (x - datetime(2000, 1, 1)).total_seconds()
