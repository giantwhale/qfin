import gdax, time, sys, os
from datetime import datetime
from collections import OrderedDict
import numpy as np
import math
currency_pair = "BTC-USD"

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = logger.debug
INFO = logger.info
WARN = logger.warn
ERROR = logger.error

class Bar(object):
    
    def __init__(self, open=np.nan, close=np.nan, high=np.nan, low=np.nan, vwap=np.nan,
        buy_vol=0, sell_vol=0, n_buys=0, n_sells=0):
        self.open       = open
        self.close      = close
        self.high       = high
        self.low        = low
        self.vwap       = vwap
        self.buy_vol    = buy_vol
        self.sell_vol   = sell_vol
        self.n_buys     = n_buys
        self.n_sells    = n_sells

    def print(self):
        INFO("OPEN=%.2f CLOSE=%.2f HIGH=%.2f LOW=%.2f VWAP=%.2f BUY_VOL=%.2f SELL_VOL=%.2f N_BUYS=%d N_SELLS=%d" % 
           (self.open, self.close, self.high, self.low, self.vwap, 
            self.buy_vol, self.sell_vol, self.n_buys, self.n_sells))

    def get_csv_header(self):
        return "OPEN,CLOSE,HIGH,LOW,VWAP,BUY_VOL,SELL_VOL,N_BUYS,N_SELLS"   

    def get_csv_entry(self):
        return "{},{},{},{},{},{},{},{},{}".format( 
            self.open, self.close, self.high, self.low, self.vwap, 
            self.buy_vol, self.sell_vol, self.n_buys, self.n_sells)


class myWebsocketClient(gdax.WebsocketClient):

    def __init__(self):
        gdax.WebsocketClient.__init__(self)
        self.reset_bar()
    
    def reset_bar(self):
        self.bar = Bar()

    def on_open(self):
        self.url = "wss://ws-feed.gdax.com/"
        self.products = [currency_pair]
    
    def on_message(self, msg):
        if msg['type'] != 'match':
            return
        price           = float(msg['price'])
        size            = float(msg['size'])
        side            = str(msg['side'])
        vwap            = 0 if np.isnan(self.bar.vwap) else self.bar.vwap
        self.bar.vwap   = (vwap * (self.bar.buy_vol + self.bar.sell_vol) + price * size) / \
                            (self.bar.buy_vol + self.bar.sell_vol + size)
        if np.isnan(self.bar.open): self.bar.open = price
        if np.isnan(self.bar.high): self.bar.high = price
        else: self.bar.high = price if price > self.bar.high else self.bar.high
        if np.isnan(self.bar.low): self.bar.low = price
        else: self.bar.low = price if price < self.bar.low else self.bar.low
        self.bar.close  = price
        # buy/sell in GDAX match messages is from liquidity maker's point of view
        # to unify definitions, we use liquidity taker's point of view
        if side == 'buy':
            self.bar.n_sells += 1
            self.bar.sell_vol += size
        elif side == 'sell':
            self.bar.n_buys += 1
            self.bar.buy_vol += size
        else:
            WARN("MATCH message is neither buy nor sell")

 
def main(wsClient, f):
    wsClient.start()
    interval = 5 * 60 # in seconds
    tick = (math.floor(time.time() / interval) + 1) * interval + 1
    while True:
        while time.time() < tick:
            continue
        wsClient.bar.print()
        wsClient.reset_bar()
        tick = (math.floor(time.time() / interval) + 1) * interval + 1
        bar_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        f.write("{},{}\n".format(bar_end_time, wsClient.bar.get_csv_entry()))
        INFO("BAR_END_TIME={}".format(bar_end_time))


def cleanup(wsClient, f):
    INFO("Clean up ...")
    wsClient.close()
    f.close()


if __name__ == '__main__':
    wsClient = myWebsocketClient()
    output_file = 'test.csv'
    flag = os.path.exists(output_file)
    f = open(output_file, 'a+')
    if not flag: f.write("BAR_END_TIME" + wsClient.bar.get_csv_header() + "\n")
    try:
        main(wsClient, f)
    except KeyboardInterrupt:
        cleanup(wsClient, f)





