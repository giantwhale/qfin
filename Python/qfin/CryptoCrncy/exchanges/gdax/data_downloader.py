from time import time, sleep

from .._data_downloader import BaseDataDownloader
from .product import Product
from .gdax import GDAX

logger = logging.getLogger(__name__)
DEBUG  = logger.debug


class DataDownloder(BaseDataDownloader, GDAX):

    def __init__(self):
        super(DataDownloder, self).__init__()
        self._product_cls = Product

    def add_product(self, base_ccy, quote_ccy, max_quote_cache=1000, max_trade_cache=1000):
        product = Product(base_ccy, quote_ccy, max_quote_cache, max_trade_cache)
        self._products[product.id] = product

    def start_server(self):

        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:5555")

        while 1:
            try:
                req = socket.recv(zmq.NOBLOCK)
            except zmq.error.Again as e:
                req = None

            if req:
                data = self.process_request(req)
                socket.send(data)

            self.update_market_data()
            sleep(1)

    def process_request(self, req):
        DEBUG("[Market Data Server] Processing: %s" %  req)
        val = random.random()
        DEBUG("[Market Data Server] Random Num: %f" %  val)
        ba  = bytearray(struct.pack("f", val))
        return ba

    def update_market_data(self):
        self.load_trade_data()
        self.load_quote_data()

    def load_trade_data(self):
        print("Load trade data")

    def load_quote_data(self):
        print("Load quote data")


