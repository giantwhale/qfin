from .._execution import BaseExecution, BaseOrder

import logging
from numpy import float as np_float

logger = logging.getLogger(__name__)
WARN   = logger.warn
INFO   = logger.info
DEBUG  = logger.debug
ERROR  = logger.error



class GdaxOrder(BaseOrder):

    def __init__(self, order_id, base_ccy, quote_ccy, side, price, size):
        self._order_id   = order_id
        self._side  = side
        self._price = np_float(price)
        self._size  = np_float(size)
        self._base  = base_ccy.upper()
        self._quote = quote_ccy.upper()

        assert self._side in {'buy', 'sell'}
        assert self._price > 0 and self._size > 0
        assert len(self._base) == len(self._quote) == 3

    @property
    def pairname(self):
        return self._base + "-" + self._quote

    @property
    def side(self):
        return self._side

    @property
    def price(self):
        return self._price

    @property
    def size(self):
        return self._size

    @property
    def exchange_name(self):
        return 'GDAX'

    @property
    def order_id(self):
        return self._order_id

    def _set_order_id(self, order_id):
        return self._order_id


class GdaxExecution(BaseExecution):
    """
    GDAX execution engine
    - need private client for  place/cancel orders
    - need public client for snapshotting data
    - need streaming client for streaming data
    """

    def __init__(self):
        super(GdaxExecution, self).__init__()

    def cancel_order(self, private_client, order_id):
        if not order_id in self._orders:
            ERROR("Order id [%s] is not in local set of active order")
        response = private_client.cancel_order(order_id)
        # sample return {'message': 'Invalid order id'}
        del self._orders[order_id]
        if isinstance(response, dict) and 'message' in response:
            DEBUG("Cancel order response: [%s]" % response)

    def cancel_all_orders(self, private_client):
        response = private_client.cancel_all() # return a list of canceled order ids
        if response:
            # save and delete each order from local list
            for order_id in response:
                order = self._orders.get(order_id)
                if order is not None:
                    self.save_record(self._orders[order_id])
                    del self._orders[order_id]
                else:
                    # Sometime we manually send orders, we should properly label 
                    # these orders here 
                    WARN("Order is not recorded @FixMe")
                    print('len(orders) = %d' % len(self._orders))
        else:
            WARN("No order got canceled")

        if self._orders:
            INFO("Local set of active orders is cleaned up")
        else:
            WARN("Local set of active orders is not clean after cancel all")

    def send_order(self, client, base_ccy, quote_ccy, side, price, size, horizon=None):
        # order is a dict of at least three fields: price, size, product_id
        # horizon is not implemented
        order = GdaxOrder(order_id=None, base_ccy=base_ccy, quote_ccy=quote_ccy, 
                          side=side, price=price, size=size)

        fn    = client.buy if order.side == 'buy' else client.sell
        resp  = fn(price      = r'%.2f' % order.price, 
                   size       = r'%.2f' % order.size, 
                   product_id = order.pairname)
        if 'id' in resp:
            order._set_order_id(resp['id'])
            self._orders[order.order_id] = order
            return order
        elif 'mesage' in resp:
            WARN("Unsuccessful order placement because of %s" % resp['message'])
        else:
            return None

    def save_record(self):
        raise NotImplementedError

    def get_fill(self, client, order_id):
        pass

    def get_all_fills(self, client):
        pass
