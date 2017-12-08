import numpy as np
from numpy import nan_to_num as fix


class Algobot(object):

    def execute(self, snap, exchanges, horizon=None):
        grouped = snap.groupby('Exchange')
        for key, df in grouped:
            exch = exchanges.get(key)
            if exch is None:
                raise KeyError("Exchange %s does not exist" % key)

            for _, item in df.iterrows():
                base_ccy  = item['Ccy']
                if base_ccy == 'USD':
                    continue

                quantity  = item['Trade']
                if quantity == 0.0:
                    continue
                elif quantity > 0:
                    side  = 'buy'
                    price = item['Bid']
                else:
                    side  = 'sell'
                    price = item['Ask']
                size = np.abs(quantity)
                
                pos_cur   = item['PosCurr']
                pos_avail = item['PosAvail']

                print("  Sending order [{exch}]: {side:4s} {size} {ccy} @ {prc:8.2f}, CurrPos={cur:6.2f}, AvailPos={avail:6.2f}".format(
                    exch=key, side=side, size=size, ccy=base_ccy, prc=price, cur=pos_cur, avail=pos_avail))
                
                exch.send_order(base_ccy=base_ccy, quote_ccy='USD', 
                    side=side, price=price, size=size, horizon=horizon)

