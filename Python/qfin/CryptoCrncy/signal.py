from numpy import nan_to_num as fix
from numpy import random
from pandas import DataFrame, Series


class Signal(object):
    pass


class SimpleSignal(Signal):

    def __init__(self, name, key, freq):
        self._name      = str(name)
        self._key       = str(key)
        self._freq      = freq  # ??? @ToDo: this is used to round time stamps

    def update_account(self, account, timestamp):
        pass


class RandomSimpleSignal(SimpleSignal):

    def __init__(self, name, key='', freq='5m'):
        super(RandomSimpleSignal, self).__init__(name, key, freq)

    def load(self, exchanges):
        v_exch, v_ccy, v_sig = [], [], []
        for name, exch in exchanges.items():
            for ccy in exch.ccy_list:
                if ccy == 'USD':
                    continue
                v_exch.append(name)
                v_ccy .append(ccy)
                v_sig .append(random.normal(0.0, 1.0))
        sig = DataFrame({
                'Exch' : v_exch,
                'Ccy'  : v_ccy ,
                'Alpha': v_sig ,
                }, columns='Exch Ccy Alpha'.split())
        return sig

    def update_account(self, asof, account, exchanges):
        # @ToDo: let's add timestamp as well, for backtest purposes
        sig = self.load(exchanges)

        # sig.join(account.univ, 
        sig1 = account.univ.merge(sig, how='left', on=['Exch', 'Ccy'])
        account.set_alpha(fix(sig1['Alpha'].values))

        return account
