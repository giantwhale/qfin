from collections import OrderedDict
import numpy  as np
import pandas as pd
from pandas import DataFrame, Series, concat


class Universe(object):

    def __init__(self, names, ccy_list):
        self._univ = DataFrame(OrderedDict([
                ( 'Exch', names    ),
                ( 'Ccy' , ccy_list ),
            ]))

    @classmethod
    def build(cls, exchanges, init_account=None):
        assert isinstance(exchanges, dict)

        v_names = []
        v_ccys  = []
        for name, exch in exchanges.items():
            v_names.extend([name for _ in range(exch.num_ccy)])
            v_ccys.extend(exch.ccy_list)

        if init_account is not None:
            raise NotImplementedError("Universe from init_account")

        return cls(v_names, v_ccys)

    @property
    def data(self):
        return self._univ

    @property
    def exch_names(self):
        return self._univ['Exch'].unique().tolist()

    @property
    def size(self):
        return self._univ.shape[0]

    def __str__(self):
        return 'Universe:\n' + str(self._univ)

    def __repr__(self):
        return self.__str__()
