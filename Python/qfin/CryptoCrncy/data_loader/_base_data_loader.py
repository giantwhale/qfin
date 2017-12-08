from abc import ABCMeta, abstractmethod, abstractproperty


class BaseDataLoader(metaclass=ABCMeta):

    def __init__(self, run_type, load_method):
        assert run_type in {'SIM', 'PROD'}
        self._run_type    = run_type 
        self._load_method = load_method

    @abstractproperty
    def key(self):
        """A Unique key for the current data loader object."""
        pass

    @property
    def run_type(self):
        """'SIM' or 'PROD'"""
        return self._run_type

    @property
    def load_method(self):
        """How should we load data"""
        return self._load_method

    # @abstractmethod
    # def get_values(self, asof=None):
    #     """Return values of the requested data
    #    
    #     :param asof: datetime, if None, return the most recent data (PROD).
    #     """
    #     pass

    def load(self, asof=None, account=None):
        """A wrapper around _load(...), in case we need to use cache"""
        if asof is None or self._run_type == 'PROD':
            return self._load(asof=None, account=account)

        if self._load_method == 'file':
            raise NotImplementedError("")
        else:
            raise ValueError("Invalid load method: %s" % self._load_method)

    @abstractmethod
    def _load(self, asof=None, account=None):
        """Return values of the requested data

        :param asof: datetime, if None, return the most recent data (PROD).
        :param account: account from the `trade_engine`. 
        :return: true if data is loaded, false if not
        """
        pass

    def update(self, loader):
        assert isinstance(loader, type(self))
