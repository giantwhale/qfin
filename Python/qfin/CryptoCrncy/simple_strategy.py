from abc import ABCMeta, abstractmethod, abstractproperty

class SimpleStrategy(metaclass=ABCMeta):

    @abstractmethod
    def calculate_alpha(self, account, loaders):
        pass

    @abstractproperty
    def alpha_horizon(self):
        pass
