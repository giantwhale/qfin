from . import SingleExchStrategy 


class SimpleMomentum(SingleExchStrategy):

    def __init__(self, num_bars=30, bar_dur=5):
        self.num_bars = int(num_bars)
        assert self.num_bars > 0
        self.bar_dur  = int(bar_dur) 
