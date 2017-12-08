
class Bar(object):
    def __init__(self, bartime, open, close, high, low, buyvol, sellvol, avgsprd, dur):
        self.bartime  = bartime
        self.duration = int(dur)
        self.open     = float(open)
        self.close    = float(close)
        self.high     = float(high)
        self.low      = float(low)
        self.buyvol   = float(buyvol)
        self.sellvol  = float(sellvol)
        self.avgsprd  = float(avgsprd)


class Bar5m(bar):
    def __init__(self, open, close, high, low, buyvol, sellvol, avgsprd):
        super(Bar5m, self).__init__(open, close, high, low, buyvol, sellvol, avgsprd, 5)

