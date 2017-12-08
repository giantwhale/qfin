# numpy utilities
from numpy import isfinite

def non_na(x, y):
    x        = x.copy()
    good     = isfinite(x)
    x[~good] = y[~good]
    return x
