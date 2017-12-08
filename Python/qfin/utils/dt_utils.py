import datetime as dt
from datetime import datetime, timedelta


def round_dt(asof, seconds=5, forward=False):
    if seconds < 0:
        raise ValueError('seconds must be non-negative')
    if seconds == 0:
        return asof
    hh  = asof.hour
    mm  = asof.minute
    ss  = asof.second

    total_sec = hh * 3600 + mm * 60 + ss
    total_sec = total_sec // seconds * seconds
    if forward:
        total_sec += seconds
    asof = asof.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=total_sec)

    return asof

def floor_dt(asof, seconds=0, minutes=0):
    return round_dt(asof, seconds=minutes * 60 + seconds, forward=False)

def ceil_dt(asof, seconds=0, minutes=0):
    return round_dt(asof, seconds=minutes * 60 + seconds, forward=True)

