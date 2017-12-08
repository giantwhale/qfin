import os
from os.path import join as path_join

import pandas as pd
from datetime import datetime

from .. import settings


def save_signal(df, ymd_hms, name, datatype='.'):
    """
    :param datatype: rawdata, 
    """
    name = name.replace('.', '/')
    ymd  = ymd_hms[:8]
    root = settings.workspace_cryptoccy

    dest_dir = path_join(root, datatype, name, ymd)
    os.makedirs(dest_dir, exist_ok=True)

    fname = ymd_hms + '.csv'
    df.to_csv(path_join(dest_dir, fname), index=False, float_format='%g')


def load_signal(ymd_hms, name, datatype):
    name = name.replace('.', '/')
    root = settings.workspace_cryptoccy
    
    if len(ymd_hms) > 8:
        ymd = ymd_hms[:8]
        fullname = path_join(root, datatype, name, ymd, ymd_hms + '.csv')
    else:
        fullname = path_join(root, datatype, name, ymd_hms + '.csv')  # ymd only

    if not os.path.exists(fullname):
        return None
    df = pd.read_csv(fullname)
    return df


def data_dir_path(asof, name, datatype, exch=None):
    root = settings.workspace_cryptoccy
    if isinstance(asof, datetime):
        ymd = asof.strftime('%Y%m%d')
    else:
        ymd = ymd[:8]

    if exch is None:
        x = path_join(root, datatype, name, ymd)
    else:
        x = path_join(root, datatype, exch.name, name, ymd)
    return x


def archive_dir_path(name, datatype, exch=None):
    root = settings.workspace_cryptoccy
    if exch is None:
        x = path_join(root, 'archive', datatype, name)
    else:
        x = path_join(root, 'archive', datatype, exch.name, name)
    return x

