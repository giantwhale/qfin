import os
from os.path import join as path_join

from . import signal

from .data_engine import DataEngine
from .trade_engine   import TradeEngine
from .archive_engine import ArchiveEngine

# Load Base Strategies 
from .simple_strategy import SimpleStrategy

__ALL__ = ['signal', 'DataEngine', 'TradeEngineLive', 'TradeEngine','ArchiveEngine']



# Setup workspace

# if not os.path.exists(root):
#     raise RuntimeError("path %s does not exist" % root) 

# for subdir in ['logs', 'locks', 'rawdata', 'signals', 'archive', 'debug']:
#     directory = path_join(root, subdir)
#     os.makedirs(directory, exist_ok=True)

