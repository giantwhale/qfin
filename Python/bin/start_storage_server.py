import click

import os
from os.path import join as path_join
import sys
from time import time
from io import StringIO
import traceback

import logging
from datetime import datetime
utcnow = datetime.utcnow

#import fcntl

from qfin import settings
from qfin.CryptoCrncy.exchanges import GDAX
from qfin.CryptoCrncy.storage_engine import StorageEngine
from qfin.utils import sendmail
from qfin import ConfigLoader


def nowstamp():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def setup_logger(log_level, log_dir=None):
    logger = logging.getLogger('qfin')
    level  = eval('logging.%s' % log_level.upper())
    logger.setLevel(level)

    sch       = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    sch.setFormatter(formatter)
    sch.setLevel(level)
    logger.addHandler(sch)

    if log_dir is not None and os.path.exists(log_dir):
        log_file  = path_join(log_dir, 'storage_engine_{:s}.log'.format(
                        datetime.now().strftime('%Y%m%d') # use local time
                    )) 
        fch       = logging.FileHandler(log_file, mode='w', encoding=None, delay=False)
        formatter = logging.Formatter('%(asctime)s - %(name)s -\t %(levelname)s: %(message)s')
        fch.setFormatter(formatter)
        fch.setLevel(level)
        logger.addHandler(fch)
        print("log messages are saved at %s" % log_file)
    else:
        print("log_dir is missing or doesn't exists, no log file will be generated.")

    return logger


def run():
    gdax = GDAX()
    
    engine = StorageEngine()
    engine.add_server('54.91.32.6')  # Primary server
    # engine.add_server('34.230.65.167') # Backup server

    engine.add_data_item('rawdata.tradeticks')
    engine.add_data_item('rawdata.quoteticks')
    engine.add_data_item('account.account_hist')
    
    engine.run(asof=utcnow(), lookback=5)


@click.command()
@click.option('--config',   type=click.Path(exists=True), help='path to config file')
@click.option('--run_type', default=None, type=str, help='SIM/PROD, override the config file.')
@click.option('--log_level', default='info', type=str, help='debug/prod, if prod, email notification will be sent')
def safe_run(config, run_type, log_level):
    
    conf = ConfigLoader(config)
    settings.load_config(conf)
    if run_type is not None:
        run_type = run_type.upper()
        assert run_type in {'PROD', 'SIM'}
        settings.run_type = run_type

    logger = setup_logger(log_level, path_join(settings.workspace_cryptoccy, 'logs'))

    buff       = StringIO()
    stimestamp = nowstamp()
    stime      = time()
    
    try:
        run()
    except:
        runtime = time() - stime
        traceback.print_exc(file=buff)
        
        subject = '[Qfin Failed Proc] %s' %  __file__
        text    = \
            "Proc Started at {StartTime} UTC\n" \
            "  Terminated at {TermTime} UTC\n" \
            "Total run time: {RunMin:d} min {RunSec:.3f} seconds\n\n" \
            "Command: {Cmd}\n\n" \
            "Traceback message: \n\n {Traceback}\n".format(
                    StartTime = stimestamp,
                    TermTime  = nowstamp(),
                    RunMin    = int(runtime // 60),
                    RunSec    = runtime % 60,
                    Cmd       = ' '.join(sys.argv),
                    Traceback = buff.getvalue(),
                )
        print(subject + "\n")
        print(text)

    else:
        runtime = time() - stime
        print('[Qfin Proc Successfully Completed] %s' %  __file__)
        print(
            "Proc Started at {StartTime} UTC\n" \
            "  Terminated at {TermTime} UTC\n" \
            "Total run time: {RunMin:d} min {RunSec:.3f} seconds\n\n" \
            "Command: {Cmd}\n" .format(
                    StartTime = stimestamp,
                    TermTime  = nowstamp(),
                    RunMin    = int(runtime // 60),
                    RunSec    = runtime % 60,
                    Cmd       = ' '.join(sys.argv),
                )
            )

if __name__ == '__main__':
    print("Start the archive engine at %s" % utcnow())
    safe_run()

