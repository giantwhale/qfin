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

import fcntl

from qfin.CryptoCrncy import DataEngine
from qfin.CryptoCrncy.exchanges import GDAX
from qfin.utils import sendmail
from qfin import ConfigLoader
from qfin import settings


def nowstamp():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def setup_logger(log_level='INFO', log_dir=None):
    logger = logging.getLogger('qfin')
    level  = eval('logging.%s' % log_level.upper())
    logger.setLevel(level)

    sch       = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    sch.setFormatter(formatter)
    sch.setLevel(level)
    logger.addHandler(sch)

    if log_dir is not None and os.path.exists(log_dir):
        log_file  = path_join(log_dir, 'data_server_{:s}.log'.format(
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

    gdax   = GDAX()
    gdax.add_product('BTC', 'USD')
    gdax.add_product('ETH', 'USD')
    gdax.add_product('LTC', 'USD')
    gdax.add_product('ETH', 'BTC')
    gdax.add_product('LTC', 'BTC')

    engine = DataEngine(gdax)
    engine.start_server()


def safe_run(email, log_level):

    logger = setup_logger(log_level, log_dir=path_join(settings.workspace_cryptoccy, 'logs'))

    if settings.run_type == 'PROD':
        receipients = settings.prod_email_receipients
    else:
        receipients = settings.debug_email_receipients

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
        if email:
            sendmail(
                  receipient = receipients
                , subject    = subject
                , text       = text
                )
        else:
            print(subject + "\n")
            print(text)

    else:
        runtime = time() - stime
        subject = '[Qfin Proc Successfully Completed] %s' %  __file__        
        text    = \
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
        if email:
            sendmail(
                  receipient = receipients
                , subject    = subject
                , text       = text
                )
        else:
            print(subject + "\n")
            print(text)

@click.command()
@click.option('--config',   type=click.Path(exists=True), help='path to config file')
@click.option('--run_type', default=None, type=str, help='SIM/PROD, override the config file.')
@click.option('--email/--no-email', default=False)
@click.option('--log_level', default='info', type=str, help='debug/prod, if prod, email notification will be sent')
def main(config, run_type, email, log_level):
    """main function to start data engine"""
    conf = ConfigLoader(config)
    settings.load_config(conf)
    if run_type is not None:
        run_type = run_type.upper()
        assert run_type in {'PROD', 'SIM'}
        settings.run_type = run_type

    os.makedirs(path_join(settings.workspace_cryptoccy, 'locks'), exist_ok=True)
    os.makedirs(path_join(settings.workspace_cryptoccy, 'logs'), exist_ok=True)
    
    lockfile = path_join(settings.workspace_cryptoccy, 'locks', 'lock_data_engine')

    with open(lockfile, 'a') as f:
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print("File locked. Exiting...")
            os._exit(0)

        f.write('Runtime: {} UTC, {} US-EAST-1, pid: {}\n'.format(
                utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                os.getpid(),
            ))
        f.flush()

        safe_run(email, log_level)

if __name__ == '__main__':

    main()
