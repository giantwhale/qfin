import click

import os
from os.path import join as path_join
import sys
from time import time
from io import StringIO
import traceback

import logging
from datetime import datetime, timedelta
utcnow = datetime.utcnow

import fcntl

from qfin.CryptoCrncy.exchanges import GDAX
from qfin.CryptoCrncy.bar_data_generator import BarDataGenerator
from qfin.utils import sendmail, load_module
from qfin import ConfigLoader
from qfin import settings


def nowstamp():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def setup_logger(log_level, log_dir=None):
    logger = logging.getLogger('qfin')
    level = eval('logging.%s' % log_level.upper())
    logger.setLevel(level)

    sch       = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    sch.setFormatter(formatter)
    sch.setLevel(level)
    logger.addHandler(sch)

    if log_dir is not None and os.path.exists(log_dir):
        log_file  = path_join(log_dir, 'gen_bars_{:s}.log'.format(
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


def safe_run(startdate, enddate, log_level, email):
    setup_logger(log_level, log_dir=path_join(settings.workspace_cryptoccy, 'logs'))

    gen = BarDataGenerator()
    if settings.run_type == 'SIM':
        gen.add_frequency(minutes=5)
        gen.add_frequency(minutes=30)
        receipients = settings.debug_email_receipients
    else: # prod mode
        enddate = datetime(2099, 12, 31)
        receipients = settings.prod_email_receipients


    buff       = StringIO()
    stimestamp = nowstamp()
    stime      = time()

    try:
        gen.run(startdate, enddate)
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


@click.command()
@click.option('--config',    type=click.Path(exists=True), help='path to config file')
@click.option('--startdate', default=None,   type=str, help='start date, yyyymmdd, default T-5')
@click.option('--enddate',   default=None,   type=str, help='end date, yyyymmdd, default T-1')
@click.option('--run_type',  default=None,   type=str, help='SIM or PROD, override the config file')
@click.option('--log_level', default='info', type=str, help='log level, debug, info, etc.')
@click.option('--email/--no-email', default=False)
def run(config, startdate, enddate, run_type, log_level, email):

    conf = ConfigLoader(config)
    settings.load_config(conf)
    if run_type is not None:
        run_type = run_type.upper()
        assert run_type in {'PROD', 'SIM'}
        settings.run_type = run_type

    os.makedirs(path_join(settings.workspace_cryptoccy, 'locks'), exist_ok=True)
    os.makedirs(path_join(settings.workspace_cryptoccy, 'logs'), exist_ok=True)

    startdate = datetime.strptime(startdate, '%Y%m%d') if startdate else (datetime.utcnow() - timedelta(days=3))
    enddate   = datetime.strptime(enddate,   '%Y%m%d') if enddate   else (datetime.utcnow() - timedelta(days=1))

    lockfile = path_join(settings.workspace_cryptoccy, 'locks', 'lock_gen_bar_data')

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

        safe_run(startdate, enddate, log_level, email)


if __name__ == '__main__':
    run()
