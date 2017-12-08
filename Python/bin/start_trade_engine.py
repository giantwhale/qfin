from time import sleep

import click

import os
from os.path import join as path_join

import logging
from datetime import datetime
utcnow = datetime.utcnow

import fcntl

from qfin.CryptoCrncy import TradeEngine
from qfin.CryptoCrncy.exchanges import GDAX
from qfin import ConfigLoader
from qfin import settings


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
        log_file  = path_join(log_dir, 'trade_engine_{:s}.log'.format(
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
    engine = TradeEngine()

    gdax   = GDAX()
    gdax.add_product('BTC', 'USD')
    gdax.add_product('ETH', 'USD')
    gdax.add_product('LTC', 'USD')
    gdax.add_product('ETH', 'BTC')
    gdax.add_product('LTC', 'BTC')
    engine.add_exch(gdax)
    
    engine.start_server()

@click.command()
@click.option('--config',   type=click.Path(exists=True), help='path to config file')
@click.option('--run_type', default=None, type=str, help='SIM/PROD, override the config file.')
@click.option('--email/--no-email', default=False)
@click.option('--log_level', default='info', type=str, help='debug/prod, if prod, email notification will be sent')
def main(config, run_type, email, log_level):
    """main function to start trade engine"""
    conf = ConfigLoader(config)
    settings.load_config(conf)
    if run_type is not None:
        run_type = run_type.upper()
        assert run_type in {'PROD', 'SIM'}
        settings.run_type = run_type

    root = settings.workspace_cryptoccy

    lockfile = path_join(root, 'locks', 'lock_trade_engine')

    with open(lockfile, 'a') as f:
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print("File locked. Exiting...")
            os._exit(0)

        f.write('Runtime: {}, pid: {}\n'.format(
                utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                os.getpid(),
            ))
        f.flush()

        logger = setup_logger(log_level, log_dir=path_join(root, 'logs'))
        run()

if __name__ == '__main__':

    main()