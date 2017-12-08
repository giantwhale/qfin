import bottle
from bottle import route, response

from os.path import isfile
from datetime import datetime, timedelta
utcnow = datetime.utcnow
from collections import deque
from qfin.utils.dt_utils import floor_dt
import pymongo
from pymongo import MongoClient

import numpy as np
import pandas as pd

import traceback

global accounts_queues
accounts_queues = {}
accounts_queues['5']   = deque(maxlen=96) # newest first 
accounts_queues['30']  = deque(maxlen=96) # newest first 
accounts_queues['120'] = deque(maxlen=96) # newest first 


global rebals_queues
rebals_queues = {}
rebals_queues['5']   = deque(maxlen=96) # newest first 
rebals_queues['30']  = deque(maxlen=96) # newest first 
rebals_queues['120'] = deque(maxlen=96) # newest first 


def qfin_load_bars():

    res = {}

    # server_addr = '54.91.32.6'
    server_addr = 'localhost'

    client = MongoClient(server_addr, 27017)  # 27017 is the default port
    db     = client.crypto_ccy

    now = datetime.utcnow()
    periods = [
        (n, now - timedelta(hours=h)) 
            for n, h in [
                  ('past15m', 0.25)
                , ('past1h' , 1   )
                , ('past6h' , 6   )
                , ('past12h', 12  )
                , ('past1d' , 24  )
            ]]

    # Bar Data 
    for barname in ['bar1m']:
        colnames = []
        colvalues = []
        
        collection = db[barname]
        for k, v in periods:
            colnames.append(k)
            colvalues.append(collection.find({"time": {"$gte": v} }).count())
        res[barname] = {
                'colnames': ['Server', 'Last Updated'] + colnames,
                'rows': [['Primary', utcnow().strftime('%Y-%m-%d %H:%M:%S')] + colvalues],
            }


    # Snap Data
    current_account = []
    cursor   = db.current_account.find({}, { 'Exchange': 1, 'Time': 1 })
    nowstamp = utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for doc in cursor:
        current_account.append([
            doc.get('Exchange', '_NA_')
          , doc.get('Time').strftime('%Y-%m-%d %H:%M:%S') if 'Time' in doc else '_NA_'
          , nowstamp
        ])

    res['curracct'] = {
            'colnames': ['Exchange','Last Updated','Current Time'],
            'rows': current_account
        }

    client.close()

    return res


# Account Related 
# ---------------------------------------------------------------------

def load_position_file(t):
    one_min = timedelta(minutes=1)
    datadir = '/data/crypto_ccy/account/positions/'
    for i in range(5):
        t += one_min
        fname = datadir + t.strftime('%Y%m%d/%Y%m%d_%H%M%S.csv')
        if isfile(fname):
            try:
                acct = pd.read_csv(fname)
                return t, acct
            except:
                break
    return t, None


def qfin_load_accounts(rebal_intv=5):
    global accounts_queues
    accounts_queue = accounts_queues.get('%d' % rebal_intv)
    if accounts_queue is None:
        return None

    intv = timedelta(minutes=rebal_intv)
    now  = utcnow()

    t   = floor_dt(now, minutes=rebal_intv) - timedelta(hours=rebal_intv * 96)
    if len(accounts_queue) > 0:
        t = max(t, accounts_queue[0][0] + intv)

    tend = now - timedelta(minutes=3)
    while t < tend:
        t1, acct = load_position_file(t)
        if acct is None:
            item = {
                'HHMM'     : t.strftime('%d %b %H:%M'),
                'USDValTot': None, 
                'USDValCcy': None,
                'Prices': [None, None, None],
            }
        else:
            px  = 0.5 * (acct['Bid'].values + acct['Ask'].values)
            pos = acct['PosCurr'].fillna(0.0).values
            usd = np.float(acct['PosCurr'][acct['BaseCcy'] == 'USD'])
            totval = np.sum(px * pos)
            ss = pd.Series(px, index=acct['BaseCcy'])
            px = ss.loc[['BTC','ETH','LTC']].tolist()
            item = {
                'HHMM': t.strftime('%d %b %H:%M'),
                'USDValTot': np.round(totval, 2),
                'USDValCcy': np.round(totval - usd, 2),
                'Prices': px,
            }
            
        accounts_queue.appendleft((t, item))
        t += intv

    hhmm = []
    usd_val_tot = []
    usd_val_ccy = []
    prices = []
    for _, x in reversed(accounts_queue):
        hhmm.append(x['HHMM'])
        usd_val_tot.append(x['USDValTot'])
        usd_val_ccy.append(x['USDValCcy'])
        prices.append(x['Prices'])

    prices = np.array(prices).astype('float')
    good = np.sum(np.isfinite(prices), axis=1)
    if np.sum(good) > 0:
        idx = np.argwhere(good).flatten()[0]
    else:
        idx = 0
    rets = prices / prices[[idx], :] - 1
    rets = np.where(np.isfinite(rets), rets, None)

    res = [
            {
                'caption': 'Past 1Day Return (%d min bars)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'BTC $ %.2f' % (x['Prices'][0] or -1),
                        'borderColor': '#FC2525',
                        'fill': False,                
                        'data': rets[:,0].tolist(),
                    },
                    {
                        'label': 'ETH $ %.2f' % (x['Prices'][1] or -1),
                        'borderColor': '#33ACFF',
                        'fill': False,                
                        'data': rets[:,1].tolist(),
                    },
                    {
                        'label': 'LTC $ %.2f' % (x['Prices'][2] or -1),
                        'borderColor': '#33FF86',
                        'fill': False,                
                        'data': rets[:,2].tolist(),
                    }
                ]
            },
            {
                'caption': 'Portfolio Value (%d min bars)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'USDValTot %.2f' % x['USDValTot'],
                        'borderColor': '#FC2525',
                        'fill': False,                
                        'data': usd_val_tot,
                    },
                    {
                        'label': 'USDValCcy %.2f' % x['USDValCcy'],
                        'borderColor': '#33ACFF',
                        'fill': False,                
                        'data': usd_val_ccy,                    
                    }
                ]
            },
        ]

    return res


# Account Rebalance
# ---------------------------------------------------------------------

def load_rebal_file(t):
    one_min = timedelta(minutes=1)
    datadir = '/data/crypto_ccy/account/rebals/active_rebal/'
    for i in range(5):
        t += one_min
        fname = datadir + t.strftime('%Y%m%d/%Y%m%d_%H%M00.csv')
        if isfile(fname):
            try:
                acct = pd.read_csv(fname)
                return t, acct
            except:
                break
    return t, None


def qfin_load_rebals(rebal_intv=5):
    global rebals_queues
    rebals_queue = rebals_queues.get('%d' % rebal_intv)
    if rebals_queues is None:
        return None

    intv = timedelta(minutes=rebal_intv)
    now  = utcnow()

    t   = floor_dt(now, minutes=rebal_intv) - timedelta(minutes=rebal_intv * 96)
    if len(rebals_queue) > 0:
        t = max(t, rebals_queue[0][0] + intv)

    hhmm = []

    BTCPx, BTC0, BTC1, BTC_Alpha = [], [], [], []
    ETHPx, ETH0, ETH1, ETH_Alpha = [], [], [], []
    LTCPx, LTC0, LTC1, LTC_Alpha = [], [], [], []

    tend = now - timedelta(minutes=3)
    while t < tend:
        t1, rebal = load_rebal_file(t)
        hhmm.append(t.strftime('%d %b %H:%M'))
        if rebal is None:
            # mv.append(None)
            BTCPx.append(None)
            BTC0.append(None)
            BTC1.append(None)
            BTC_Alpha.append(None)
            ETHPx.append(None)
            ETH0.append(None)
            ETH1.append(None)
            ETH_Alpha.append(None)
            LTCPx.append(None)
            LTC0.append(None)
            LTC1.append(None)
            LTC_Alpha.append(None)
        else:
            rebal = rebal.loc[:, ['BaseCcy', 'Bid', 'Ask', 'Alpha', 'MvOptm', 'MvCurr']]
            rebal['Mid'] = 0.5 * (rebal['Bid'] + rebal['Ask'])

            # mv.append(rebal['MvCurr'].sum())

            rebal = rebal.set_index('BaseCcy').astype('float')
            rebal = rebal.where(pd.notnull(rebal), None)
            
            z = rebal.loc['BTC']
            BTCPx.append(z['Mid'])
            BTC0.append(z['MvCurr'])
            BTC1.append(z['MvOptm'])
            BTC_Alpha.append(z['Alpha'])
            
            z = rebal.loc['ETH']
            ETHPx.append(z['Mid'])
            ETH0.append(z['MvCurr'])
            ETH1.append(z['MvOptm'])
            ETH_Alpha.append(z['Alpha'])

            z = rebal.loc['LTC']
            LTCPx.append(z['Mid'])
            LTC0.append(z['MvCurr'])
            LTC1.append(z['MvOptm'])
            LTC_Alpha.append(z['Alpha'])
        t += intv
    
    res = [{
                'caption': 'Alpha (Freq = %d min)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'BTC',
                        'borderColor': '#FC2525',
                        'fill': False,
                        'borderWidth': 1,
                        'pointStyle': 'circle',
                        'data': BTC_Alpha,
                    },
                    {
                        'label': 'ETH',
                        'borderColor': '#33ACFF',
                        'fill': False,
                        'borderWidth': 1,
                        'data': ETH_Alpha,
                    },
                    {
                        'label': 'LTC',
                        'borderColor': '#33FF86',
                        'fill': False,
                        'borderWidth': 1,
                        'data': LTC_Alpha,
                    }
                ]
            },
            {
                'caption': 'BTC Trades (Freq = %d min)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'BTC Price (Right)',
                        'borderColor': '#999999',
                        'fillColor': 'rgba(220,220,220,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-1',
                        'data': BTCPx,
                    },
                    {
                        'label': 'BTC Curr MV',
                        'borderColor': '#FC2525',
                        'fill': False,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-0',
                        'data': BTC0,
                    },
                    {
                        'label': 'BTC Optm MV',
                        'borderColor': '#33ACFF',
                        'fillColor': 'rgba(51,172,255,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'yAxisID': 'y-axis-0',
                        'data': BTC1,
                    }
                ]
            },
            {
                'caption': 'ETH Trades (Freq = %d min)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'ETH Price (Right)',
                        'borderColor': '#999999',
                        'fillColor': 'rgba(220,220,220,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-1',
                        'data': ETHPx,
                    },
                    {
                        'label': 'ETH Curr MV',
                        'borderColor': '#FC2525',
                        'fill': False,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-0',
                        'data': ETH0,
                    },
                    {
                        'label': 'ETH Optm MV',
                        'borderColor': '#33ACFF',
                        'fillColor': 'rgba(51,172,255,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'yAxisID': 'y-axis-0',
                        'data': ETH1,
                    }
                ]
            },
            {
                'caption': 'LTC Trades (Freq = %d min)' % rebal_intv,
                'labels': hhmm,
                'datasets': [
                    {
                        'label': 'LTC Price (Right)',
                        'borderColor': '#999999',
                        'fillColor': 'rgba(220,220,220,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-1',
                        'data': LTCPx,
                    },
                    {
                        'label': 'LTC Curr MV',
                        'borderColor': '#FC2525',
                        'fill': False,
                        'pointStyle': 'circle',
                        'yAxisID': 'y-axis-0',
                        'data': LTC0,
                    },
                    {
                        'label': 'LTC Optm MV',
                        'borderColor': '#33ACFF',
                        'fillColor': 'rgba(51,172,255,0.75)',
                        'fill': True,
                        'borderWidth': 1,
                        'yAxisID': 'y-axis-0',
                        'data': LTC1,
                    }
                ]
            },
        ]

    return res


def enable_cors(fn):
    def _enable_cors(*args, **kwargs):
        # set CORS headers
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

        if bottle.request.method != 'OPTIONS':
            # actual request; reply with the actual response
            return fn(*args, **kwargs)

    return _enable_cors



app = bottle.app()

@app.route('/data/<intv>', method='GET')
@enable_cors
def alldata():
    response.headers['Content-type'] = 'application/json'
    dct = {}
    intv = int(intv)
    dct['Accounts'] = qfin_load_accounts(intv)
    dct['Bars'] = qfin_load_bars()
    return dct


@app.route('/bars', method='GET')
@enable_cors
def bars():
    response.headers['Content-type'] = 'application/json'
    dct = {}
    dct['Bars'] = qfin_load_bars()
    return dct


@app.route('/accounts/<intv>', method='GET')
@enable_cors
def accounts(intv):
    response.headers['Content-type'] = 'application/json'
    dct = {}

    try:
        intv = int(intv)
        dct['Accounts'] = qfin_load_accounts(intv)
    except:
        print("Failed to load accounts.")
        traceback.print_exc()

    return dct


@app.route('/rebals/<intv>', method='GET')
@enable_cors
def rebals(intv):
    response.headers['Content-type'] = 'application/json'
    dct = {}

    try:
        intv = int(intv)
        dct['Data'] = qfin_load_rebals(intv)
    except:
        print("Failed to load rebals.")
        traceback.print_exc()

    return dct


app.run(host='0.0.0.0', port=8001, debug=True)

