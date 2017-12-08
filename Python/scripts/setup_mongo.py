import pymongo
from pymongo import MongoClient


def setup_taq1m(db):
    db['taq1m'].drop()
    # db.create_collection('taq1m', capped=True, size=221184000, max=4320) # past 3 days
    db.create_collection('taq1m')
    db.taq1m.create_index([('BarTime', pymongo.ASCENDING)], unique=True)


def setup_current_quote(db):
    db['current_quote'].drop()
    # db.create_collection('current_quote', capped=True, size=10240, max=1)
    db.create_collection('current_quote', capped=True, size=10240, max=1)


def setup_bar5m(db):
    # In production we only allow 5min bars
    db['bar5m'].drop()
    # db.create_collection('bar5m', capped=True, size=32 * 1024 * 1024, max=2016) # past 7 days
    db.create_collection('bar5m')
    db.bar5m.create_index([('BarTime', pymongo.ASCENDING)], unique=True)


def run():
    client = MongoClient('localhost', 27017)
    db = client.crypto_ccy

    print("Setting up taq1m")
    setup_taq1m(db)

    print("Setting up current_quote")
    setup_current_quote(db)

    print("Setting up bar5m")
    setup_bar5m(db)


if __name__ == '__main__':
    run()
