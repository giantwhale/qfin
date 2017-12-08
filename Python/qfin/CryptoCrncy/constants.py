# @DeleteMe:  these variables are not really needed
# when we deprecat CryptoCrncy.proc.* we should be able to safely delete
# this file.

products = ['BTC', 'ETH', 'LTC']

pairs = [
    ('BTC', 'USD'),
    ('ETH', 'USD'), ('ETH', 'BTC'), 
    ('LTC', 'USD'), ('LTC', 'BTC'),	
]

zmq_server_addr = 'tcp://*:5555'
zmq_client_addr = 'tcp://localhost:5555'
