try:
    from io import BytesIO  # Python 3.x
except ImportError:
    from cStringIO import StringIO as BytesIO # Python 2.7

import zmq

import msgpack
import numpy
from pandas import DataFrame, Series, read_pickle


# native python objects
def send_native(socket, data, copy=True, track=False):
    buf = msgpack.packb(data)
    return socket.send(buf, copy=copy, track=track)


def recv_native(socket, copy=True, track=False):
    buf  = socket.recv(copy=copy, track=track)
    data = msgpack.unpackb(buf)
    return data


# Numpy Array
def send_ndarray(socket, data, copy=True, track=False):
    md = dict(
        dtype = str(data.dtype),
        shape = data.shape,
    )
    socket.send_json(md, zmq.SNDMORE)
    return socket.send(data, copy=copy, track=track)


def recv_ndarray(socket, copy=True, track=False):
    md   = socket.recv_json()
    buf  = socket.recv(copy=copy, track=track)
    data = numpy.frombuffer(buf, dtype=md['dtype'])
    return data.reshape(md['shape'])


# Pandas DataFrame and Series
def send_pandas(socket, data, copy=True, track=False): # what're copy & track?
    bio = BytesIO()
    data.to_pickle(bio)
    return socket.send(bio.getvalue(), copy=copy, track=track)


def recv_pandas(socket, copy=True, track=False):
    msg = socket.recv(copy=copy, track=track)
    x   = read_pickle(BytesIO(msg))
    return x
