from io import BytesIO
import pickle

def serialize(x):
    buffer = BytesIO()
    pickle.dump(x, buffer)
    return buffer.getvalue()


def deserialize(x):
    return pickle.loads(x)


