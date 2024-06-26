import zarr

from zfdb import *
from zfdb.FDBStore import FDBStore

def test_initialization():
    store = FDBStore()
    root = zarr.group(store=store, chunk_store=None)

    root.tree()
    tmp = root['{"class": "od", "date": "20240202", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1", "param": "130", "step": "0"}']

    print(tmp)
    print(tmp[:])


if __name__ == "__main__":
    test_initialization()
