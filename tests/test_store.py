import zarr

from zfdb import *
from zfdb.FDBStore import FDBStore

def test_initialization():
    store = FDBStore()
    root = zarr.group(store=store, chunk_store=None)

    # print("--- PRINTING TREE ROOT ----")
    # print(root.tree())
    # print("---------------------------")

    # ranges are tricky to implement. TODO: HOW TO MATCH KEYS?
    tmp = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1000", "param": "129", "step": "0"}' ]

    print("------")
    print(tmp.shape)
    print(tmp)
    print(tmp[0, 0, 0, :])
    print("------")
    print("\n")
    print("\n")
    print("-----------------Levellist: 1000-----------------")
    print(tmp[0, 0, 0, 10:100].shape)
    print(tmp[0, 0, 0, 10:100])
    print("-------")
    print("\n")
    print("\n")
    print("------------Levellist: 850-----------------")
    tmp_850 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    print(tmp_850[0, 0, 0, 10:20].shape)
    print(tmp_850[0, 0, 0, 10:20])
    print("-------")
    print("\n")
    print("\n")
    # print("-----------------Timeslice: -----------------")
    # tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp[0:1, 0, 0, :].shape)
    # print("-------")
    # print("\n")
    # print("\n")

if __name__ == "__main__":
    test_initialization()
