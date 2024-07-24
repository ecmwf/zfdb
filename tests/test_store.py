import zarr

from zfdb import *
from zfdb.business.FDBStore import FDBStore

from numpy.testing import assert_array_equal
 
def test_retrieving_a_field():
    store = FDBStore()
    root = zarr.group(store=store, chunk_store=None)

    # print("--- PRINTING TREE ROOT ----")
    # print(root.tree())
    # print("---------------------------")

    # ranges are tricky to implement. TODO: HOW TO MATCH KEYS?
    tmp = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1000", "param": "129", "step": "0"}' ]

    print("------ General accessability test ---------------")

    print("------ Retrieving a field ---------------")
    print(tmp.shape)
    print(tmp)
    # print(tmp[0, 0, 0, :])

    # print("-----------------Levellist: 1000-----------------")
    # print(tmp[0, 0, 0, 10:100].shape)
    # print(tmp[0, 0, 0, 10:100])
    # 
    # print("------------Levellist: 850-----------------")
    # tmp_850 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp_850[0, 0, 0, 10:20].shape)
    # print(tmp_850[0, 0, 0, 10:20])

    # print("-----------------Timeslice: -----------------")
    # tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp_time_slice[0:2, 0, 0, :].shape)
    #
    # print("-----------------Subsection Individual: -----------------")
    # tmp_850_2 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # tmp_850_3 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    #
    # print("-----------------20240601: -----------------")
    # print(tmp_850[0, 0, 0, 10:20])
    # print("-----------------20240602: -----------------")
    # print(tmp_850_2[0, 0, 0, 10:20])
    # print("-----------------20240603: -----------------")
    # print(tmp_850_3[0, 0, 0, 10:20])
    #
    # print("-----------------Slicing Subgrid: -----------------")
    # tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp_time_slice[0:3, 0, 0, 10:20].shape)
    # print(tmp_time_slice[0:3, 0, 0, 10:20])
    # print("---------------------------------------------------")
    #
    # print("-----------------Slicing Subgrid 2D: -----------------")
    #
    # time_param_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
    # print(time_param_slice[0:3, 0:2, 0, 10:20].shape)
    # print(time_param_slice[0:3, 0:2, 0, 10:20])
    #
    # time_param_slice_01 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
    # time_param_slice_02 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
    # time_param_slice_03 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
    #
    #
    # print(time_param_slice_01[0, 0:2, 0, 10:20])
    # print(time_param_slice_02[0, 0:2, 0, 10:20])
    # print(time_param_slice_03[0, 0:2, 0, 10:20])


# def test_slicing_subselection_vs_individual_retrieval():
#     store = FDBStore()
#     root = zarr.group(store=store, chunk_store=None)
#
#     time_param_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#     print(time_param_slice[0:3, 0:2, 0, 10:20].shape)
#     print(time_param_slice[0:3, 0:2, 0, 10:20])
#
#     time_param_slice_01 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#     time_param_slice_02 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#     time_param_slice_03 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#
#     assert_array_equal(time_param_slice[0:3, 0:2, 0, 10:20][0], time_param_slice_01[0, 0:2, 0, 10:20])
#     assert_array_equal(time_param_slice[0:3, 0:2, 0, 10:20][1], time_param_slice_02[0, 0:2, 0, 10:20])
#     assert_array_equal(time_param_slice[0:3, 0:2, 0, 10:20][2], time_param_slice_03[0, 0:2, 0, 10:20])
#
# def test_aggregating_time_param_values():
#     store = FDBStore()
#     root = zarr.group(store=store, chunk_store=None)
#
#     time_param_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#
#     time_param_slice_01 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#     time_param_slice_02 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#     time_param_slice_03 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
#
#     assert time_param_slice[0].shape == time_param_slice_01[0].shape
#     assert time_param_slice[1].shape == time_param_slice_02[0].shape
#     assert time_param_slice[2].shape == time_param_slice_03[0].shape
#
#     assert_array_equal(time_param_slice[0], time_param_slice_01[0])
#     assert_array_equal(time_param_slice[1], time_param_slice_02[0])
#     assert_array_equal(time_param_slice[2], time_param_slice_03[0])
