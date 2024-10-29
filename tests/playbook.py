import zarr

from zfdb import *
from zfdb.FDBMapping import FDBStore


def test_initialization():
    store = FDBMapping()
    root = zarr.group(store=store, chunk_store=None)

    # print("--- PRINTING TREE ROOT ----")
    # print(root.tree())
    # print("---------------------------")

    # cube = root.group["class=od,domain='g'"]
    # cube = cube.group["type=fc"]
    # tmp = cube[date=["20240601", "20240602"],number=["1" ,... , "50"]]

    # ranges are tricky to implement. TODO: HOW TO MATCH KEYS?
    # tmp = root[ '{"class": "ai", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1000", "param": "129", "step": "0"}' ]
    ai_group = root[{"class": "ai"}]
    ai_date_group = ai_group[{"date": "20240601"}]

    # tmp = ai_date_group[ '{"domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1000", "param": "129", "step": "0"}' ]
    # tmp = ai_date_group[ '{"domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "1000", "param": "129", "step": "0"}' ]
    ai_date_param_group = ai_date_group[{"param": "129"}]
    subselection_group = ai_date_param_group[{"time": "0000", "levelist": "1000"}]
    tmp = subselection_group[
        {
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "levtype": "pl",
            "type": "fc",
            "step": "0",
        }
    ]

    print("------ General accessability test ---------------")
    # print(ai_group.shape)
    # print(ai_group)
    # print(ai_group.info)
    # print(subselection_group.shape)
    # print(subselection_group)
    # print(subselection_group.info)
    print(ai_date_param_group.shape)
    print(ai_date_param_group)
    print(ai_date_param_group.info)
    # print(tmp.shape)
    # print(tmp)
    # print(tmp.info)
    # print(subselection_group.tree())
    # print(subselection_group.info)
    # print(ai_date_param_group)
    # print(ai_date_param_group.tree())
    # print(ai_date_param_group.info)
    # print("\n")
    # print("------ Retrieving a field ---------------")
    # print(tmp[0, 0, 0, :])
    #
    # print("-----------------Levellist: 1000-----------------")
    # print("**************** Subsection of 20240601/129/1000 - indices 10 to 20 **********")
    # print(tmp[0, 0, 0, 10:20].shape)
    # print(tmp[0, 0, 0, 10:20])
    # print("\n")
    #
    # print("------------Levellist: 850-----------------")
    # print("**************** Subsection of 20240601/129/850 - indices 10 to 20 **********")
    # tmp_850 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp_850[0, 0, 0, 10:20].shape)
    # print(tmp_850[0, 0, 0, 10:20])
    # print("\n")
    #
    # print("-----------------Timeslice: -----------------")
    # print("**************** Subsection of 20240601/20240602/20240603/129/850 - indices 10 to 20 **********")
    # tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print("Shape of whole request:", tmp_time_slice[0:3, 0, 0, :].shape)
    # print(tmp_time_slice[0:3, 0, 0, 10:20])
    #
    # print("-----------------Subsection Individual: -----------------")
    # tmp_850_2 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # tmp_850_3 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    #
    # print("**************** Subsection of 20240601/129/850 - indices 10 to 20 **********")
    # print(tmp_850[0, 0, 0, 10:20])
    # print("**************** Subsection of 20240602/129/850 - indices 10 to 20 **********")
    # print(tmp_850_2[0, 0, 0, 10:20])
    # print("**************** Subsection of 20240603/129/850 - indices 10 to 20 **********")
    # print(tmp_850_3[0, 0, 0, 10:20])
    # print("\n")
    #
    # print("-----------------Slicing Subgrid: -----------------")
    # print("**************** Subsection of 20240601/20240602/20240603/129/850 **********")
    # tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
    # print(tmp_time_slice[0:3, 0, 0, 10:20].shape)
    # print(tmp_time_slice[0:3, 0, 0, 10:20])
    # print("\n")
    #
    # print("**************** Subsection of 20240601/20240602/20240603/130/850 **********")
    # tmp_time_slice_130 = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "130", "step": "0"}' ]
    # print(tmp_time_slice_130[0:3, 0, 0, 10:20].shape)
    # print(tmp_time_slice_130[0:3, 0, 0, 10:20])
    # print("-----------------Slicing Subgrid 2D: -----------------")
    #
    # time_param_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129/130", "step": "0"}' ]
    # print(time_param_slice[0:3, 0:2, 0, 10:20].shape)
    # print(time_param_slice[0:3, 0:2, 0, 10:20])
    # print("---------------------------------------------------")


if __name__ == "__main__":
    test_initialization()
