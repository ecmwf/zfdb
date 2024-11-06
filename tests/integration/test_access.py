from zarr.hierarchy import Group

from zfdb.business.FDBStore import FDBMapping

import zarr
import numpy as np


class TestAccess:
    def test_create_root_group(self):
        store = FDBMapping()
        root = zarr.group(store=store, chunk_store=None)

        print(root.info)

    def test_simple_group_structure(self):
        store = FDBMapping()
        root = zarr.group(store=store, chunk_store=None)

        ai_date_param_group = root[{"class": "ai"}][{"date": "20240601"}][
            {"param": "129"}
        ]
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
        # tmp = subselection_group[ {"domain": "g", "expver": "0001", "stream": "oper", "levtype": "pl", "type": "fc", "step": "0"} ]
        # tmp = subselection_group[ {"domain": "g", "expver": "0001", "stream": "enfo", "levtype": "pl", "type": "pf", "number": list(range(1, 51)), "step": list(range(0, 20))} ]
        # shape = (20, 50, 54200)

        print(tmp.shape)

        print(tmp)
        print(subselection_group.tree())
        print(subselection_group.info)

        print(ai_date_param_group)
        print(ai_date_param_group.tree())
        print(ai_date_param_group.info)

        assert len(root) == 1584
        assert len(ai_date_param_group) == 66
        assert len(subselection_group) == 11
        assert len(tmp) == 1

    def test_retrieve_values_from_group_field(self):
        store = FDBMapping()
        root = zarr.group(store=store, chunk_store=None)

        ai_group = root[{"class": "ai"}]
        ai_date_group = ai_group[{"date": "20240601"}]

        ai_date_param_group = ai_date_group[{"param": "129"}]
        subselection_group = ai_date_param_group[{"time": "0000", "levelist": "1000"}]
        data = subselection_group[
            {
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "levtype": "pl",
                "type": "fc",
                "step": list(range(0, 24, 12)),
            }
        ]

        print(data[0, 0, 0, :])

    def test_time_slicing(self):
        store = FDBMapping()
        root = zarr.group(store=store, chunk_store=None)

        # tmp_time_slice is a group
        # TODO: How to handle the mapping of date, time and step to get a valid time.
        tmp_time_slice = root[
            '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}'
        ]
        tmp_850 = root[
            '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}'
        ]
        tmp_850_2 = root[
            '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}'
        ]
        tmp_850_3 = root[
            '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}'
        ]

        tmp_all_slice = tmp_time_slice[0:3, 0, 0, 10:20]

        np.testing.assert_array_equal(tmp_all_slice[0], tmp_850[0, 0, 0, 10:20])
        np.testing.assert_array_equal(tmp_all_slice[1], tmp_850_2[0, 0, 0, 10:20])
        np.testing.assert_array_equal(tmp_all_slice[2], tmp_850_3[0, 0, 0, 10:20])
