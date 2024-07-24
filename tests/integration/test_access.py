from zarr.hierarchy import Group

from zfdb.business.FDBStore import FDBStore
from zfdb.business.Request import Request, RequestMapper

import zarr
import numpy as np

from zfdb.gribjump.request import GribJumpRequestMapper

class TestAccess:

    def test_create_root_group(self):
        store = FDBStore()
        root = zarr.group(store=store, chunk_store=None)

        print(root.info)

    def test_simple_group_structure(self):
        store = FDBStore()
        root = zarr.group(store=store, chunk_store=None)

        ai_group = root[GribJumpRequestMapper.map_from_dict({"class": "ai"})]
        ai_date_group = ai_group[GribJumpRequestMapper.map_from_dict({"date": "20240601"})]

        ai_date_param_group = ai_date_group[GribJumpRequestMapper.map_from_dict({"param": "129"})]
        subselection_group = ai_date_param_group[GribJumpRequestMapper.map_from_dict({"time": "0000", "levelist": "1000"})]
        tmp = subselection_group[ GribJumpRequestMapper.map_from_dict({"domain": "g", "expver": "0001", "stream": "oper", "levtype": "pl", "type": "fc", "step": "0"}) ]

        print(tmp.shape)

        print(tmp)
        print(subselection_group.tree())
        print(subselection_group.info)

        print(ai_date_param_group)
        print(ai_date_param_group.tree())
        print(ai_date_param_group.info)

        assert len(tmp) == 1
        assert len(root) == 4992
        assert len(subselection_group) == 28
        assert len(ai_date_param_group) == 728

    def test_retrieve_values_from_group_field(self):
        store = FDBStore()
        root = zarr.group(store=store, chunk_store=None)

        ai_group = root[GribJumpRequestMapper.map_from_dict({"class": "ai"})]
        ai_date_group = ai_group[GribJumpRequestMapper.map_from_dict({"date": "20240601"})]

        ai_date_param_group = ai_date_group[GribJumpRequestMapper.map_from_dict({"param": "129"})]
        subselection_group = ai_date_param_group[GribJumpRequestMapper.map_from_dict({"time": "0000", "levelist": "1000"})]
        tmp = subselection_group[ GribJumpRequestMapper.map_from_dict({"domain": "g", "expver": "0001", "stream": "oper", "levtype": "pl", "type": "fc", "step": "0"}) ]

        print(tmp[0, 0, 0, :])

    def test_time_slicing(self):
        store = FDBStore()
        root = zarr.group(store=store, chunk_store=None)

        tmp_time_slice = root[ '{"class": "ai", "date": "20240601/20240602/20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
        tmp_850 = root[ '{"class": "ai", "date": "20240601", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
        tmp_850_2 = root[ '{"class": "ai", "date": "20240602", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]
        tmp_850_3 = root[ '{"class": "ai", "date": "20240603", "domain": "g", "expver": "0001", "stream": "oper", "time": "0000", "levtype": "pl", "type": "fc", "levelist": "850", "param": "129", "step": "0"}' ]

        tmp_all_slice = tmp_time_slice[0:3, 0, 0, 10:20]

        np.testing.assert_array_equal(tmp_all_slice[0], tmp_850[0, 0, 0, 10:20])
        np.testing.assert_array_equal(tmp_all_slice[1], tmp_850_2[0, 0, 0, 10:20])
        np.testing.assert_array_equal(tmp_all_slice[2], tmp_850_3[0, 0, 0, 10:20])
