from pathlib import Path
import zarr

from zarr.convenience import open_group

from zfdb.business import FDBMapping
from zfdb.business.MarsRequestBuilder import MarsRequestBuilder

class TestAnemoiCmp:
    def test_simple_anemoi_cmp(self, data_path):

        anemoi_root_group = open_group("./tests/data/anemoi_minimal.zarr")
        data = anemoi_root_group["data"]

        mars_request_set = MarsRequestBuilder(data_path / "integration" / "mars_requests").build_requests()

        # TODO: TKR
        # store = FDBMapping(mars_join=complex_object, zarr_data_layout=mapper)
        store = FDBMapping(mars_request_set=mars_request_set)
        root = zarr.group(store=store, chunk_store=None)

        test =  root[{'CLASS': 'OD', 'TYPE': 'AN', 'STREAM': 'OPER', 'EXPVER': '0001', 'REPRES': 'GG', 'LEVTYPE': 'SFC', 'PARAM': '165/166/168/167/172/151/160/235/163/134/136/129', 'TIME': '0000/0600/1200/1800', 'STEP': '0', 'DOMAIN': 'G', 'TARGET': '1.grb', 'RESOL': 'AUTO', 'GRID': '20/20', 'DATE': '20241101'}]

        list_iter = store.fdb.list(keys=True)

        print([x for x in list_iter])

        # root["data"]
        # root["dates"]
        root[{"class": "od"}]

        print(anemoi_root_group.tree())
        print(data)
