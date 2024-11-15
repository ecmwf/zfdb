from pathlib import Path
import zarr

from zarr.convenience import open_group

from zfdb.business import FdbZarrMapping
from zfdb.business.MarsRequestBuilder import MarsRequestBuilder


class TestAnemoiCmp:
    def test_simple_anemoi_cmp(self, data_path):
        anemoi_root_group = open_group("./tests/data/anemoi_minimal.zarr")
        data = anemoi_root_group["data"]

        mars_request_set = MarsRequestBuilder(
            data_path / "integration" / "recipe.yaml"
        ).build_requests()

        # TODO: TKR
        # store = FDBMapping(mars_join=complex_object, zarr_data_layout=mapper)
        store = FdbZarrMapping(mars_request_set=mars_request_set)
        root = zarr.group(store=store, chunk_store=None)

        build_group = root["_build"]
        root["count"]
        root["dates"]
        root["data"]
        root["has_nans"]
        root["latitudes"]
        root["longitudes"]
        root["maximum"]
        root["mean"]
        root["minimum"]
        root["squares"]
        root["stdev"]
        root["sums"]

        print(anemoi_root_group.tree())
        print(data)
