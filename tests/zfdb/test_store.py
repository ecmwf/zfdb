import numpy as np
import yaml
import zarr
import zarr.storage
from support.util import compare_zarr_stores, print_in_closest_unit

from zfdb import FdbZarrMapping
from zfdb.datasources import ConstantValue, ConstantValueField, make_dates_source
from zfdb.mapping import make_anemoi_dataset_like_view
from zfdb.zarr import FdbZarrArray, FdbZarrGroup


def test_mapping(read_only_fdb_setup) -> None:
    tmp_path, data_path = read_only_fdb_setup
    recipe_path = (
        data_path
        / "recipes"
        / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml"
    )
    recipe = yaml.safe_load(recipe_path.read_text())

    mapping = make_anemoi_dataset_like_view(recipe=recipe)
    store = zarr.open_group(mapping, mode="r")
    zstore = zarr.DirectoryStore(tmp_path / "xx.zarr")
    zarr.copy_store(store.store, zstore)
    assert compare_zarr_stores(store.store, zstore)
    anemoi_store = zarr.open_group(
        data_path / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.zarr",
        mode="r",
    )

    import time

    timings = {"both": [], "zarr": [], "fdb": []}
    for chunk_idx in range(anemoi_store["data"].chunks[0]):
        t0 = time.perf_counter_ns()
        expected = anemoi_store["data"][chunk_idx][0][0]
        t1 = time.perf_counter_ns()
        actual = store["data"][chunk_idx][6][0]
        t2 = time.perf_counter_ns()
        assert np.array_equal(expected, actual)
        timings["both"].append(t2 - t0)
        timings["zarr"].append(t1 - t0)
        timings["fdb"].append(t2 - t1)

    print(f"avg timings both = {print_in_closest_unit(np.average(timings['both']))}")
    print(f"avg timings zarr = {print_in_closest_unit(np.average(timings['zarr']))}")
    print(f"avg timings fdb = {print_in_closest_unit(np.average(timings['fdb']))}")


def test_fdb_zarr_store_can_be_copied_by_zarr_python(tmp_path) -> None:
    mapping = FdbZarrMapping(
        FdbZarrGroup(
            children=[
                FdbZarrGroup(name="xxx"),
                FdbZarrGroup(
                    name="retrograde",
                    children=[
                        FdbZarrArray(name="value1", datasource=ConstantValue(6)),
                        FdbZarrArray(
                            name="value2",
                            datasource=ConstantValueField(
                                value=123, shape=(100, 10, 1000), chunks=(100, 10, 100)
                            ),
                        ),
                        FdbZarrArray(
                            name="dates",
                            datasource=make_dates_source(
                                np.datetime64("1979-01-01T00:00:00", "s"),
                                np.datetime64("2024-01-01T00:00:00", "s"),
                                np.timedelta64(6, "h"),
                            ),
                        ),
                    ],
                ),
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r")
    zstore = zarr.DirectoryStore(tmp_path / "tt.zarr")
    zarr.copy_store(store.store, zstore)
    assert compare_zarr_stores(store.store, zstore)
