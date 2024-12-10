import numpy as np
import yaml
import zarr
import zarr.storage
from support.util import compare_zarr_stores

from zfdb import FdbZarrMapping
from zfdb.datasources import ConstantValue, ConstantValueField, make_dates_source
from zfdb.mapping import build_fdb_store_from_recepie
from zfdb.zarr import FdbZarrArray, FdbZarrGroup


def test_mapping(read_only_fdb_setup) -> None:
    tmp_path, data_path = read_only_fdb_setup
    recepie_path = (
        data_path
        / "recepies"
        / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml"
    )
    recepie = yaml.safe_load(recepie_path.read_text())

    mapping = build_fdb_store_from_recepie(recepie=recepie)
    store = zarr.open_group(mapping, mode="r")
    # zstore = zarr.DirectoryStore(tmp_path / "xx.zarr")
    # zarr.copy_store(store.store, zstore)
    # assert compare_zarr_stores(store.store, zstore)
    anemoi_store = zarr.open_group(
        data_path / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.zarr",
        mode="r",
    )

    for expected_chunk, actual_chunk in zip(anemoi_store["data"], store["data"]):
        expected = expected_chunk[0][0]
        actual = actual_chunk[6][0]
        assert np.array_equal(expected, actual)


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
