# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import copy

import numpy as np
import pytest
import yaml
import zarr
import zarr.storage
from utils.util import copy as store_copy

from zfdb import (
    ChunkAxisType,
    ConstantValue,
    ConstantValueField,
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup,
    FdbZarrStore,
    Request,
    make_anemoi_dataset_like_view,
    make_dates_source,
)
from zfdb.zarr import from_cpu_buffer


def test_zarr_use_spec_v2(read_only_fdb_setup) -> None:
    assert zarr.config.get("default_zarr_format") == 3


@pytest.mark.asyncio
def test_make_anemoi_dataset_like_view(read_only_fdb_setup) -> None:
    tmp_path, data_path = read_only_fdb_setup
    recipe_path = data_path / "recipes" / "example.yaml"
    recipe = yaml.safe_load(recipe_path.read_text())

    mapping = make_anemoi_dataset_like_view(recipe=recipe)
    store = zarr.open_group(mapping, mode="r")
    data = store.get("data")

    assert data
    query = copy.deepcopy(data[:, :, :, :])
    assert np.array_equal(query, data[:, :, :, :])


@pytest.mark.asyncio
async def test_make_anemoi_dataset_like_view(read_only_fdb_setup) -> None:
    tmp_path, data_path = read_only_fdb_setup
    recipe_path = data_path / "recipes" / "example.yaml"
    recipe = yaml.safe_load(recipe_path.read_text())

    mapping = make_anemoi_dataset_like_view(recipe=recipe)
    metadata_bytes = await mapping.get(".zmetadata")

    assert metadata_bytes
    metadata_string = from_cpu_buffer(metadata_bytes)

    assert "metadata" in metadata_string
    assert len(metadata_string["metadata"]) > 0


def test_make_view(read_only_fdb_setup) -> None:
    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(
                        request=[
                            Request(
                                request={
                                    "date": np.arange(
                                        np.datetime64("2020-01-01"),
                                        np.datetime64("2020-01-03"),
                                    ),
                                    "time": ["00", "06", "12", "18"],
                                    "class": "ea",
                                    "domain": "g",
                                    "expver": "0001",
                                    "stream": "oper",
                                    "type": "an",
                                    "step": "0",
                                    "levtype": "sfc",
                                    "param": ["10u", "10v"],
                                },
                                chunk_axis=ChunkAxisType.DateTime,
                            )
                        ]
                    ),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)


@pytest.mark.asyncio
async def test_access_test(read_only_fdb_setup) -> None:
    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(
                        request=[
                            Request(
                                request={
                                    "date": np.arange(
                                        np.datetime64("2020-01-01"),
                                        np.datetime64("2020-01-03"),
                                    ),
                                    "time": ["00", "06", "12", "18"],
                                    "class": "ea",
                                    "domain": "g",
                                    "expver": "0001",
                                    "stream": "oper",
                                    "type": "an",
                                    "step": "0",
                                    "levtype": "sfc",
                                    "param": ["10u", "10v"],
                                },
                                chunk_axis=ChunkAxisType.DateTime,
                            )
                        ]
                    ),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data
    print(data[:, :, :, :])


@pytest.mark.asyncio
async def test_access_listing_test(read_only_fdb_setup) -> None:
    mapping = FdbZarrStore(
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

    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("retrograde/value1")
    assert data[0] == 6

    data2 = store.get("retrograde/value2")
    assert np.all(data2[:] == 123)


# @pytest.mark.asyncio
# async def test_fdb_zarr_store_can_be_copied_by_zarr_python(tmp_path) -> None:
#     mapping = FdbZarrStore(
#         FdbZarrGroup(
#             children=[
#                 FdbZarrGroup(name="xxx"),
#                 FdbZarrGroup(
#                     name="retrograde",
#                     children=[
#                         FdbZarrArray(name="value1", datasource=ConstantValue(6)),
#                         FdbZarrArray(
#                             name="value2",
#                             datasource=ConstantValueField(
#                                 value=123, shape=(100, 10, 1000), chunks=(100, 10, 100)
#                             ),
#                         ),
#                         FdbZarrArray(
#                             name="dates",
#                             datasource=make_dates_source(
#                                 np.datetime64("1979-01-01T00:00:00", "s"),
#                                 np.datetime64("2024-01-01T00:00:00", "s"),
#                                 np.timedelta64(6, "h"),
#                             ),
#                         ),
#                     ],
#                 ),
#             ]
#         )
#     )
#     store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
#     zstore = zarr.storage.LocalStore(root=tmp_path / "tt.zarr")
#     await store_copy(mapping, store, zstore)
#     assert compare_zarr_stores(store.store, zstore)
