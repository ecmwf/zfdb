# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import math
from pathlib import Path

import zarr
import zarr.storage
from zarr.abc.store import Store

from zfdb.mapping import FdbZarrStore
from zfdb.zarr import from_cpu_buffer


def zarr_groups(group: zarr.Group):
    """
    Zarr does not provide and out of the box way to traverse all groups in a
    tree of groups as they do with arrays hence we need to provide one.
    """
    for child in group.groups():
        yield child
        yield from zarr_groups(child[1])


def compare_zarr_stores(
    store_a: zarr.storage.StoreLike, store_b: zarr.storage.StoreLike
) -> bool:
    """
    Takes two zar stores and compares all groups/arrays in the store.
    Two groups are identical if their path is identical
    Two arrays are identical if:
        - their path match
        - hash of the uncompressed data match
        - .zattrs and .zarray match
    """
    a = zarr.open_group(store_a, mode="r", zarr_format=2)
    b = zarr.open_group(store_b, mode="r", zarr_format=2)
    if isinstance(a, zarr.Array) and isinstance(b, zarr.Array):
        return a._async_array.__hash__ == b._async_array.__hash__
    if isinstance(a, zarr.Group) and isinstance(b, zarr.Group):
        return all(
            array_a[0] == array_b[0]
            and array_a[1]._async_array.__hash__ == array_b[1]._async_array.__hash__
            for (array_a, array_b) in zip(a.arrays(), b.arrays(), strict=True)
        ) and all(
            group_a[0] == group_b[0]
            for (group_a, group_b) in zip(zarr_groups(a), zarr_groups(b), strict=True)
        )
    return False


def print_in_closest_unit(val_in_ns) -> str:
    """
    Prints a nano second time value as human readable string.
    10 -> '10ns'
    1000 -> '10μs'
    1000000 - > '10ms'

    raises ValueError on values >= 1e12
    """
    units = ["ns", "μs", "ms", "s"]
    exp = int(math.log10(val_in_ns)) // 3
    if exp < len(units):
        return f"{val_in_ns / 10 ** (exp * 3)}{units[exp]}"
    raise ValueError


async def copy(mapping: FdbZarrStore, store, dest: Store):
    if not dest.supports_writes:
        raise RuntimeError("Destination store doesn't support write")

    known_entries = [
        Path(entry)
        for entry in mapping._build_paths(mapping._child)
        if entry.endswith(".json")
    ]

    for entry in known_entries:
        meta_data = await mapping.get(entry.as_posix())
        meta_data_dict = from_cpu_buffer(meta_data)

        entry = entry

        if meta_data_dict["node_type"] == "group":
            print(f"Found group: {entry}")
            array_name = entry.parent

        if meta_data_dict["node_type"] == "array":
            print(f"Found array: {entry}")
            array_name = entry.parent
            array = store.get(array_name.as_posix())
            array = array
