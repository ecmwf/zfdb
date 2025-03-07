# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""
This module contains generator for all dot files that can
be part of a zarr store: '.zarray', '.zgroup' and '.zattrs'
"""

import itertools
import json
from abc import ABC, abstractmethod
from dataclasses import KW_ONLY, asdict, dataclass, field
from functools import cache

import numpy as np

from .error import ZfdbError


@dataclass(frozen=True)
class DotZarrArray:
    _: KW_ONLY
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    # TODO(kkratz) Use enums for allowed types
    dtype: str | list[str]
    order: str
    fill_value: bool | int | float | None = None
    filters: None = None
    compressor: None = None
    dimension_separator: str = "."
    zarr_format: int = 2

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)


@dataclass(frozen=True)
class DotZarrGroup:
    _: KW_ONLY
    zarr_format: int = 2

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)


@dataclass()
class DotZarrAttributes:
    _: KW_ONLY
    copyright: str = "ecmwf"
    variables: list[dict] = field(default_factory=list)

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)


class DataSource(ABC):
    @abstractmethod
    def create_dot_zarr_array(self) -> DotZarrArray: ...

    @abstractmethod
    def create_dot_zarr_attrs(self) -> DotZarrAttributes: ...

    @abstractmethod
    def chunks(self) -> tuple[int, ...]: ...

    @abstractmethod
    def __getitem__(self, key: tuple[int, ...]) -> bytes: ...

    @abstractmethod
    def __contains__(self, key: tuple[int, ...]) -> bool: ...


class FdbZarrArray:
    def __init__(self, *, name: str = "", datasource: DataSource):
        # full path to the array, e.g foo/bar without .zarray
        self._name = name
        self._datasource = datasource
        self._metadata = self._datasource.create_dot_zarr_array()
        self._attributes = self._datasource.create_dot_zarr_attrs()

    def __getitem__(self, key: tuple[str, ...]) -> bytes:
        assert len(key) == 1
        if key[0] == ".zarray":
            return self._metadata.asbytes()
        if key[0] == ".zattrs":
            return self._attributes.asbytes()
        chunk_ids = [int(x) for x in key[0].split(".")]
        return self._datasource[*chunk_ids]

    @property
    def name(self) -> str:
        return self._name

    @cache
    def paths(self) -> list[str]:
        """
        Zarr paths associated to this array, this includes .zarray, .zattrs and all chunks.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        # NOTE(kkratz):
        # Adding chunk paths here has become relevant to support store-to-store with `zarr.copy_store`,
        # zarr 2.18.3 does not seem to rely on those paths for chunk discovery.
        files = [".zarray", ".zattrs"]
        if len(chunks_per_axis := self._datasource.chunks()) > 0:
            tuples = itertools.product(*[np.arange(0, x) for x in chunks_per_axis])
            chunk_names = [".".join([str(i) for i in t]) for t in tuples]
            files += chunk_names
        return files


class FdbZarrGroup:
    def __init__(
        self,
        *,
        name: str = "",
        children: list["FdbZarrGroup | FdbZarrArray"] = [],
    ):
        self._name = name
        self._metadata = DotZarrGroup()
        self._attributes = DotZarrAttributes()
        for c in children:
            if c.name == "":
                raise ZfdbError(
                    "A group with the empty name can only be the root group."
                )
        self._children = {c.name: c for c in children}

    def __getitem__(self, key: tuple[str, ...]) -> bytes:
        if len(key) == 1:
            if key[0] == ".zgroup":
                return self._metadata.asbytes()
            if key[0] == ".zattrs":
                return self._attributes.asbytes()
        else:
            return self._children[key[0]][*key[1:]]
        raise KeyError(f"Unknown key {key}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def children(self) -> list["FdbZarrArray | FdbZarrGroup"]:
        return list(self._children.values())

    def paths(self) -> list[str]:
        """
        Zarr paths associated to this group, excluding child groups or arrays.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        return [".zgroup", ".zattrs"]
