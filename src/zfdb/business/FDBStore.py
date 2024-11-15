from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from math import ceil

import numpy as np
from pyfdb import FDB

from zfdb.business.ZarrMetadataBuilder import (
    DotZarrArray,
    DotZarrAttributes,
    DotZarrGroup,
)


class ZfdbError(Exception):
    pass


class DataSource(ABC):
    @abstractmethod
    def create_dot_zarr_array(self) -> DotZarrArray: ...

    def __getitem__(self, key) -> bytes: ...

    def __contains__(self, key) -> bool: ...


class ConstantValue(DataSource):
    def __init__(self, value: int):
        self._value = value

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(shape=[1], chunks=[1], dtype="int32", order="C")

    def __getitem__(self, key) -> bytes:
        if key == "0":
            return self._value.to_bytes(4, "little")
        raise KeyError

    def __contains__(self, key) -> bool:
        return key == "0"


class ConstantValueField(DataSource):
    """
    Created just for testing purposes
    """

    def __init__(self, value: int, shape: list[int], chunks: list[int]) -> None:
        self._value = value
        if len(shape) != len(chunks):
            raise ZfdbError()
        self._shape = shape
        self._chunks = chunks
        self._chunk_counts = [ceil(s / c) for s, c in zip(self._shape, self._chunks)]
        self._data = np.full(
            shape=(self._chunks), fill_value=self._value, dtype=np.int32, order="C"
        )

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=self._shape, chunks=self._chunks, dtype="int32", order="C"
        )

    def __getitem__(self, key) -> bytes:
        index = [int(s) for s in key.split(".")]
        if len(index) != len(self._shape):
            raise KeyError
        for d, c in zip(index, self._chunk_counts):
            if not (0 <= d < c):
                raise KeyError
        # TODO(kkratz): this copies the data into a new bytes obj
        return self._data.tobytes()

    def __contains__(self, key) -> bool:
        index = [int(s) for s in key.split(".")]
        for d, c in zip(index, self._chunk_counts):
            if not (0 <= d < c):
                return False
        return True


class DatesSource(DataSource):
    def __init__(
        self,
        start: np.datetime64,
        stop: np.datetime64,
        interval: np.timedelta64,
        chunk: int,
    ) -> None:
        self._start = start
        self._stop = stop
        self._interval = interval
        self._chunk = chunk
        self._chunks = [chunk]
        count = (stop - start) // interval
        if start + count * interval == stop:
            count += 1
        self._data = [
            np.zeros(shape=[self._chunk], dtype="datetime64[s]")
            for _ in range(0, count)
        ]
        self._shape = [int(count)]
        self._chunk_count = ceil(count / self._chunk)
        for idx in range(0, count):
            chunk_id = idx // self._chunk
            self._data[chunk_id][idx - chunk_id * self._chunk] = (
                self._start + self._interval * idx
            )

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=self._shape, chunks=self._chunks, dtype="datetime64[s]", order="C"
        )

    def __getitem__(self, key) -> bytes:
        # TODO(kkratz): better error handling & parsing
        index = int(key)
        if not (0 <= index < self._chunk_count):
            raise KeyError
        # TODO(kkratz): this is inefficient, but good enough for a prototype though
        # copies the data into a new bytes obj
        return self._data[index].tobytes()

    def __contains__(self, key) -> bool:
        # TODO(kkratz): better error handling & parsing
        index = int(key)
        if not (0 <= index < self._chunk_count):
            return False
        return True


class FdbZarrArray:
    def __init__(self, path: str, *, datasource=None):
        # full path to the array, e.g foo/bar without .zarray
        self._path = path
        self._datasource = datasource
        if self._datasource:
            self._metadata = self._datasource.create_dot_zarr_array()
        else:
            self._metadata = DotZarrArray(shape=[1], chunks=[1], dtype=">i4", order="C")
        self._attributes = DotZarrAttributes()

    def __contains__(self, key: str) -> bool:
        if key == ".zarray":
            return True
        if key == ".zattrs":
            return True
        if self._datasource:
            return key in self._datasource
        # TODO(kkratz): delegate to datasource to check if this exists
        # return key in datasource

        # this return needs to move into the datasource
        return False

    def __getitem__(self, key: str) -> bytes:
        if key == ".zarray":
            return self._metadata.asbytes()
        if key == ".zattrs":
            return self._attributes.asbytes()
        if self._datasource:
            return self._datasource[key]
        # TODO(kkratz): delegate to datasource to check if this exists
        # return datasource[key]

        # this raise needs to move into the datasource
        raise KeyError(f"Key {key} not found")

    @property
    def path(self) -> str:
        return self._path


class FdbZarrGroup:
    def __init__(self, path: str):
        # full path to the group, e.g foo/bar without .zgroup
        self._path = path
        self._metadata = DotZarrGroup()

    def __contains__(self, key) -> bool:
        return key == ".zgroup"

    def __getitem__(self, key: str) -> bytes:
        if key == ".zgroup":
            return self._metadata.asbytes()
        raise KeyError(f"Key {key} not found")

    @property
    def path(self) -> str:
        return self._path


class FdbZarrMapping(MutableMapping):
    """Provide access to FDB with a MutableMapping.

    .. note:: This is an experimental feature.

    Requires the `pyFDB <https://redis-py.readthedocs.io/>`_
    package to be installed.

    Parameters
    ----------
    """

    def __init__(
        self,
        mars_request_set,
        fdb_config=None,
    ):
        self._fdb = FDB(config=fdb_config)

        # TODO(kkratz): populate below structure based on recepie.yaml
        self._known_paths = {
            x.path: x
            for x in [
                FdbZarrGroup(""),  # Don't forget the root group
                FdbZarrArray("count"),
                FdbZarrArray(
                    "dates",
                    datasource=DatesSource(
                        start=np.datetime64("1979-01-01T00:00:00", "s"),
                        stop=np.datetime64("2022-12-31T18:00:00", "s"),
                        interval=np.timedelta64(6, "h"),
                        chunk=2,
                    ),
                ),
                FdbZarrArray("data"),
                FdbZarrArray("has_nans"),
                FdbZarrArray("latitudes"),
                FdbZarrArray("longitudes"),
                FdbZarrArray("maximum"),
                FdbZarrArray("mean"),
                FdbZarrArray("minimum"),
                FdbZarrArray("squares"),
                FdbZarrArray("stdev"),
                FdbZarrArray("sums"),
                FdbZarrArray("_build/flags", datasource=ConstantValue(7)),
                FdbZarrArray("_build/lengths"),
                FdbZarrGroup("_build"),  # Don't forget the root group
            ]
        }

    def __getitem__(self, key):
        prefix, _, suffix = key.rpartition("/")
        if arr := self._known_paths.get(prefix):
            return arr[suffix]
        raise KeyError(f"Key: {key} not found")

    def __iter__(self):
        yield from self._known_paths

    def __len__(self):
        return len(self._known_paths)

    def __setitem__(self, _k, _v):
        # TODO(kkratz): should raise proper exception
        pass

    def __delitem__(self, _k):
        # TODO(kkratz): should raise proper exception
        pass

    def __contains__(self, key) -> bool:
        prefix, _, suffix = key.rpartition("/")
        if arr := self._known_paths.get(prefix):
            return suffix in arr
        return False
