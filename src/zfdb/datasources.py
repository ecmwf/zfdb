# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Datasources

Contains implementations of datasources and factory functions for crating them.
"""

import itertools
import math
from functools import cache

import eccodes
import numpy as np
import pyfdb
import pygribjump

from .error import ZfdbError
from .request import Request, into_mars_request_dict
from .zarr import DataSource, DotZarrArray, DotZarrAttributes


class ConstantValue(DataSource):
    """
    This DataSource represents a single constant value.
    Provided for testing.
    """

    def __init__(self, value: int):
        self._value = value

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=(1,), chunks=(1,), dtype="int32", order="C", fill_value=self._value
        )

    def create_dot_zarr_attrs(self) -> DotZarrAttributes:
        return DotZarrAttributes()

    def chunks(self) -> tuple[int]:
        return (0,)

    def __getitem__(self, _) -> bytes:
        # This tells zarr to use the fill value
        raise KeyError

    def __contains__(self, key) -> bool:
        return key == "0"


class ConstantValueField(DataSource):
    """
    This DataSource represents n-dimentsional data with a uniform constant value.
    Provided for testing.
    """

    def __init__(
        self, value: int, shape: tuple[int, ...], chunks: tuple[int, ...]
    ) -> None:
        self._value = value
        if len(shape) != len(chunks):
            raise ZfdbError()
        self._shape = shape
        self._chunks = chunks
        self._chunk_counts = [
            math.ceil(s / c) for s, c in zip(self._shape, self._chunks)
        ]
        self._data = np.full(
            shape=(self._chunks), fill_value=self._value, dtype=np.int32, order="C"
        )

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=self._shape,
            chunks=self._chunks,
            dtype="int32",
            order="C",
            fill_value=self._value,
        )

    def create_dot_zarr_attrs(self) -> DotZarrAttributes:
        return DotZarrAttributes()

    def chunks(self) -> tuple[int]:
        return (0,)

    def __getitem__(self, _) -> bytes:
        # This tells zarr to use the fill value
        raise KeyError

    def __contains__(self, key) -> bool:
        return False


class NDarraySource(DataSource):
    """
    Uses a numpy.ndarray as backend.
    """

    def __init__(self, array: np.ndarray) -> None:
        self._array = array

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=self._array.shape,
            chunks=self._array.shape,
            dtype=str(self._array.dtype),
            order="C",
            # TODO(kkratz): Replace hard-coded 'C'
            # order=self._array.dtype.order,
        )

    def create_dot_zarr_attrs(self) -> DotZarrAttributes:
        return DotZarrAttributes()

    def chunks(self) -> tuple[int, ...]:
        return (1,) * len(self._array.shape)

    @cache
    def __getitem__(self, key: tuple[int, ...]) -> bytes:
        # TODO(kkratz): Currently duplicates memory used because of the caced copy of bytes,
        # but zarr does not work on a memoryview
        if len(key) != self._array.ndim:
            raise KeyError
        if any(x != 0 for x in key):
            raise KeyError
        return self._array.tobytes()

    def __contains__(self, key) -> bool:
        if len(key) != self._array.ndim:
            return False
        if any(x != 0 for x in key):
            return False
        return True


class FdbSource(DataSource):
    """
    Uses FDB as a backend.
    Data is retrieved from FDB and assembled on each access.
    """

    def __init__(
        self,
        *,
        extractor: str = "eccodes",
        fdb: pyfdb.FDB | None = None,
        gribjump: pygribjump.GribJump | None = None,
        request: Request | list[Request],
    ) -> None:
        if extractor == "eccodes":
            self.extract = self._extract_with_eccodes
        elif extractor == "gribjump":
            self.extract = self._extract_with_gribjump
        else:
            raise ZfdbError("Unkown extractor specified.")
        if not fdb:
            fdb = pyfdb.FDB()
        self._fdb = fdb
        if not gribjump:
            gribjump = pygribjump.GribJump()
        self._gribjump = gribjump
        if isinstance(request, Request):
            self._requests = [request]
        else:
            self._requests = request

        ## INFO(TKR): Here in the retrieve call r[0] led to quite a strange request:
        # Instead of selecting >20200101</20200102 the date was modified as such: 2
        # This is due to selecting on a string instead of array, which returned the first index.
        # Chunk axis has a _date entry like: _date: '20200101/20200102' instead of an array.
        # There was an wrongly placed to_mars_representation in the init method of request
        # I separated the into_mars function into one operating on dict for the transformation to a mars request dict
        # and one function for the transformation of the individual values.
        [print(r[0]) for r in self._requests]
        streams = [self._fdb.retrieve(r[0]) for r in self._requests]
        messages = itertools.chain.from_iterable(
            [eccodes.StreamReader(s) for s in streams]
        )

        field_count = 0
        self._field_names = []
        field_size = None
        for msg in messages:
            field_count += 1
            self._field_names.append(
                {"level": msg.get("level"), "name": msg.get("shortName")}
            )
            this_field_size = msg.get("numberOfDataPoints")
            if not field_size:
                field_size = this_field_size
            elif field_size != this_field_size:
                raise ZfdbError(
                    f"Found different field sizes {field_size} and {this_field_size}"
                )

        # TODO(kkratz): This needs to be made generic
        num_chunks = len(self._requests[0].chunk_axis())
        self._shape = (num_chunks, field_count, int(1), field_size)
        self._chunks = (1, field_count, 1, field_size)
        self._chunks_per_dimension = tuple(
            [math.ceil(a / b) for (a, b) in zip(self._shape, self._chunks)]
        )

    def create_dot_zarr_array(self) -> DotZarrArray:
        return DotZarrArray(
            shape=self._shape,
            chunks=self._chunks,
            dtype="float32",
            order="C",
        )

    def create_dot_zarr_attrs(self) -> DotZarrAttributes:
        return DotZarrAttributes(variables=self._field_names)

    def chunks(self) -> tuple[int, ...]:
        return self._chunks_per_dimension

    def __getitem__(self, key: tuple[int, ...]) -> bytes:
        if len(key) != len(self._shape):
            raise KeyError
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            raise KeyError
        return self.extract(key)

    def __contains__(self, key: tuple[int, ...]) -> bool:
        if len(key) != len(self._shape):
            return False
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            return False
        return True

    def _extract_with_eccodes(self, key) -> bytes:
        buffer = np.zeros(self._chunks, dtype="float32")
        streams = [
            eccodes.StreamReader(self._fdb.retrieve(r[key[0]])) for r in self._requests
        ]
        for idx, msg in enumerate(itertools.chain.from_iterable(streams)):
            buffer[0, idx, 0, :] = msg.data
        return buffer.tobytes()

    def _extract_with_gribjump(self, key) -> bytes:
        polyrequest = [
            (list_result["keys"], [(0, self._shape[3])])
            for list_result in itertools.chain.from_iterable(
                [self._fdb.list(r[key[0]]) for r in self._requests]
            )
        ]

        gj_result = self._gribjump.extract(polyrequest)
        buffer = np.zeros(self._chunks, dtype="float32")
        for idx, field in enumerate(gj_result):
            buffer[0, idx, 0, :] = field[0][0][0]
        return buffer.tobytes()


def make_dates_source(
    start: np.datetime64, stop: np.datetime64, interval: np.timedelta64
) -> NDarraySource:
    dates_count = 1 + (stop - start) // interval
    array = np.zeros(dates_count, dtype="datetime64[s]")
    for idx in range(0, dates_count):
        array[idx] = start + interval * idx
    return NDarraySource(array)


def make_lat_long_sources(
    fdb: pyfdb.FDB,
    request: dict,
) -> tuple[NDarraySource, NDarraySource]:
    request = into_mars_request_dict(request)
    msg = fdb.retrieve(request)
    content = msg.read()
    gid = eccodes.codes_new_from_message(bytes(content))
    length = int(eccodes.codes_get(gid, "numberOfDataPoints"))
    lat = np.ndarray(
        length, dtype="float64", buffer=eccodes.codes_get_double_array(gid, "latitudes")
    )
    lon = np.ndarray(
        length,
        dtype="float64",
        buffer=eccodes.codes_get_double_array(gid, "longitudes"),
    )
    eccodes.codes_release(gid)

    return NDarraySource(lat), NDarraySource(lon)
