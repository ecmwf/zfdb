"""Datasources

Contains implementations of datasources and factory functions for crating them.
"""

import math
from functools import cache

import earthkit.data as ekd
import numpy as np
import pyfdb
import pygribjump
import itertools

from .error import ZfdbError
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
        # but zarr dies not work on a memoryview
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
        fdb: pyfdb.FDB,
        gribjump: pygribjump.GribJump,
        mars_requests: list[dict],
        start: np.datetime64,
        stop: np.datetime64,
        interval: np.timedelta64,
    ) -> None:
        self._fdb = fdb
        self._gribjump = gribjump
        self._mars_requests = mars_requests
        self._start = start
        self._stop = stop
        self._interval = interval
        self._gj = pygribjump.GribJump()
        # TODO(kkratz): This needs to do the same as the anemoi datasets code: issue a
        # query for one datetime and infer the layout from the returned data after
        # joining all requests.

        # shape -> [datetime, field_id, ensemble, field_data]
        dates_count = 1 + (stop - start) // interval
        fields_count = self._extract_fields_count_from_mars_request(mars_requests)

        reference_mars_request = mars_requests[0].copy()
        date, time = np.datetime_as_string(start, unit="s").split("T")
        reference_mars_request["date"] = date
        reference_mars_request["time"] = time.split(":")[0]

        # HACK
        values_count = 40320
        # query_result = ekd.from_source("mars", reference_mars_request)
        # values_count = len(query_result[0].grid_points()[0])

        self._shape = (int(dates_count), fields_count, int(1), values_count)
        self._chunks = (1, fields_count, 1, values_count)
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
        date, time = str(self._start).split("T")
        time = time.split(":")[0]

        def to_name(keys) -> str:
            if keys["levtype"] == "pl":
                return f"{keys['param']}_{keys['levelist']}"
            return keys["param"]

        listing = []
        for request in self._mars_requests:
            request = request.copy()
            request["date"] = date.replace("-", "")
            request["time"] = time
            request.pop("grid", None)
            listing.append(
                [to_name(i["keys"]) for i in self._fdb.list(request, keys=True)]
            )

        return DotZarrAttributes(variables=listing)

    def chunks(self) -> tuple[int, ...]:
        return self._chunks_per_dimension

    def __getitem__(self, key: tuple[int, ...]) -> bytes:
        if len(key) != len(self._shape):
            raise KeyError
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            raise KeyError
        # NOTE(kkratz): This codew only supports chunking along the datetime axis
        t = self._start + self._interval * key[0]
        date, time = str(t).split("T")
        time = time.split(":")[0]
        polyrequests = []
        for request in self._mars_requests:
            request = request.copy()
            request["date"] = date.replace("-", "")
            request["time"] = time
            request.pop("grid", None)
            polyrequests.append(
                [
                    (i["keys"], [(0, self._shape[3])])
                    for i in self._fdb.list(request, keys=True)
                ]
            )
        gj_results = [
            self._gribjump.extract(polyrequest) for polyrequest in polyrequests
        ]
        buffer = np.zeros(self._chunks, dtype="float32")
        for idx, field in enumerate(itertools.chain.from_iterable(gj_results)):
            buffer[0, idx, 0, :] = field[0][0][0]
        return buffer.tobytes()

    def __contains__(self, key: tuple[int, ...]) -> bool:
        if len(key) != len(self._shape):
            return False
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            return False
        return True

    @staticmethod
    def _extract_fields_count_from_mars_request(mars_requests: list[dict]) -> int:
        return sum(
            [len(req["param"]) * len(req.get("level", [1])) for req in mars_requests]
        )


def make_dates_source(
    start: np.datetime64, stop: np.datetime64, interval: np.timedelta64
) -> NDarraySource:
    dates_count = 1 + (stop - start) // interval
    array = np.zeros(dates_count, dtype="datetime64[s]")
    for idx in range(0, dates_count):
        array[idx] = start + interval * idx
    return NDarraySource(array)


def make_lat_long_sources(
    reference_mars_request: dict,
) -> tuple[NDarraySource, NDarraySource]:
    query_result = ekd.from_source("mars", reference_mars_request)
    gp = query_result[0].grid_points()
    return NDarraySource(
        np.ndarray(len(gp[0]), dtype="float64", buffer=gp[0])
    ), NDarraySource(np.ndarray(len(gp[1]), dtype="float64", buffer=gp[1]))
