import json
from typing import List, Optional

from _pytest.legacypath import pytest_load_initial_conftests
import pyfdb
from zarr.storage import Store
from zarr.types import DIMENSION_SEPARATOR

from zfdb.business.GribJumpRequestMerger import GribJumpRequestMerger
from zfdb.business.Request import Request, RequestMapper

from zfdb.business.ZarrKeyMatcher import ZarrKeyMatcher
from zfdb.business.ZarrMetadataBuilder import ZarrMetadataBuilder


class FDBMapping(Store):
    """Storage class using FDB.

    .. note:: This is an experimental feature.

    Requires the `pyFDB <https://redis-py.readthedocs.io/>`_
    package to be installed.

    Parameters
    ----------
    prefix : string
        Name of prefix for FDB keys
    dimension_separator : {'.', '/'}, optional
        Separator placed between the dimensions of a chunk.
    **kwargs
        Keyword arguments passed through to the `redis.Redis` function.

    """

    def __init__(
        self,
        mars_request_set,
        prefix="",
        dimension_separator: Optional[DIMENSION_SEPARATOR] = None,
        **kwargs,
    ):
        self._prefix = prefix
        self._kwargs = kwargs
        self._dimension_separator = dimension_separator

        self.gribjump_merger = GribJumpRequestMerger()
        self.fdb = pyfdb.FDB()

        self._readable = True
        self._listable = True
        self._erasable = False
        self._writeable = False

        self.known_groups = [
            "_build"
        ]
        self.known_arrays = [
            "count",
            "dates",
            "data",
            "has_nans",
            "latitudes",
            "longitudes",
            "maximum",
            "mean",
            "minimum",
            "squares",
            "stdev",
            "sums",
            "_build/flags",
            "_build/lengths",
        ]

        for req in mars_request_set:
            self.gribjump_merger

    def __contains__(self, _key) -> bool:
        if _key == ".zgroup":
            return True

        # Avoid initialization of the zarr array
        # We are implementing a view
        if _key == ".zarray" or self.plain_group_array(_key):
            return False

        # request: Request = RequestMapper.map_from_raw_input_dict(_key)
        #
        # if ZarrKeyMatcher.is_group(request):
        #     # TODO: Combine prefix to a under-specified mars request and check
        #     # whether it's in the axis object.
        #
        #     if request.is_fully_specified():
        #         return False
        #     else:
        #         return True
        #
        # if ZarrKeyMatcher.is_array(request):
        #     if request.is_fully_specified():
        #         return True
        #     else:
        #         return False
        #
        # if ZarrKeyMatcher.is_group_shape_information(request):
        #     return False
        #
        # if ZarrKeyMatcher.has_chunking(request):
        #     return True
        #
        # return False

    def _key(self, key):
        return f"{self._prefix}:{key}"

    def plain_group_array(self, key):
        for group in self.known_groups:
            if key == group + "/.zarray":
                return True

        return False

    def is_group(self, key):
        if key == ".zgroup":
            return True
        for group in self.known_groups:
            if key == group + "/.zgroup":
                return True

    def is_known_array(self, key):
        for array in self.known_arrays:
            if key == array + "/.zarray":
                return True

    # build/.zarray
    # build/.zgroup
    # build/.zattrs
    # .zarray
    # .zgroup
    # .zattrs
    def __getitem__(self, key):
        # Faking the zarr 2 file format
        if self.is_group(key):
            return '{"zarr_format": 2}'

        if self.plain_group_array(key):
            raise KeyError("No array found")

        if self.is_known_array(key):
            # Do mapping magic here.
            # - Create several mars requests
            # - Do those
            # - Merge the results accordingly
            pass

    def __setitem__(self, key, value):
        raise NotImplementedError("This method is not implemented")

    def __delitem__(self, key):
        raise NotImplementedError("This method is not implemented")

    def keylist(self) -> list[Request]:
        listIterator = self.fdb.list(keys=True)
        return [RequestMapper.map_from_dict(it["keys"]) for it in listIterator]

    def __iter__(self):
        yield from self.keylist()

    def __len__(self):
        return len(self.keylist())

    def __getstate__(self):
        raise NotImplementedError("This method is not implemented")

    def __setstate__(self, state):
        prefix, kwargs = state
        self.__init__(prefix=prefix, **kwargs)

    def clear(self):
        raise NotImplementedError("This method is not implemented")

    @staticmethod
    def _filter_group(
        raw_mars_request: dict[str, list[str]], group_request: dict[str, list[str]]
    ):
        for key in group_request.keys():
            if key in raw_mars_request:
                for value in group_request[key]:
                    if value in raw_mars_request[key]:
                        continue
                    else:
                        return False

        return True

    def listdir(self, path: str = "") -> List[str]:
        listIterator = self.keylist()
        raw_group_request = RequestMapper.map_from_raw_input_dict(
            path
        ).build_mars_request()

        result = []

        for it in listIterator:
            raw_mars_request = it.build_mars_request()

            if FDBMapping._filter_group(raw_mars_request, raw_group_request):
                result.append(json.dumps(it.build_mars_keys_span()))

        return result

    def rmdir(self, path: str = "") -> None:
        raise NotImplementedError("This method is not implemented")
