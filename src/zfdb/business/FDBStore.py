import json
from typing import List, Optional
from collections.abc import MutableMapping

from _pytest.legacypath import pytest_load_initial_conftests
import pyfdb

from zfdb.business.GribJumpRequestMerger import GribJumpRequestMerger
from zfdb.business.Request import Request, RequestMapper

from zfdb.business.ZarrKeyMatcher import ZarrKeyMatcher
from zfdb.business.ZarrMetadataBuilder import ZarrMetadataBuilder


class FDBZArray:
    def __init__(self, path: str):
        # full path to the array, e.g foo/bar without .zarray
        self._path = path
        self._metadata = {
            "zarr_format": 2,
            "shape": [1],
            "chunks": [1],
            "dtype": ">i4",
            "compressor": None,
            "fill_value": 666,
            "order": "C",
            "filters": None,
            "dimension_separator": ".",
        }
        self._attributes = dict()

    def __contains__(self, key: str) -> bool:
        if key == ".zarray":
            return True
        if key == ".zattrs":
            return True
        # TODO(kkratz): delegate to datasource to check if this exists
        # return key in datasource

        # this return needs to move into the datasource
        return False

    def __getitem__(self, key: str) -> bytes:
        if key == ".zarray":
            return json.dumps(self._metadata).encode("utf8")
        if key == ".zattrs":
            return json.dumps(self._attributes).encode("utf8")
        # TODO(kkratz): delegate to datasource to check if this exists
        # return datasource[key]

        # this raise needs to move into the datasource
        raise KeyError(f"Key {key} not found")

    @property
    def path(self) -> str:
        return self._path


class FDBZGroup:
    def __init__(self, path: str):
        # full path to the group, e.g foo/bar without .zgroup
        self._path = path
        self._metadata = {"zarr_format": 2}

    def __contains__(self, key) -> bool:
        return key == ".zgroup"

    def __getitem__(self, key: str) -> bytes:
        if key == ".zgroup":
            return json.dumps(self._metadata).encode("utf8")
        raise KeyError(f"Key {key} not found")

    @property
    def path(self) -> str:
        return self._path


class FDBMapping(MutableMapping):
    """Storage class using FDB.

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
        self.gribjump_merger = GribJumpRequestMerger()
        self.fdb = pyfdb.FDB(config=fdb_config)

        self._known_paths = {
            x.path: x
            for x in [
                FDBZGroup(""),  # Don't forget the root group
                FDBZArray("count"),
                FDBZArray("dates"),
                FDBZArray("data"),
                FDBZArray("has_nans"),
                FDBZArray("latitudes"),
                FDBZArray("longitudes"),
                FDBZArray("maximum"),
                FDBZArray("mean"),
                FDBZArray("minimum"),
                FDBZArray("squares"),
                FDBZArray("stdev"),
                FDBZArray("sums"),
                FDBZArray("_build/flags"),
                FDBZArray("_build/lengths"),
                FDBZGroup("_build"),  # Don't forget the root group
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

    # build/.zarray
    # build/.zgroup
    # build/.zattrs
    # .zarray
    # .zgroup
    # .zattrs
    # def __getitem__(self, key):
    #     # Faking the zarr 2 file format
    #     if self.is_group(key):
    #         return '{"zarr_format": 2}'

    #     if self.plain_group_array(key):
    #         raise KeyError("No array found")

    #     if self.is_known_array(key):
    #         # Do mapping magic here.
    #         # - Create several mars requests
    #         # - Do those
    #         # - Merge the results accordingly
    #         pass

    # def __setitem__(self, key, value):
    #     raise NotImplementedError("This method is not implemented")

    # def __delitem__(self, key):
    #     raise NotImplementedError("This method is not implemented")

    def keylist(self) -> list[Request]:
        listIterator = self.fdb.list(keys=True)
        return [RequestMapper.map_from_dict(it["keys"]) for it in listIterator]

    # def __iter__(self):
    #     yield from self.keylist()

    # def __len__(self):
    #     return len(self.keylist())

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
