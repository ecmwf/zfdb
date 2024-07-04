from numpy import true_divide, who
from zarr.storage import Store
from zarr.types import DIMENSION_SEPARATOR

from typing import Optional, List

from pyeccodes import Reader

from zfdb.GribJumpRequestMerger import GribJumpRequestMerger
from zfdb.GribTools import inspect_grib_indices, scan_gribfile

import pyfdb
import json
import numpy as np

from zfdb.ZarrKeyMatcher import ZarrKeyMatcher
from zfdb.GribJumpRequestMerger import GribJumpRequestMerger

class FDBKey:
    def __init__(self, key) -> None:
        self._keys  = key["keys"]
        self._path  = key["path"]
        self._offset  = key["offset"]
        self._length  = key["length"]


class FDBStore(Store):
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
            prefix="", 
            dimension_separator: Optional[DIMENSION_SEPARATOR] = None, 
            **kwargs
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

        # TODO(TKR) Hardcoded for now
        self._path = "/Users/tkremer/.local/fdb5-space/"
    
    def __contains__(self, _key) -> bool:

        # Avoid initialization of the zarr store
        # We are implementing a view
        if _key == ".zgroup":
            return True

        if _key.endswith(".zgroup"):
            return False

        # Avoid initialization of the zarr array
        # We are implementing a view
        if _key ==".zarray":
            return False

        # Check for zarrar meta data avaialability
        if _key.endswith("/.zarray"):
            # Strip zarr specific suffix
            key = ZarrKeyMatcher.strip_metadatafile(_key)
            return self.gribjump_merger.existing(key)

        # Check for the actual data
        stripped_key = ZarrKeyMatcher.strip_chunking(_key, 4)
        if self.gribjump_merger.existing(stripped_key):
            return True
        else:
            raise KeyError()

    def _key(self, key):
        return f"{self._prefix}:{key}"

    def __getitem__(self, key):

        print("Current get-item request:", key)

        # Faking the zarr 2 file format
        if key == ".zgroup":
            return "{\"zarr_format\": 2}"

        if key.endswith("/.zarray"):
            key = ZarrKeyMatcher.strip_metadatafile(key)
            return self.gribjump_merger.zarr_metadata(key)

        mars_request = ZarrKeyMatcher.strip_chunking(key, 4)
        return self.gribjump_merger.request(mars_request)

    def __setitem__(self, key, value):
        raise NotImplementedError("This method is not implemented")

    def __delitem__(self, key):
        raise NotImplementedError("This method is not implemented")

    def keylist(self):
        listIterator = self.fdb.list(keys=True)
        return [it for it in listIterator]

    def __iter__(self):
        yield from self.keylist()

    def __len__(self):
        return len(self.keylist())

    def __getstate__(self):
        raise NotImplementedError("This method is not implemented")
        # return self._prefix, self._kwargs

    def __setstate__(self, state):
        prefix, kwargs = state
        self.__init__(prefix=prefix, **kwargs)

    def clear(self):
        raise NotImplementedError("This method is not implemented")
        # for key in self.keys():
        #     del self[key]

    def listdir(self, path: str = "") -> List[str]:
        # path = normalize_storage_path(path)
        # return _listdir_from_keys(self, path)
        listIterator = self.keylist()

        return [json.dumps(it["keys"]) for it in listIterator]

    def rmdir(self, path: str = "") -> None:
        raise NotImplementedError("This method is not implemented")
        # if not self.is_erasable():
        #     raise NotImplementedError(
        #         f'{type(self)} is not erasable, cannot call "rmdir"'
        #     )  # pragma: no cover
        # path = normalize_storage_path(path)
        # _rmdir_from_keys(self, path)

