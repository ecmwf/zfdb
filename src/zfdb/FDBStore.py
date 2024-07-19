import json
from typing import List, Optional

import pyfdb
from zarr import hierarchy
from zarr.storage import Store
from zarr.types import DIMENSION_SEPARATOR

from zfdb.GribJumpRequestMerger import GribJumpRequestMerger
from zfdb.ZarrKeyMatcher import ZarrKeyMatcher
from zfdb.requests.Request import Request, RequestMapper


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

    def __contains__(self, _key) -> bool:
        if _key == ".zgroup":
            return True

        # Avoid initialization of the zarr array
        # We are implementing a view
        if _key == ".zarray":
            return False

        request:Request = RequestMapper.map_from_str(_key)
        
        if ZarrKeyMatcher.is_group(_key):
            mars_request = ZarrKeyMatcher.merge_group_information_into_mars_request(str(request))
            # TODO: Combine prefix to a under-specified mars request and check
            # whether it's in the axis object.

            if mars_request is None:
                return False

            # if self.gribjump_merger.is_full_specified_request(request.full_request()):
            #     return False
            # else:
            return True
            # return not self.gribjump_merger.is_full_specified_request(mars_request):

        if ZarrKeyMatcher.is_array(_key):
            mars_request = ZarrKeyMatcher.merge_group_information_into_mars_request(str(request))

            if mars_request is None:
                return False

            if self.gribjump_merger.is_full_specified_request(request.full_request()):
                return True
            else:
                return False

        if ZarrKeyMatcher.is_group_shape_information(_key):
            return False

        if ZarrKeyMatcher.has_chunking(_key):
            mars_request = ZarrKeyMatcher.strip_chunking(_key)
            return True
            # return self.gribjump_merger.request(mars_request)

        return False

    def _key(self, key):
        return f"{self._prefix}:{key}"

    def __getitem__(self, key):
        # Faking the zarr 2 file format
        if key == ".zgroup":
            return '{"zarr_format": 2}'

        if ZarrKeyMatcher.is_array(key):
            key = ZarrKeyMatcher.strip_metadata_remove_group_hiearchy(key)
            return self.gribjump_merger.zarr_metadata(key)

        if ZarrKeyMatcher.is_group(key):
            key = ZarrKeyMatcher.strip_metadata_remove_group_hiearchy(key)
            return '{"zarr_format": 2}'

        mars_request = ZarrKeyMatcher.strip_chunking_remove_group_hierarchy(key)
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
