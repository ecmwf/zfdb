from numpy import true_divide, who
from zarr.storage import Store
from zarr.types import DIMENSION_SEPARATOR

from typing import Optional, List

from pyeccodes import Reader

from zfdb.GribTools import inspect_grib_indices, scan_gribfile

import pygribjump
import pyfdb
import json

from zfdb.ZarrMetadataBuilder import ZarrMetadataBuilder

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

        self.gribjump = pygribjump.pygribjump.GribJump()
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
        
        keys = [el for el in map(lambda key: key["keys"], self.keylist())]

        if _key.endswith("/.zarray"):
            # Strip zarr specific suffix
            key = json.loads(_key.removesuffix("/.zarray"))
            return key in keys

        if json.loads(_key.rstrip("/0.0")) in keys:
            return True
        else:
            raise KeyError()

    def _key(self, key):
        return f"{self._prefix}:{key}"

    def __getitem__(self, key):

        # Faking the zarr 2 file format
        if key == ".zgroup":
            return "{\"zarr_format\": 2}"

        if hasattr(key, "keys"):
            return self.gribjump.extract( (key["keys"], [ (0, 10) ]))

        if key.endswith("/.zarray"):
            key = key.removesuffix("/.zarray")
            key = json.loads(key)


            # data_retriever =self.gribjump.extract( (key["keys"], [ (0, 10) ]))
            # msgs = scan_gribfile(data_retriever)
            # global_attrs, coords, var_info = inspect_grib_indices(msgs)

            # builder = ZarrMetadataBuilder()
            # builder.zarr_format(2)
            # builder.dtype(var_info["dtype"])
            # builder.shape(var_info["sh"])
            # json_tes = builder.build()
            #
            # return json_tes
                    # "compressor": { 
                    #     "blocksize": 0, 
                    #     "clevel": 5, 
                    #     "cname": "lz4", 
                    #     "id": "blosc", 
                    #     "shuffle": 1 
                    # }, 

            return r"""
                { 
                    "zarr_format": 2,
                    "dtype": "float64",
                    "shape": [10, 1],
                    "fill_value": "0",
                    "chunks": [10, 1],
                    "compressor": null,
                    "order": "C",
                    "filters": null
                }
                """
                # """.format(var_info[('t', 'isobaricInhPa')]["data_shape"][0],
                #            var_info[('t', 'isobaricInhPa')]["data_shape"][0])

        return self.gribjump.extract([(json.loads(key.rstrip("/1.0")), [(0, 10)])])[0][0][0][0]
        # return next(Reader(self.gribjump.retrieve(json.loads(key.rstrip("/0.0")))))._buffer



    def __setitem__(self, key, value):
        raise NotImplementedError("This method is not implemented")
        # value = ensure_bytes(value)
        # self.client[self._key(key)] = value

    def __delitem__(self, key):
        raise NotImplementedError("This method is not implemented")
        # count = self.client.delete(self._key(key))
        # if not count:
        #     raise KeyError(key)

    def keylist(self):
        listIterator = self.fdb.list(keys=True)

        return [it for it in listIterator]
        # raise NotImplementedError("This method is not implemented")
        # Should probably implement some kind of list() operation
        # offset = len(self._key(""))  # length of prefix
        # return [key[offset:].decode("utf-8") for key in self.client.keys(self._key("*"))]

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

