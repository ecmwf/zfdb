from typing import override
from zarr.storage import BaseStore
from zarr.indexing import collections

import pyfdb.pyfdb as pyfdb


from collections.abc import MutableMapping 

fdb = pyfdb.FDB()

class FDBStoreV3(BaseStore):

    def __init__(self) -> None:
        self._readable = True
        self._writeable = False
        self._listable = True
        self._erasable = True

    @override
    def close(self) -> None:
        fdb.flush()

    @override
    def d
