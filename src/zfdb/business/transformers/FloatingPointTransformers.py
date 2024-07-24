from zarr.creation import StorageTransformer

import numpy as np
import json

from zfdb.requests.Request import Request

class FloatingPoint8Transformer(StorageTransformer):

    valid_types = ["indexed"]
    extension_uri = ""

    def __init__(self) -> None:
        super().__init__("indexed")

    def rename(self, src_path: str, dst_path: str) -> None:
        return self.inner_store.rename(src_path, dst_path)

    def list_prefix(self, prefix):
        return self.inner_store.list_prefix(prefix)

    def erase_prefix(self, prefix):
        return self.inner_store.erase_prefix(prefix)

    def rmdir(self, path=None):
        return self.inner_store.rmdir(path)

    def __contains__(self, key):
        return self.inner_store.__contains__(key)

    def __setitem__(self, key, value):
        return self.inner_store.__setitem__(key, value)

    def __getitem__(self, key: Request):
        result = self.inner_store.__getitem__(json.dumps(key.build_mars_keys_span()))

        if isinstance(result, np.array):
           return result.astype(dtype=np.float8)

    def __delitem__(self, key):
        return self.inner_store.__delitem__(key)

    def __iter__(self):
        return self.inner_store.__iter__()

    def __len__(self):
        return self.inner_store.__len__()
