import numpy as np
from numpy._core.multiarray import dtype

from zfdb.business.FDBStore import FDBStore
from zfdb.business.Request import Request

class FloatingPoint16Transformer(FDBStore):

    def __init__(self, inner_store) -> None:
        self.inner_store = inner_store

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

    def __getitem__(self, key):
        result = self.inner_store.__getitem__(key)

        if isinstance(result, str):
            return result
        elif isinstance(result, np.ndarray):
           return result.astype(dtype=np.float16).astype(dtype=np.float64)
        else:
            return result

    def keylist(self) -> list[Request]:
        return self.inner_store.keylist()

    def __delitem__(self, key):
        return self.inner_store.__delitem__(key)

    def __iter__(self):
        return self.inner_store.__iter__()

    def __len__(self):
        return self.inner_store.__len__()
