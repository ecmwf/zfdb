from zfdb import FDBStore

import zarr

def test_create_root_group():
    store = FDBStore()
    root = zarr.group(store=store, chunk_store=None)

    print(root.info)
