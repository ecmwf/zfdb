import zarr
import zarr.storage


def zarr_groups(group: zarr.Group):
    """
    Zarr does not provide and out of the box way to travers all groups in a
    tree of groups as they do with arrays hence we need to provide one.
    """
    for child in group.groups():
        yield child
        yield from zarr_groups(child[1])


def compare_zarr_stores(
    store_a: zarr.storage.Store, store_b: zarr.storage.BaseStore
) -> bool:
    """
    Takes two zar stores and compares all groups/arrays in the store.
    Two gropups are identical if tehir path is identical
    Two arrays are identical if:
        - their path match
        - hash of the uncompressed data match
        - .zattrs and .zarray match
    """
    a = zarr.open(store_a)
    b = zarr.open(store_b)
    if isinstance(a, zarr.Array) and isinstance(b, zarr.Array):
        return a.hexdigest() == b.hexdigest()
    if isinstance(a, zarr.Group) and isinstance(b, zarr.Group):
        return all(
            array_a[0] == array_b[0]
            and array_a[1].hexdigest() == array_b[1].hexdigest()
            for (array_a, array_b) in zip(
                a.arrays(recurse=True), b.arrays(recurse=True), strict=True
            )
        ) and all(
            group_a[0] == group_b[0]
            for (group_a, group_b) in zip(zarr_groups(a), zarr_groups(b), strict=True)
        )
    return False
