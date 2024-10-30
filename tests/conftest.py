import os
import pathlib
import shutil

import ecmwfapi
import pyfdb
import pytest
import yaml


@pytest.fixture(scope="session", autouse=True)
def download_testdata_from_mars(request) -> None:
    """
    Downloads test data into <source-root>/tests/data if 'test_data.grib does
    not yet exist'. This is a workaround to prevent the need to commit multiple
    GB of test data.
    """
    target_path = request.config.rootpath / "tests" / "data" / "testdata.grib"
    if target_path.exists():
        return
    mars_request = """
        retrieve,
            class=ai,
            date=20240601/20240602/20240603/20240604/20240605/20240606,
            domain=g,
            expver=0001,
            stream=oper,
            time=0000,
            levtype=pl,
            step=0/to/120/by/12,
            param=129/130/131/132,
            type=fc
    """
    c = ecmwfapi.ECMWFService("mars")
    c.execute(mars_request, str(target_path))


@pytest.fixture(scope="session", autouse=True)
def gribjump_env() -> None:
    """
    Sets default environment variables that are not dependent on individual testsetup.
    """
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"


@pytest.fixture(scope="session")
def data_path(request) -> pathlib.Path:
    """
    Provides path to test data
    """
    path = request.config.rootpath / "tests" / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="session", autouse=True)
def read_only_fdb_setup(data_path, tmp_path_factory) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """
    tmp_path = tmp_path_factory.mktemp("fdb_loc")
    db_store_path = tmp_path / "db_store"
    db_store_path.mkdir()
    schema_path = tmp_path / "schema"
    config = dict(
        type="local",
        engine="toc",
        schema=str(schema_path),
        spaces=[
            dict(
                handler="Default",
                roots=[
                    {"path": str(db_store_path)},
                ],
            )
        ],
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))
    shutil.copy(data_path / "schema", schema_path)
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)

    grib_files = data_path.glob("*.grib")
    fdb = pyfdb.FDB()
    for f in grib_files:
        fdb.archive(f.read_bytes())
    return tmp_path
