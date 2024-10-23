import os
import pathlib
import shutil

import yaml
import pytest
import pyfdb


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
