import os
import pathlib
import shutil

import ecmwfapi
import pyfdb
import pytest
import yaml


@pytest.fixture(scope="session", autouse=False)
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


@pytest.fixture(scope="session", autouse=False)
def download_anemoi_compare_data_from_mars(request) -> None:
    """
    Downloads test data into <source-root>/tests/data if 'test_data.grib does
    not yet exist'. This is a workaround to prevent the need to commit multiple
    GB of test data.
    """
    root_path = (
        request.config.rootpath / "tests" / "data" / "integration" / "mars_requests"
    )
    destination_path = request.config.rootpath / "tests" / "data"

    request_files = [
        root_path / x for x in os.listdir(root_path) if os.path.isfile(root_path / x)
    ]
    print("Request files: ", *request_files)

    if set(["1.grib", "2.grib", "3.grib", "4.grib", "5.grib"]) <= set(
        os.listdir(destination_path)
    ):
        return

    for request_file in request_files:
        c = ecmwfapi.ECMWFService("mars")

        with open(request_file, "r") as current_request:
            c.execute(
                current_request.read(),
                destination_path / f"{pathlib.Path(request_file).stem}.grib",
            )


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


@pytest.fixture(scope="session", autouse=False)
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
        print(f"Archiving {f} into FDB at {tmp_path}")
        fdb.archive(f.read_bytes())
    return tmp_path
