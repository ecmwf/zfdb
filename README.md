<p align="center">
  <a href="https://github.com/ecmwf/codex/tree/main/Project%20Maturity#sandbox">
    <img src="https://img.shields.io/badge/Maturity-Sandbox-yellow" alt="Maturity Sandbox">
  </a>
  <a href="https://opensource.org/licenses/apache-2-0">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0">
  </a>
</p>

# ZarrFDB

A zarr store implementation using [FDB](https://github.com/ecmwf/fdb) as an back-end.

This project is currently an experiment.

## Usage

> [!IMPORTANT]  
>
> In belows example the view into data in FDB is described by a dictionary of
> mars keywords. It is important to know that you need to fully specify your
> request otherwise no data will be returned. 
>
> How the resulting zarr array is chunked depends on the `chunk_axis`, this can
> be on the date-time or on the step axis. Currently chunking can only be
> enabled on one axis and always provides one chunk per value on that axis.
> I.e. you cannot group multiple steps into one zarr chunk.

Example:

```python
import numpy as np
import zarr

from zfdb import (
    ChunkAxisType,
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup
    FdbZarrMapping,
    Request,
)

mapping = FdbZarrMapping(
    FdbZarrGroup(
        children=[
            FdbZarrArray(
                name="data",
                datasource=FdbSource(
                    request=[
                        Request(
                            request={
                                "date": np.arange(
                                    np.datetime64("2020-01-01"),
                                    np.datetime64("2020-01-03"),
                                ),
                                "time": ["00", "06", "12", "18"],
                                "class": "ea",
                                "domain": "g",
                                "expver": "0001",
                                "stream": "oper",
                                "type": "an",
                                "step": "0",
                                "levtype": "sfc",
                                "param": ["10u", "10v"],
                            },
                            chunk_axis=ChunkAxisType.DateTime,
                        )
                    ]
                ),
            )
        ]
    )
)
store = zarr.open_group(mapping, mode="r")
```

## How to run tests

### Downloading testdata

The tests will use the MARS web api to download approx. 1.5GB of testdata and store this under
`tests/data/testdata.grib`. The download will be skipped if this file is
present. To be able to download data you will need to have an api-key placed in
`~/.ecmwfapirc`. You can get your api key from <https://api.ecmwf.int/v1/key/>

### Executing the tests

To run the tests `libgribjump` and `libfdb5` need to findable via `FDB5_DIR`
and `GRIBJUMP_DIR` respectively. If you have installed `gribjump` and
dependencies via `ecbundle`, both variables should point to the install
directory.

Calling `pip install -e .` from the source root will create an editable install 
of zfdb into your site-packages and install all dependencies from pyproject.toml

Run the tests with `pytest -vv -s` or alternatively use `pytest -vv -s --basetemp=tmp`
to createa folder `tmp` in the current working directory and use this as
location for all temporary files used in the tests, e.g. the pyfdb storage
location.

All together:
```
export FDB5_DIR=<path to fdb installation path>
export GRIBJUMP_DIR=<path to gribjump installation path>
pip install -e .
pytest -vv -s
```

## License

See [LICENSE](LICENSE)
