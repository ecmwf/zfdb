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
