<p align="center">
  <img src="https://img.shields.io/badge/ESEE-Foundation-orange" alt="ESEE Foundation">
  <a href="https://github.com/ecmwf/codex/blob/cookiecutter/Project%20Maturity/project-maturity.md">
    <img src="https://img.shields.io/badge/Maturity-Emerging-violet" alt="Maturity Emerging">
  </a>

<a href="https://github.com/ecmwf/zfdb/actions/workflows/ci.yaml">
    <img src="https://github.com/ecmwf/zfdb/actions/workflows/ci.yaml/badge.svg" alt="CI Status">
  </a>

<a href="https://codecov.io/gh/ecmwf/zfdb">
    <img src="https://codecov.io/gh/ecmwf/zfdb/branch/develop/graph/badge.svg" alt="Code Coverage">
  </a>

  <a href="https://opensource.org/licenses/apache-2-0">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0">
  </a>

  <a href="https://github.com/ecmwf/zfdb/releases">
    <img src="https://img.shields.io/github/v/release/ecmwf/zfdb?color=blue&label=Release&style=flat-square" alt="Latest Release">
  </a>
  <a href="https://zfdb.readthedocs.io/en/latest/?badge=latest">
    <img src="https://readthedocs.org/projects/zfdb/badge/?version=latest" alt="Documentation Status">
  </a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> *
  <a href="#installation">Installation</a> *
  <a href="#contributors">Contributors</a> *
  <a href="https://zfdb.readthedocs.io/en/latest/">Documentation</a>
</p>

# zfdb

A zarr store implementation using [FDB](https://github.com/ecmwf/fdb) as an back-end.


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


# Features

Access FDB data as if was an Anemoi Dataset.

# Quick Start


# Installation

Install from the Github repository directly:
```
python -m pip install https://github.com/ecmwf/zfdb
```

## License

© 2025 ECMWF. All rights reserved.
