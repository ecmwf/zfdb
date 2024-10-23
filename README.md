# zfdb

A zarr store implementation using [FDB](https://github.com/ecmwf/fdb) as an back-end.

This project is ALPHA and will be experimental for the foreseeable future. 
Interfaces and functionality are likely to change.

## How to run tests

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
