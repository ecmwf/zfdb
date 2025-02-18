# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import re
from collections.abc import MutableMapping

import numpy as np
import pyfdb
import pygribjump

from .datasources import FdbForecastDataSource, FdbSource, make_dates_source
from .error import ZfdbError
from .request import Request
from .zarr import FdbZarrArray, FdbZarrGroup


class FdbZarrMapping(MutableMapping):
    """Provide access to FDB with a MutableMapping.

    .. note:: This is an experimental feature.

    Requires the `pyFDB <https://redis-py.readthedocs.io/>`_
    package to be installed.

    Parameters
    ----------
    """

    def __init__(self, child: FdbZarrGroup | FdbZarrArray):
        self._child = child

        def build_paths(item, parent_path=None) -> list[str]:
            path = f"{parent_path}/{item.name}" if parent_path else item.name
            files = [f"{path}/{f}" if path != "" else f for f in item.paths()]
            if isinstance(item, FdbZarrGroup):
                for child in item.children:
                    files += build_paths(child, path)
            return files

        self._known_paths = build_paths(self._child)

    def __getitem__(self, key):
        keys = key.split("/")
        return self._child[*keys]

    def __iter__(self):
        yield from iter(self._known_paths)

    def __len__(self):
        return len(self._known_paths)

    def __setitem__(self, _k, _v):
        # TODO(kkratz): should raise proper exception
        pass

    def __delitem__(self, _k):
        # TODO(kkratz): should raise proper exception
        pass

    def __contains__(self, key) -> bool:
        return key in self._known_paths


def extract_mars_requests_from_recipe(recipe: dict):
    base_request = dict()
    # NOTE(kkratz): There is more available than just common.mars_request
    if "common" in recipe and "mars_request" in recipe["common"]:
        base_request = recipe["common"]["mars_request"]

    if "dates" not in recipe:
        raise ZfdbError("Expected 'dates' in recipe")

    start_date = recipe["dates"]["start"]
    end_date = recipe["dates"]["end"]

    # TODO(kkratz): There is code in anemoi.utils to convert the textual representation to a timedelta64
    # see: anemoi.utils.dates.as_timedelta(...)
    def parse_frequency(s: str):
        match = re.match(r"^(\d+)([d|h|m|s])$", s)
        if not match:
            raise ZfdbError(f"Invalid frequency str in recipe ('{s}')")
        return int(match.group(1)), match.group(2)

    frequency = parse_frequency(recipe["dates"]["frequency"])

    # recipes do support more than joins under input but for the sake of example we require
    # this to be present for now
    if "input" not in recipe or "join" not in recipe["input"]:
        raise ZfdbError("Expected 'input.join' in recipe")

    #  for now we only act on "mars" inputs, they need to be joined along the date-time axis
    inputs = recipe["input"]["join"]
    requests = [base_request | src["mars"] for src in inputs if "mars" in src]
    return start_date, end_date, frequency, requests


def make_forecast_data_view(
    *,
    fdb: pyfdb.FDB | None = None,
    gribjump: pygribjump.GribJump | None = None,
    request: Request | list[Request],
) -> FdbZarrMapping:
    requests = request if isinstance(request, list) else [request]

    if not fdb:
        fdb = pyfdb.FDB()
    if not gribjump:
        gribjump = pygribjump.GribJump()

    return FdbZarrMapping(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbForecastDataSource(fdb, gribjump, requests),
                ),
            ]
        )
    )


def make_anemoi_dataset_like_view(
    *,
    fdb: pyfdb.FDB | None = None,
    gribjump: pygribjump.GribJump | None = None,
    recipe: dict,
) -> FdbZarrMapping:
    # get common mars request part
    start_date, end_date, frequency, mars_requests = extract_mars_requests_from_recipe(
        recipe
    )
    if not fdb:
        fdb = pyfdb.FDB()
    if not gribjump:
        gribjump = pygribjump.GribJump()

    reference_mars_request = mars_requests[0]
    date, time = start_date.split("T")
    reference_mars_request["date"] = date
    reference_mars_request["time"] = time.split(":")[0]
    # lat_src, lon_src = make_lat_long_sources(reference_mars_request)
    return FdbZarrMapping(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="dates",
                    datasource=make_dates_source(
                        start=np.datetime64(start_date, "s"),
                        stop=np.datetime64(end_date, "s"),
                        interval=np.timedelta64(frequency[0], frequency[1]),
                    ),
                ),
                # FdbZarrArray(name="latitudes", datasource=lat_src),
                # FdbZarrArray(name="longitudes", datasource=lon_src),
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(
                        fdb,
                        gribjump,
                        mars_requests,
                        np.datetime64(start_date),
                        np.datetime64(end_date),
                        np.timedelta64(frequency[0], frequency[1]),
                    ),
                ),
            ]
        )
    )
