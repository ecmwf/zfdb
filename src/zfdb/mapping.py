import re
from collections.abc import MutableMapping

import numpy as np
import pyfdb
import pygribjump

from .datasources import FdbSource, make_dates_source, make_lat_long_sources
from .error import ZfdbError
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


def extract_mars_requests_from_recepie(recepie: dict):
    base_request = dict()
    # NOTE(kkratz): There is more available than just common.mars_request
    if "common" in recepie and "mars_request" in recepie["common"]:
        base_request = recepie["common"]["mars_request"]

    if "dates" not in recepie:
        raise ZfdbError("Expected 'dates' in recepie")

    start_date = recepie["dates"]["start"]
    end_date = recepie["dates"]["end"]

    # TODO(kkratz): There is code in anemoi.utils to convert the textual representation to a timedelta64
    # see: anemoi.utils.dates.as_timedelta(...)
    def parse_frequency(s: str):
        match = re.match(r"^(\d+)([d|h|m|s])$", s)
        if not match:
            raise ZfdbError(f"Invalid frequency str in recipe ('{s}')")
        return int(match.group(1)), match.group(2)

    frequency = parse_frequency(recepie["dates"]["frequency"])

    # recepies do support more than joins under input but for the sake of example we require
    # this to be present for now
    if "input" not in recepie or "join" not in recepie["input"]:
        raise ZfdbError("Expected 'input.join' in recepie")

    #  for now we only act on "mars" inputs, they need to be joined along the date-time axis
    inputs = recepie["input"]["join"]
    requests = [base_request | src["mars"] for src in inputs if "mars" in src]
    return start_date, end_date, frequency, requests


def build_fdb_store_from_recepie(
    *, fdb_config: str | dict | None = None, recepie: dict
) -> FdbZarrMapping:
    fdb = pyfdb.FDB(config=fdb_config)
    gribjump = pygribjump.GribJump()
    # get common mars request part
    start_date, end_date, frequency, mars_requests = extract_mars_requests_from_recepie(
        recepie
    )

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
