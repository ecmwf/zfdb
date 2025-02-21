#!/usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import json

import fsspec
import requests
import zarr

view = {
    "requests": [
        {
            "cls": "od",
            "stream": "oper",
            "expver": 1,
            "typ": "fc",
            "levtype": "sfc",
            "date_time": "2025-01-01T00:00:00",
            "steps": [0, 1, 2, 3, 4],
            "params": ["165", "166"],
        },
        {
            "cls": "od",
            "stream": "oper",
            "expver": 1,
            "typ": "fc",
            "levtype": "pl",
            "level": ["50", "100"],
            "date_time": "2025-01-01T00:00:00",
            "steps": [0, 1, 2, 3, 4],
            "params": ["133", "130"],
        },
    ]
}

if __name__ == "__main__":
    url = "http://localhost:5000/create"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=json.dumps(view))

    print(response.content)
    hash = response.json()["hash"]
    store = fsspec.get_mapper(f"http://localhost:5000/get/zarr/{hash}")
    z_grp = zarr.open(store, mode="r")

    print(z_grp.attrs)

    for x in z_grp.attrs.items():
        print(x)

    print(z_grp["data"][0, 0, 0, 0:30])
