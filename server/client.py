#!/usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import asyncio
import json

import requests
import zarr

view = {
    "requests": [
        {
            "class": "od",
            "stream": "oper",
            "expver": 1,
            "type": "fc",
            "levtype": "sfc",
            "date": "2025-01-01",
            "time": "0000",
            "step": [0, 1, 2, 3, 4],
            "param": ["165", "166"],
            "domain": "g",
        },
        {
            "class": "od",
            "stream": "oper",
            "expver": 1,
            "type": "fc",
            "levtype": "pl",
            "levelist": ["50", "100"],
            "date": "2025-01-01",
            "time": "0000",
            "step": [0, 1, 2, 3, 4],
            "param": ["133", "130"],
            "domain": "g",
        },
    ]
}


async def main():
    url = "http://localhost:5000/create"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=json.dumps(view))

    print(response.content)
    hash = response.json()["hash"]
    z_grp = zarr.open_group(
        f"http://localhost:5000/get/zarr/{hash}",
        mode="r",
        zarr_format=3,
        use_consolidated=False,
    )
    session = await z_grp.store.fs.set_session()

    print(z_grp.get("data").shape)
    print(z_grp.get("data").chunks)
    for idx in range(5):
        print(idx)
        print(z_grp.get("data")[idx][0][0][0:10])

    await session.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
