import json
import pathlib
import requests
import zarr
import fsspec

if __name__ == "__main__":
    json_request = None
    with open(pathlib.Path("./test.json"), "r") as request_file:
        json_request = json.loads(request_file.read())

    url = "http://localhost:5000/create"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=json.dumps(json_request))

    print(response.content)
    hash =  response.json()["hash"]
    store = fsspec.get_mapper(f'http://localhost:5000/get/zarr/{hash}')
    z_grp = zarr.open(store, mode="r")

    print(z_grp.attrs)

    for x in z_grp.attrs.items():
        print(x)

    print(z_grp["data"][0,0,0,0:30])
