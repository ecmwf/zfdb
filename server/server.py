from flask import Flask, Response, request, jsonify
import json
import zfdb

import pyfdb
import pygribjump

import numpy as np


app = Flask(__name__)

view_hashes = {}

fdb = pyfdb.FDB()
gribjump = pygribjump.GribJump()


def map_requests_from_json(json) -> list[zfdb.Request]:
    for r in json["requests"]:
        print(np.datetime64(r["date_time"]))
        r["date_time"] = np.datetime64(r["date_time"])

    print(f"{json}")

    return [zfdb.Request(**r) for r in json["requests"]]


def to_response(hased_request: int) -> Response:
    return_dict = {"hash": str(hased_request)}
    return Response(
        response=json.dumps(return_dict),
        status=200,
        content_type="application-type/json",
    )


@app.route("/")
def hello_world():
    return "Hello, World!"


@app.route("/create", methods=["POST"])
def process_json():
    print(f"Request: {request}")
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    # In case we have this view for the requests already
    hashed_request = hash(json.dumps(data))

    print(f"Computed Hash: {hashed_request}")
    if hashed_request in view_hashes:
        return to_response(hashed_request)

    requests = map_requests_from_json(data)

    mapping = zfdb.make_forecast_data_view(
        request=requests,
        fdb=fdb,
        gribjump=gribjump,
    )

    view_hashes[hashed_request] = mapping

    print("Retrieved the following requests:")
    for r in requests:
        print(r)

    return to_response(hashed_request)


@app.route("/get/zarr/<hash>", methods=["GET"])
@app.route("/get/zarr/<hash>/<root_grp>", methods=["GET"])
@app.route("/get/zarr/<hash>/<root_grp>/<group_lvl_one>", methods=["GET"])
@app.route(
    "/get/zarr/<hash>/<root_grp>/<group_lvl_one>/<group_lvl_two>", methods=["GET"]
)
def retrieve_zarr(hash, root_grp=None, group_lvl_one=None, group_lvl_two=None):
    print("Routes", hash, root_grp, group_lvl_one, group_lvl_two)
    print(view_hashes)

    zarr_path = "/".join(
        [frag for frag in [root_grp, group_lvl_one, group_lvl_two] if frag]
    )

    try:
        mapping = view_hashes[int(hash)]
    except KeyError as ke:
        return Response(response=f"Couldn't find hash in {hash}", status=500)

    print(f"Zarr path: {zarr_path}")

    try:
        content = mapping[zarr_path]
    except KeyError as ke:
        return Response(response="", status=404)

    return Response(response=content, status=200)


if __name__ == "__main__":
    app.run(debug=True)
