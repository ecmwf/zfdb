import json
import yaml
from pathlib import Path


class MarsRequest(dict):
    def __str__(self) -> str:
        return json.dumps(self.keys)


class MarsRequestBuilder:

    def __init__(self, path_to_requests: Path) -> None:
        if isinstance(path_to_requests, str):
            path_to_requests = Path(path_to_requests)
        self.path_to_requests = path_to_requests


    def build_requests(self):
        req_files = self.path_to_requests.glob("*.req")

        result_list = []

        for req_file in req_files:
            with open(self.path_to_requests / req_file, "r") as request:
                mars_request =  self.parse_req_into_mars_keys(request.readlines())
                result_list.append(mars_request)

        return result_list

    def parse_req_into_mars_keys(self, request_lines: list[str]):

        mars_keys = MarsRequest()
        # drop the first line (verb)
        request_lines = request_lines[1:]

        for line in request_lines:
            # Remove whitespaces
            if line.strip() == "":
                continue

            # to lower case
            line = line.lower()

            [key, value] =  line.split("=")
            mars_keys[key.strip()] = value.strip().rstrip(",")

        return mars_keys
            
