import json

class MarsRequest:

    def __init__(self, mars_requst_str: str) -> None:
        mars_request: dict = json.loads(mars_requst_str)

        for key in mars_request.keys():
            self.__setattr__(key, key)
