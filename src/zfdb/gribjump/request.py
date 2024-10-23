import json


class MarsRequest:
    def __init__(self, keys: dict[str, str]) -> None:
        self.keys = keys


class SanitizedMarsRequest:
    def __init__(self, keys: dict[str, list[str]]) -> None:
        self.keys = keys

    def __str__(self) -> str:
        return json.dumps(self.keys)


class GribJumpRequestMapper:
    @staticmethod
    def __sanitize(dic: dict[str, str]) -> dict[str, list[str]]:
        result = {}

        for key in dic.keys():
            values = dic[key]
            if isinstance(values, list):
                result[key] = values
            elif isinstance(values, str):
                if "/" in values:
                    result[key] = values.split("/")
                else:
                    result[key] = [values]
            else:
                raise RuntimeError(f"Can't sanitize {dic}")

        return result

    @staticmethod
    def map_from_dict(dic: dict[str, str]) -> SanitizedMarsRequest:
        """
        Creates a Request from a given dict in raw_format via conversion to
        Json and calling the create method on its string representation
        """
        dict_sanitized = GribJumpRequestMapper.__sanitize(dic)
        return SanitizedMarsRequest(dict_sanitized)

    @staticmethod
    def map_from_str(str_repr: str) -> SanitizedMarsRequest:
        ## Root group is empty string
        if str_repr == "":
            return SanitizedMarsRequest(keys={})

        # Check whether this is a mars language str
        if "=" in str_repr:
            token = str_repr.split(",")

            if token[0] != "retrieve":
                raise RuntimeError("Can only handle retreive request")

            token = token[1:]

            mars_keys = {}

            for t in token:
                k, v = t.split("=")
                k = k.strip()
                v = v.strip()
                mars_keys[k] = v

            return SanitizedMarsRequest(GribJumpRequestMapper.__sanitize(mars_keys))
        else:
            mars_keys = json.loads(str_repr)
            return SanitizedMarsRequest(GribJumpRequestMapper.__sanitize(mars_keys))
