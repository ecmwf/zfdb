from zfdb.requests.Request import RequestMapper, Request


class TestUtils:
    @staticmethod
    def collapse_keys(dictionary: dict[str, list[str]]) -> dict[str, str]:
        result = {}

        for key in dictionary.keys():
            if isinstance(dictionary[key], list):
                result[key] = "/".join(dictionary[key])
            else:
                result[key] = dictionary[key]
        return result

    @staticmethod
    def build_example_zarray_with_prefix_and_postfix(postfix: str | None = ".zarray"):
        # given
        raw_prefix_ai = {
            "class": ["ai"],
        }

        raw_prefix_domain = {
            "class": ["domain"],
        }

        raw_prefix_list = [Request(keys=raw_prefix_ai), Request(keys=raw_prefix_domain)]

        raw_request = {
            "domain": ["g"],
            "expver": ["0001"],
            "stream": ["oper"],
            "time": ["0000"],
            "levtype": ["pl"],
            "type": ["fc"],
            "levelist": ["1000"],
            "param": ["129"],
            "step": ["0"],
        }

        if postfix is None or len(postfix) == 0:
            postfix = None

        return Request(prefix=raw_prefix_list, keys=raw_request, postfix=postfix)
