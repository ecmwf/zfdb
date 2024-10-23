import json
import re
from typing import Self

from copy import deepcopy


class MarsRequest:
    def __init__(self, keys: str) -> None:
        self.keys = keys


class Request:
    """
    Class for internally representing a mars request. There is some logic to it:
    In Zarr directories are groups and individual files can be arrays. To
    cope with this concept, and make virtual groups in FDB possible, there is
    can be sets of groups, separated by '/', which will be interpreted as a group
    hierarchy. This parts becomes the prefix of a given request. Keys of a request
    can be missing keys, which are needed to fully specify a request. The postfix
    can have multiple Zarr-specific values.

    ---------------------------------------
    Example:

        {"key1": "value1"}/{"key2": "value2"}/{"key3: "value3"...}/.zarray
    ------------- prefix --------------------|------ keys --------| postfix

    There are several method for conversion to internal dictionaries with lists
    of values or even dicts which are in the original format of the FDB requests.

    IMPORTANT: Use the @RequstMapper class to create a new Request
    """

    def __init__(
        self,
        keys: dict[str, list[str]],
        *,
        prefix: list[Self] | None = None,
        postfix: str | None = None,
    ) -> None:
        for key in keys.keys():
            if not isinstance(keys[key], list):
                raise RuntimeError(
                    f"Request needs to be build from key-value-pairs, where value is an instance of list. Found: {keys[key]}"
                )

        self.keys = keys
        self.prefix = prefix
        self.postfix = postfix

    def remove_group_hierachy(self) -> Self:
        """
        Removes the prefix/group hierarchy information from a request. This is an __immutable__ operation
        """
        return Request(
            prefix=None, keys=deepcopy(self.keys), postfix=deepcopy(self.postfix)
        )

    def remove_postfix(self) -> Self:
        """
        Removes the postfix from a request. This is an __immutable__ operation
        """
        return Request(
            prefix=deepcopy(self.prefix), keys=deepcopy(self.keys), postfix=None
        )

    def build_mars_request(self) -> dict[str, list[str]]:
        """
        Creates a mars request (dict with mars keys), in which
        each key can has a list of values. This is a processed version of
        a span in the actual mars language. Each span is converted into a list.
        """
        if self.prefix is None:
            return self.keys

        resulting_dict = {}

        for req in self.prefix:
            resulting_dict |= req.keys

        resulting_dict |= self.keys

        return resulting_dict

    def build_mars_keys_span(self) -> dict[str, str]:
        """
        Creates a mars request (dict with mars keys), in which each
        key can has a single value and in case of a span the values are separated
        by '/'.
        """
        raw_mars_request = self.build_mars_request()

        result = {}

        for key in raw_mars_request:
            if isinstance(raw_mars_request[key], list):
                result[key] = "/".join(raw_mars_request[key])
            else:
                result[key] = raw_mars_request[key]

        return result

    def __str__(self) -> str:
        """
        String representation for the request. The prefix will be '/' separated.
        The keys itself are in a valid Json representation. Postfix is a string.
        """
        result = ""

        if self.prefix is not None and len(self.prefix) != 0:
            for request in self.prefix:
                result += str(request)
                result += "/"

        result += json.dumps(self.keys)

        if self.postfix is not None:
            result += "/"
            result += self.postfix

        return result

    def is_fully_specified(self):
        full_request = self.build_mars_request()
        mars_keys = full_request.keys()

        if len(mars_keys) < 11:
            return False

        for key in mars_keys:
            if key in ["date", "step", "param", "levelist"]:
                continue
            values = full_request[key]
            if isinstance(values, list) and len(values) != 1:
                return False

        return True


class RequestMapper:
    @staticmethod
    def __sanitize(dic: dict[str, str]) -> dict[str, list[str]]:
        result = {}

        for key in dic.keys():
            values = dic[key]
            # If values is a list, convert it to string
            if isinstance(values, list):
                result[key] = [str(v) for v in values]
            # If values is a string, split it on / or warp it in a list
            elif isinstance(values, str):
                if "/" in values:
                    result[key] = values.split("/")
                else:
                    result[key] = [values]
            else:
                raise RuntimeError(f"Can't sanitize {dic}")

        return result

    @staticmethod
    def map_from_dict(dic: dict[str, str]):
        """
        Creates a Request from a given dict in raw_format via conversion to
        Json and calling the create method on its string representation
        """
        dict_sanitized = RequestMapper.__sanitize(dic)
        return RequestMapper.map_from_raw_input_dict(json.dumps(dict_sanitized))

    @staticmethod
    def map_from_raw_input_dict(str_repr: str) -> Request:
        # Python converting dictionary string to single quotes, but json relying
        # on double quotes for serialisation
        str_repr = str_repr.replace("'", '"')

        ## Root group is empty string
        if str_repr == "":
            return Request(keys={})

        # Find all occurrences of dictionarys within a possible str_repr
        chunking_str = r"({[^}]+\})+"
        occurrences = [x for x in re.finditer(chunking_str, str_repr)]

        # last occurrence is the actual request within a
        dicts = [json.loads(occ[0]) for occ in occurrences]
        dict_sanitized = [RequestMapper.__sanitize(dic) for dic in dicts]

        # Make the prefix a Request
        prefix = [Request(keys=x) for x in dict_sanitized[:-1]]
        if len(prefix) == 0:
            prefix = None

        postfix = None

        if (
            str_repr.endswith(".zarray")
            or str_repr.endswith(".zgroup")
            or str_repr.endswith("shape")
            or str_repr.endswith("dtype")
        ):
            possible_postfixes: list[str] = str_repr.split("}/")
            postfix = possible_postfixes[-1]

        from zfdb.business.ZarrKeyMatcher import ZarrKeyMatcher

        chunking_info = ZarrKeyMatcher.extract_chunking(str_repr)

        if chunking_info is not None:
            postfix = chunking_info

        return Request(keys=dict_sanitized[-1], prefix=prefix, postfix=postfix)
