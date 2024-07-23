import json
import re
from typing import Self

from copy import deepcopy


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

    def __init__(self, keys: dict[str, list[str]], *, prefix : list[Self] | None = None, postfix: str | None = None) -> None:
        self.keys = keys
        self.prefix = prefix
        self.postfix = postfix

    @classmethod
    def from_kw_args(cls, **kwargs) -> Self:
        keys = kwargs
        prefix = None
        postfix = None

        return cls(keys=keys, prefix=prefix, postfix=postfix)

    def remove_group_hierachy(self) -> Self:
        """
        Removes the prefix/group hierarchy information from a request. This is an __immutable__ operation
        """
        return Request(prefix=None, keys=deepcopy(self.keys), postfix=deepcopy(self.postfix))

    def remove_postfix(self) -> Self:
        """
        Removes the postfix from a request. This is an __immutable__ operation
        """
        return Request(prefix=deepcopy(self.prefix), keys=deepcopy(self.keys), postfix=None)

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
            result[key] = "/".join(raw_mars_request[key])

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


class RequestMapper:

    @staticmethod
    def __map_ranges(dic: dict[str, str]) -> dict[str, list[str]]:

        result = {}

        for key in dic.keys():
            values = dic[key]
            result[key] = values.split("/")
            
        return result

    @staticmethod
    def map_from_dict(dic: dict[str, str]):
        """
        Creates a Request from a given dict in raw_format via conversion to
        Json and calling the create method on its string representation
        """
        return RequestMapper.map_from_str(json.dumps(dic))

    @staticmethod
    def map_from_str(str_repr: str):

        ## Root group is empty string
        if str_repr == "":
            return Request(keys={})


        chunking_str =  r"({[^}]+\})+"
        occurrences = [x for x in re.finditer(chunking_str, str_repr)]

        # last occurence is the actual request within a group
        dicts =  [json.loads(occ[0]) for occ in occurrences]
        dicts_sanitized =  [RequestMapper.__map_ranges(x) for x in dicts]

        # mars_requst = json.loads(occurrences[-1][0])
        # mars_requst = RequestMapper.__make_values_set(mars_requst)
        #
        # # Merge dicts to one mars request for prefixing
        # prefix = {}
        #
        # for d in dicts:
        #     for new_key in d.keys():
        #         if new_key in prefix.keys():
        #             # TODO:(TKR) This is only working up to commutative params...
        #             if d[new_key] not in prefix[new_key]:
        #                 # In case of disambiguity 
        #                 raise RuntimeError("RequestMapper: Disambiguity in mapping groups. Duplicate keys in group definitions.")
        #
        #     prefix.update(d)
        #
        # # Normalize the dictionary by making all values to sets
        # prefix = RequestMapper.__make_values_set(prefix)
        #
        # Make the prefix a Request
        prefix = [Request(keys=x) for x in dicts_sanitized[:-1]]
        if len(prefix) == 0:
            prefix = None

        postfix = None

        if str_repr.endswith(".zarray") or \
            str_repr.endswith(".zgroup") or \
            str_repr.endswith("shape") or \
            str_repr.endswith("dtype"):
                possible_postfixes: list[str] = str_repr.split("}/")
                postfix = possible_postfixes[-1]


        from zfdb.ZarrKeyMatcher import ZarrKeyMatcher
        chunking_info = ZarrKeyMatcher.extract_chunking(str_repr)

        if chunking_info is not None:
            postfix = chunking_info

        return Request(keys=dicts_sanitized[-1], prefix=prefix, postfix=postfix)
