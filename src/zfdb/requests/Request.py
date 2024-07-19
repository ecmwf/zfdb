import json
from logging import raiseExceptions
import re
from typing import Self


from zfdb.ZarrKeyMatcher import ZarrKeyMatcher

class Request:

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

    def __add__(self, other):
        if isinstance(other, str):
            self.postfix = other
        else:
            raise RuntimeError("__add__: Other operant isn't of type string")

    def __radd__(self, other):
        if isinstance(other, str):
            self.prefix = other
        else:
            raise RuntimeError("")

    def __str__(self) -> str:
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

    def full_request(self):
        if self.prefix is not None and len(self.prefix) > 0:
            result = dict(self.prefix[0].keys)

            for dicts in self.prefix:
                result = result | dicts.keys

            return Request(result)
        else:
            return Request(self.keys)



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
        return RequestMapper.map_from_str(json.dumps(dic))


    @staticmethod
    def map_from_str(str_repr: str):

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


        chunking_info = ZarrKeyMatcher.extract_chunking(str_repr)

        if chunking_info is not None:
            postfix = chunking_info

        return Request(keys=dicts_sanitized[-1], prefix=prefix, postfix=postfix)
