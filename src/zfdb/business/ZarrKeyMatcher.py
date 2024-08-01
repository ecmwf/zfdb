from copy import deepcopy
import re
import json

from zfdb.business.Request import Request

class ZarrKeyMatcher:
    @staticmethod
    def strip_chunking(key: Request) -> Request:
        """
        Strips chunking information from a key and returns
        the key as a dictionary
        """
        if ZarrKeyMatcher.has_chunking(key):
            return Request(prefix=deepcopy(key.prefix), keys=deepcopy(key.keys), postfix=None)
        else:
            return key

    @staticmethod
    def extract_chunking(key: str) -> list[str] | None:
        """
        Strips chunking information from a key and returns
        the key as a dictionary
        """
        chunking_str =  r'(?:}/)(\d+(\.\d+)*)'
        result_list = re.findall(chunking_str, key)


        # TODO: (TKR) Make this nicer
        if len(result_list) > 0:
            if isinstance(result_list[0], tuple) and len(result_list[0]) > 1:
                return result_list[0][0]
            else:
                return result_list[0]
        else:
            return None


    @staticmethod
    def strip_chunking_remove_group_hierarchy(key: str) -> dict[str, str]:
        """
        Strips chunking information from a key and returns
        the key as a dictionary
        """
        chunking_str =  r'}/[^.][\d\.+]+'
        key_without_chunking = re.sub(chunking_str, "}", key)

    
        return ZarrKeyMatcher.remove_group_hierachy(key_without_chunking)


    @staticmethod
    def merge_group_information_into_mars_request(key: str) -> dict[str, str] | None:
        """
        """
        chunking_str =  r"({[^}]+\})+"
        occurrences = [x for x in re.finditer(chunking_str, key)]

        dicts =  [json.loads(occ[0]) for occ in occurrences]

        # Merge dicts to one mars request
        result = {}

        for d in dicts:
            for new_key in d.keys():
                if new_key in result.keys():
                    # TODO:(TKR) This is only working up to commutative params...
                    # TODO(TKR) Come up with a nice solution for subset testing
                    if not set(d[new_key]).issubset(result[new_key]):
                        # In case of disambiguity 
                        return None

            result.update(d)

        return result

    @staticmethod
    def has_chunking(key: Request) -> bool:
        if key.postfix is None:
            return False

        chunking_str =  r'(?:/)(\d+(\.\d+)*)'
        return re.search(chunking_str, key.postfix) is not None

    @staticmethod
    def is_array(key: Request) -> bool:
        if key.postfix is None:
            return False
        return key.postfix.endswith(".zarray")

    @staticmethod
    def is_group(key: Request) -> bool:
        if key.postfix is None:
            return False
        return key.postfix.endswith(".zgroup")

    
    @staticmethod
    def strip_metadatafile(key: str) -> dict[str, str]:
        """
        Strips metadata information from a key and returns
        the key as a dictionary
        """
        return json.loads(key.removesuffix("/.zarray"))

    @staticmethod
    def strip_metadata(_key: str) -> dict[str, str]:
        """
        Strips metadata information from a key and returns
        the key as a dictionary
        """
        key = _key.removesuffix("/.zarray")
        key = key.removesuffix("/.zgroup")
        # Nicely done Zarr...
        key = key.removesuffix("/shape")
        key = key.removesuffix("/dtype")

        return json.loads(key)

    @staticmethod
    def strip_metadata_remove_group_hierarchy(_key: str) -> dict[str, str]:
        """
        Strips metadata information from a key and returns
        the key as a dictionary
        """
        key = _key.removesuffix("/.zarray")
        key = key.removesuffix("/.zgroup")
        # Nicely done Zarr...
        key = key.removesuffix("/shape")
        key = key.removesuffix("/dtype")
        
        return ZarrKeyMatcher.remove_group_hierachy(key)

    @staticmethod
    def remove_group_hierachy(key: str) -> dict[str, str]:
        """
        """

        # Doesn't match zarr post fixed like
        chunking_str =  r"{([^}]+)}/?"
        occurrences = [x for x in re.finditer(chunking_str, key)]

        if len(occurrences) > 0 : 
            raw_request_str = occurrences[-1][0]
        else:
            final_key = key

        pos_begin_substr = key.find(raw_request_str)

        # 
        return json.loads(key[pos_begin_substr:])


    @staticmethod
    def is_group_shape_information(request: Request) -> bool:
        """
        """
        if request.postfix is None:
            return False

        return request.postfix.endswith("/shape/.zarray") or request.postfix.endswith("/dtype/.zarray")
