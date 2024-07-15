import re
import json

class ZarrKeyMatcher:
    @staticmethod
    def strip_chunking(key: str) -> dict[str, str]:
        """
        Strips chunking information from a key and returns
        the key as a dictionary
        """
        chunking_str =  r'}/[^.][\d\.+]+'
        key_without_chunking = re.sub(chunking_str, "}", key)

        return json.loads(key_without_chunking)

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
    def has_chunking(key: str) -> bool:
        chunking_str =  r"}/[^.][\d\.]+"
        return re.search(chunking_str, key) is not None

    @staticmethod
    def is_array(key: str) -> bool:
        return key.endswith("/.zarray")

    @staticmethod
    def is_group(key: str) -> bool:
        return key.endswith("/.zgroup")
    
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
        return json.loads(key)

    @staticmethod
    def strip_metadata_remove_group_hiearchy(_key: str) -> dict[str, str]:
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
        chunking_str =  r"{([^}]+)}/?"
        occurrences = [x for x in re.finditer(chunking_str, key)]

        if len(occurrences) > 0 : 
            final_key = occurrences[-1][0]
        else:
            final_key = key

        return json.loads(final_key)


    @staticmethod
    def is_group_shape_information(key: str) -> bool:
        """
        """
        return key.endswith("/shape/.zarray") or key.endswith("/dtype/.zarray")
