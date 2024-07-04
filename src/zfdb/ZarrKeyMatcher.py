import re
import json

class ZarrKeyMatcher:
    @staticmethod
    def strip_chunking(key: str, dimensions: int):
        """
        Strips chunking information from a key and returns
        the key as a dictionary
        """
        chunking_delim = r"\.+"
        chunking_str =  '}/[' + chunking_delim.join([r"\d+"] * dimensions) + "]+"
        key_without_chunking = re.sub(chunking_str, "}", key)

        return json.loads(key_without_chunking)
    
    @staticmethod
    def strip_metadatafile(key: str):
        """
        Strips metadata information from a key and returns
        the key as a dictionary
        """
        return json.loads(key.removesuffix("/.zarray"))

