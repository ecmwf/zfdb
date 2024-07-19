import json
import pytest


from tests.utils.Utils import TestUtils

from random import randint

from tests.utils.Utils import TestUtils
from zfdb.ZarrKeyMatcher import ZarrKeyMatcher


class TestZarrMatcher:
    def test_chunking_stripping(self):
        # Given
        keys = {"key": "value"}

        postfix = ".".join([str(randint(0, 10000)) for _ in range(randint(0, 100))])

        # When
        result = ZarrKeyMatcher.strip_chunking(json.dumps(keys) + "/" + postfix)

        # Then
        assert result == keys

    def test_chunking_extracting(self):
        # Given
        keys = {"key": "value"}

        postfix = ".".join([str(randint(0, 10000)) for _ in range(randint(0, 100))])

        # When
        result = ZarrKeyMatcher.extract_chunking(json.dumps(keys) + "/" + postfix)

        # Then
        assert result == postfix

    def test_strip_chunking_remove_group_hierarchy(self):
        # given
        request = TestUtils.build_example_zarray_with_prefix_and_postfix()

        str_request = str(request)

        # when
        resulting_dict = ZarrKeyMatcher.strip_metadata_remove_group_hierarchy(
            str_request
        )

        # then
        assert resulting_dict == request.keys

    postfixes = [".zarray", ".zgroup", "shape", "dtype"]

    @pytest.mark.parametrize("postfix_str", postfixes)
    def test_strip_metadata(self, postfix_str):
        # given
        request = TestUtils.build_example_zarray_with_prefix_and_postfix(postfix=postfix_str)

        str_request = str(request)

        # when
        resulting_dict = ZarrKeyMatcher.strip_metadata_remove_group_hierarchy(
            str_request
        )

        # then
        assert resulting_dict == request.keys

    def test_remove_group_hierachy(self):

        # given
        request = TestUtils.build_example_zarray_with_prefix_and_postfix(postfix=None)

        str_request = str(request)
        
        # When
        resulting_dict = ZarrKeyMatcher.remove_group_hierachy(str_request)

        # Then
        assert resulting_dict == request.keys

    def test_remove_group_hierachy_with_postfix(self):

        # given
        request = TestUtils.build_example_zarray_with_prefix_and_postfix(postfix=".zarray")

        str_request = str(request)
        
        # When
        resulting_dict = ZarrKeyMatcher.remove_group_hierachy(str_request)

        # Then
        assert resulting_dict == request.keys

