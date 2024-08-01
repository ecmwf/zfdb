import json

import pytest
from tests.utils.Utils import TestUtils
from zfdb.business.Request import Request, RequestMapper


class TestRequest:

    def test_default_initialization_non_list_values(self):
        keys = {"some_key": "some_value"}

        with pytest.raises(RuntimeError) as runtimeError:
            Request(keys=keys)


    def test_default_initialization(self):
        keys = {"some_key": ["some_value"]}
        prefix = [Request(keys=keys)]
        postfix = "prefix"

        request = Request(keys=keys, prefix=prefix, postfix=postfix)

        assert request.keys == keys
        assert request.prefix == prefix
        assert request.postfix == postfix

    def test_remove_group_hierachy(self):
        request = TestUtils.build_example_zarray_with_prefix_and_postfix()

        request_without_group = request.remove_group_hierachy()

        assert request_without_group.prefix is None
        assert request_without_group.prefix != request.prefix

    def test_remove_postfix(self):
        request = TestUtils.build_example_zarray_with_prefix_and_postfix()

        request_without_postfix = request.remove_postfix()

        assert request_without_postfix.postfix is None
        assert request_without_postfix.postfix != request.postfix



class TestRequestMapper:
    def test_str_no_range(self):
        raw_request = {
            "class": "ai",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        str_request = '{"prefix_key":"prefix_value"}/'
        str_request += json.dumps(raw_request)
        str_request += "/postfix"

        request = RequestMapper.map_from_raw_input_dict(str_request)

        token = str(request).split("/")

        assert token[0] == '{"prefix_key": ["prefix_value"]}'
        assert len(request.keys) == len(raw_request)
        assert len(token) == 2  # There should be no postfix, see postfix tests below

    def test_str_group_request_postfix(self):
        raw_prefix_ai = {
            "class": "ai",
        }

        raw_prefix_domain = {
            "class": "domain",
        }

        raw_prefix_list = [raw_prefix_ai, raw_prefix_domain] 

        raw_request = {
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        str_request = json.dumps(raw_prefix_list) + "/"
        str_request += json.dumps(raw_request)
        str_request += "/.zarray"

        request = RequestMapper.map_from_raw_input_dict(str_request)


        assert len(request.prefix) == len(raw_prefix_list)

        assert len(request.prefix[0].keys.keys()) == len(raw_prefix_ai)
        assert len(request.prefix[1].keys.keys()) == len(raw_prefix_domain)

        assert len(request.keys) == len(raw_request)
        assert request.postfix == ".zarray"  # There should be no postfix, see postfix tests below

        for key in raw_prefix_ai.keys():
            if key == "class":
                assert request.prefix[0].keys[key][0] == raw_prefix_ai[key]
            elif key == "domain":
                assert request.prefix[1].keys[key][0] == raw_prefix_domain[key]
            else:
                assert request.keys[key][0] == raw_request[key]

    def test_initialization_from_dict_fully_specified_no_ranges(self):
        # Given
        raw_request = {
            "class": "ai",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        # When
        request = RequestMapper.map_from_dict(raw_request)

        # Then
        assert len(request.keys) == len(raw_request)

        for key in raw_request:
            values = request.keys[key]
            assert len(values) == 1

            assert raw_request[key] == values[0]

    def test_initialization_from_dict_fully_specified_with_ranges(self):
        # Given
        raw_request = {
            "class": "ai/od",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        # When
        request = RequestMapper.map_from_dict(raw_request)

        # Then
        assert len(request.keys) == len(raw_request)

        for key in raw_request:
            values = request.keys[key]
            assert len(values) == len(raw_request[key].split("/"))

            assert raw_request[key].split("/") == values

    zarr_postfixes = [".zarray", ".zgroup", "shape", "dtype"]

    @pytest.mark.parametrize("zarr_postfix", zarr_postfixes)
    def test_initialization_from_dict_zarr_speciality(self, zarr_postfix):
        # Given
        raw_request = {
            "class": "ai/od",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        str_request = ""
        str_request += json.dumps(raw_request) + "/"
        str_request += zarr_postfix

        # When
        request = RequestMapper.map_from_raw_input_dict(str_request)

        # Then
        assert len(request.keys) == len(raw_request)

        for key in raw_request:
            values = request.keys[key]
            assert len(values) == len(raw_request[key].split("/"))
            assert raw_request[key].split("/") == values

        assert request.prefix is None
        assert request.postfix == zarr_postfix


    chunking_info = ["0", "1", "0.0", "0.1", "1.1", "1.0", "0.0.0.0"]

    @pytest.mark.parametrize("chunking_info", chunking_info)
    def test_initialization_from_dict_zarr_chunking(self, chunking_info):
        # Given
        raw_request = {
            "class": "ai/od",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "1000",
            "param": "129",
            "step": "0",
        }

        str_request = ""
        str_request += json.dumps(raw_request) + "/"
        str_request += chunking_info

        # When
        request = RequestMapper.map_from_raw_input_dict(str_request)

        # Then
        assert len(request.keys) == len(raw_request)

        for key in raw_request:
            values = request.keys[key]
            assert len(values) == len(raw_request[key].split("/"))
            assert raw_request[key].split("/") == values

        assert request.prefix is None
        assert request.postfix == chunking_info
