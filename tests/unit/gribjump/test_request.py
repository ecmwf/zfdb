from zfdb.gribjump.request import GribJumpRequestMapper

class TestRequest:
    def test_default_initialization_from_dict(self):
        dict_request = {
            "class": "ai",
            "date": "20240601/20240602/20240603",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "fc",
            "levelist": "850",
            "param": "129",
            "step": "0",
        }

        request = GribJumpRequestMapper.map_from_dict(dict_request)

        assert request.keys["class"] == ["ai"]
        assert request.keys["date"] == ["20240601","20240602","20240603"]
        assert request.keys["domain"] == ["g"]
        assert request.keys["expver"] == ["0001"]
        assert request.keys["stream"] == ["oper"]
        assert request.keys["time"] == ["0000"]
        assert request.keys["levtype"] == ["pl"]
        assert request.keys["type"] == ["fc"]
        assert request.keys["levelist"] == ["850"]
        assert request.keys["param"] == ["129"]
        assert request.keys["step"] == ["0"]


    def test_default_initialization_from_string_dict(self):
        request_str = '{"class": "ai", \
                         "date": "20240601/20240602/20240603", \
                         "domain": "g", \
                         "expver": "0001", \
                         "stream": "oper", \
                         "time": "0000", \
                         "levtype": "pl", \
                         "type": "fc", \
                         "levelist": "850", \
                         "param": "129", \
                         "step": "0"}'

        request = GribJumpRequestMapper.map_from_str(request_str)

        assert request.keys["class"] == ["ai"]
        assert request.keys["date"] == ["20240601","20240602","20240603"]
        assert request.keys["domain"] == ["g"]
        assert request.keys["expver"] == ["0001"]
        assert request.keys["stream"] == ["oper"]
        assert request.keys["time"] == ["0000"]
        assert request.keys["levtype"] == ["pl"]
        assert request.keys["type"] == ["fc"]
        assert request.keys["levelist"] == ["850"]
        assert request.keys["param"] == ["129"]
        assert request.keys["step"] == ["0"]

    def test_default_initialization_from_string_mars(self):
        request_str = "retrieve, \
                        class=ai, \
                        date=20240601/20240602, \
                        domain=g, \
                        expver=0001, \
                        stream=oper, \
                        time=0000, \
                        levtype=pl, \
                        type=fc, \
                        levelist=1000, \
                        param=129, \
                        step=0"

        request = GribJumpRequestMapper.map_from_str(request_str)

        assert request.keys["class"] == ["ai"]
        assert request.keys["date"] == ["20240601","20240602"]
        assert request.keys["domain"] == ["g"]
        assert request.keys["expver"] == ["0001"]
        assert request.keys["stream"] == ["oper"]
        assert request.keys["time"] == ["0000"]
        assert request.keys["levtype"] == ["pl"]
        assert request.keys["type"] == ["fc"]
        assert request.keys["levelist"] == ["1000"]
        assert request.keys["param"] == ["129"]
        assert request.keys["step"] == ["0"]
