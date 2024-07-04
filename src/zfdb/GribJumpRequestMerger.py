import pygribjump
import numpy as np

import copy

from zfdb.ZarrMetadataBuilder import ZarrMetadataBuilder

class GribJumpRequestMerger:

    def __init__(self) -> None:
        self.gj = pygribjump.GribJump()

    """
    Divides a given mars request into several individual GribJump requests
    and merge the result to the wished data layout
    """
    def request(self, mars_request: dict):
        gj_axes = self.gj.axes(mars_request)

        flattened_array = []
        all_requests =  GribJumpRequestMerger._assemble_all_subrequests(mars_request, gj_axes)
        
        for subreq in all_requests["date"]:
            gj_result = self.gj.extract([(subreq, [(0, 1 * 542080)])])
            # flattened_array.append(gj_result[0][i][0][0])
            flattened_array.append(gj_result[0][0][0][0])

        return np.concatenate(flattened_array)

    def _assemble_all_subrequests(mars_request: dict, gj_axes):

        # Why is the axes object returning more values than specified in mars request?
        date_keys = sorted(mars_request["date"].split("/"))
        param_keys = sorted(mars_request["param"].split("/"))

        date_requests = GribJumpRequestMerger._subrequests(mars_request, "date", date_keys)

        requests = {
                "date": []
                }
        
        for date_request in date_requests:
            param_requests = GribJumpRequestMerger._subrequests(date_request, "param", param_keys) 
            for param_request in param_requests:
                requests["date"].append(param_request)

        return requests




    @staticmethod
    def _subrequests(mars_request: dict, key: str, values: [str]):
        list_subrequests = []

        for v in sorted(values):
            sub_mars_request = copy.deepcopy(mars_request)
            sub_mars_request[key] = v
            list_subrequests.append(sub_mars_request)

        return list_subrequests


    def forward(self, mars_request: dict):
        return self.gj.extract([(mars_request, [(0, 542080)])])[0][0][0][0]

    def existing(self, mars_request: dict):
        return len(self.gj.extract([(mars_request, [(0, 542080)])])[0][0][0][0]) > 0

    def zarr_metadata(self, mars_request: dict):
        gj_axes = self.gj.axes(mars_request)
        return ZarrMetadataBuilder().default(mars_request)
