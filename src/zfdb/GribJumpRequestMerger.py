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
        time_dim = len(gj_axes['date'])

        print("***** Slices {} ********************".format(len(gj_axes["date"])))
        print(GribJumpRequestMerger._subrequests(mars_request, "date", gj_axes["date"]))
        print("****************************")

        gj_result = self.gj.extract([(mars_request, [(0, 1 * 542080)])])

        flattened_array = []

        for i in range(time_dim):
            flattened_array.append(gj_result[0][i][0][0])

        return np.concatenate(flattened_array)

    @staticmethod
    def _subrequests(mars_request: dict, key: str, values: [str]):
        list_subrequests = []

        for v in values:
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
        return ZarrMetadataBuilder().default(gj_axes)
