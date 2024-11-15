import pygribjump
import numpy as np

import copy

# from zfdb.business.ZarrMetadataBuilder import ZarrMetadataBuilder
from zfdb.business.Request import Request


class GribJumpRequestMapper:
    @staticmethod
    def to_grib_jump(dictonary: dict[str, list[str]]) -> dict[str, str]:
        pygribjump_dict = {}

        for key in dictonary.keys():
            pygribjump_dict[key] = "/".join(dictonary[key])

        return pygribjump_dict


class GribJumpRequestMerger:
    def __init__(self) -> None:
        self.gj = pygribjump.GribJump()

    """
    Divides a given mars request into several individual GribJump requests
    and merge the result to the wished data layout
    """

    def request(self, mars_request: dict[str, list[str]]):
        gribjump_request = GribJumpRequestMapper.to_grib_jump(mars_request)

        gj_axes = self.gj.axes(gribjump_request)

        flattened_array = []
        all_requests = GribJumpRequestMerger._assemble_all_subrequests(
            gribjump_request, gj_axes
        )

        for subreq in all_requests["date"]:
            gj_result = self.gj.extract([(subreq, [(0, 1 * 542080)])])
            # flattened_array.append(gj_result[0][i][0][0])
            flattened_array.append(gj_result[0][0][0][0])

        return np.concatenate(flattened_array)

    def _assemble_all_subrequests(mars_request: dict, gj_axes):
        # Why is the axes object returning more values than specified in mars request?
        date_keys = sorted(mars_request["date"].split("/"))
        param_keys = sorted(mars_request["param"].split("/"))

        date_requests = GribJumpRequestMerger._subrequests(
            mars_request, "date", date_keys
        )

        requests = {"date": []}

        for date_request in date_requests:
            param_requests = GribJumpRequestMerger._subrequests(
                date_request, "param", param_keys
            )
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

    def forward(self, mars_request: dict[str, list[str]]):
        pygribjump_request = GribJumpRequestMapper.to_grib_jump(mars_request)

        return self.gj.extract([(pygribjump_request, [(0, 542080)])])[0][0][0][0]

    def axes(self, mars_request: dict):
        return self.gj.axes(mars_request)

    # def is_full_specified_request(self, mars_request: Request):
    #     # axes = self.gj.axes(mars_request)
    #     #
    #     # if(len(axes) != len(mars_request)):
    #     #     return False
    #     #
    #     # # Flatten 1D keys
    #     # for key in mars_request.keys():
    #     #     if(len(axes[key]) == 1):
    #     #         axes[key] = axes[key][0]
    #     #
    #     # for key in axes.keys():
    #     #     # Levellist bug? Returing all keys although only one is specfied
    #     #     if key in ["levelist", "param", "step" ]:
    #     #         continue
    #     #
    #     #     # Axes consists out of a range for a key, so the request wasn't
    #     #     # fully specified
    #     #     if axes[key] != mars_request[key]:
    #     #         return False
    #     #
    #     # return True
    #
    #     full_request = mars_request.build_mars_request()
    #     mars_keys = full_request.keys()
    #
    #     if len(mars_keys) < 11:
    #         return False
    #
    #     for key in mars_keys:
    #         if key in ["date", "param", "levelist"]:
    #             continue
    #         values =  full_request[key]
    #         if isinstance(values, list) and len(values) != 1:
    #             return False
    #
    #     return True
    #

    def existing(self, mars_request: dict[str, list[str]]):
        pygribjump_request = GribJumpRequestMapper.to_grib_jump(mars_request)
        return (
            len(self.gj.extract([(pygribjump_request, [(0, 542080)])])[0][0][0][0]) > 0
        )

    def zarr_metadata(self, mars_request: dict[str, list[str]]):
        return ZarrMetadataBuilder().default(mars_request)
