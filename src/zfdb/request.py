# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from dataclasses import dataclass

import numpy as np


@dataclass(kw_only=True)
class Request:
    """
    See https://apps.ecmwf.int/mars-catalogue/
    """

    cls: str = "od"
    stream: str = "oper"
    expver: int = 1
    typ: str = "fc"
    levtype: str = "sfc"
    level: None | list[str] = None
    ensemble_number: None | int = None
    date_time: np.datetime64
    steps: list[int]
    params: list[str]

    def matches_on_time_axis(self, other) -> bool:
        return self.date_time == other.date_time and self.steps == other.steps

    def as_mars_request_for_step_index(self, step_index: int) -> dict:
        res = {
            "class": self.cls,
            "stream": self.stream,
            "expver": str(self.expver),
            "type": self.typ,
            "levtype": self.levtype,
            "step": str(self.steps[step_index]),
            "param": self._as_mars_list(self.params),
            "date": self._as_mars_date(self.date_time),
            "time": self._time_in_day(self.date_time),
            "domain": "g",
        }
        if self.level:
            res["level"] = self._as_mars_list(self.level)
        if self.ensemble_number:
            res["number"] = str(self.ensemble_number)
        return res

    @property
    def field_count(self) -> int:
        if self.level:
            return len(self.level) * len(self.params)
        return len(self.params)

    @property
    def steps_count(self) -> int:
        return len(self.steps)

    @staticmethod
    def _as_mars_list(values) -> str:
        return "/".join([str(i) for i in values])

    @staticmethod
    def _as_mars_month(val: np.datetime64) -> str:
        month_idx = int(np.datetime_as_string(val, "M").split("-")[1]) - 1
        return [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ][month_idx]

    @staticmethod
    def _as_mars_date(t: np.datetime64) -> str:
        return np.datetime_as_string(t, unit="D").replace("-", "")

    @staticmethod
    def _time_in_day(t: np.datetime64) -> str:
        return np.datetime_as_string(t).split("T")[1]
