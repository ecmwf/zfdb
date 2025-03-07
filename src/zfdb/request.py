# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import copy
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, auto

import numpy as np

from zfdb.error import ZfdbError


def is_sequence(t) -> bool:
    return not isinstance(t, str) and (
        isinstance(t, Sequence) or isinstance(t, np.ndarray)
    )


class ChunkAxis(ABC):
    @abstractmethod
    def __getitem__(self, index: int) -> dict: ...

    def __len__(self) -> int: ...

    def keys(self) -> list[str]: ...


class ChunkAxisType(Enum):
    DateTime = auto
    Step = auto


class ChunkDateTime(ChunkAxis):
    def __init__(self, date, time):
        self._date = date
        self._time = time

    def __getitem__(self, index: int) -> dict:
        date_idx = index // len(self._time)
        time_idx = index % len(self._time)
        return {"date": self._date[date_idx], "time": self._time[time_idx]}

    def __len__(self) -> int:
        return len(self._time) * len(self._date)

    def keys(self) -> list[str]:
        return ["date", "time"]


class ChunkSteps(ChunkAxis):
    def __init__(self, step):
        self._step = step

    def __getitem__(self, index: int) -> dict:
        return {"step": self._step[index]}

    def __len__(self) -> int:
        return len(self._step)

    def keys(self) -> list[str]:
        return ["step"]


def into_mars_request_dict(mars_request: dict) -> dict[str, str]:
    mars_request_result = copy.deepcopy(mars_request)

    if isinstance(mars_request_result, dict):
        for k, v in mars_request_result.items():
            mars_request_result[k] = into_mars_representation(v)
    else:
        raise RuntimeError("mars_request must be a dictionary.")
    return mars_request_result


def into_mars_representation(value) -> str:
    if is_sequence(value) and len(value) == 1:
        value = value[0]
    if isinstance(value, str):
        return value
    if isinstance(value, np.datetime64) and value.dtype == "datetime64[D]":
        return str(value).replace("-", "")
    if is_sequence(value):
        return "/".join([into_mars_representation(v) for v in value])
    else:
        return str(value)


class Request:
    def __init__(self, *, request, chunk_axis: ChunkAxisType):
        self._request = request
        self._template = request.copy()
        if chunk_axis == ChunkAxisType.DateTime:
            date = request["date"]
            if not is_sequence(date):
                date = [date]
            # date = into_mars_representation(date)
            if isinstance(date, np.ndarray):
                date = list(date)
            time = request["time"]
            if not is_sequence(time):
                time = [time]
            self._chunk_axis = ChunkDateTime(date, time)
        elif chunk_axis == ChunkAxisType.Step:
            step = request["step"]
            if not is_sequence(step):
                step = [step]
            self._chunk_axis = ChunkSteps(step)
        else:
            raise ZfdbError("Unknown chunking specified")
        for key in self._chunk_axis.keys():
            self._template.pop(key, None)
        self._template = into_mars_request_dict(self._template)

    def __getitem__(self, idx) -> dict:
        return self._template | into_mars_request_dict(self._chunk_axis[idx])

    def chunk_axis(self) -> ChunkAxis:
        return self._chunk_axis


@dataclass(kw_only=True)
class RequestOld:
    """
    See https://apps.ecmwf.int/mars-catalogue/
    """

    cls: str = "od"
    stream: str = "oper"
    expver: str = "1"
    typ: str = "fc"
    levtype: str = "sfc"
    level: None | str | list[str] = None
    ensemble_number: None | str | list[str] = None
    date: str | list[str]
    time: str | list[str]
    step: int | list[int] = 0
    params: list[str]
    # Can be one of 'datetime' or 'step'
    chunk_on: str = "datetime"

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = [self.date]
        if isinstance(self.time, str):
            self.time = [self.time]
        if isinstance(self.step, int):
            self.step = [self.step]

        if self.chunk_on == "datetime":
            self._chunk_axis = ChunkDateTime(self.date, self.time)
        elif self.chunk_on == "step":
            self._chunk_axis = ChunkSteps(self.step)
        else:
            raise ZfdbError("Unknown chunking specified")
        self._request_template = {
            "class": self.cls,
            "stream": self.stream,
            "expver": str(self.expver),
            "type": self.typ,
            "levtype": self.levtype,
            "step": str(self.steps[step_index]),
            "param": self._as_mars_list(self.params),
            "date": [self._as_mars_date(date_time) for date_time in self.date_times],
            "time": [self._time_in_day(date_time) for date_time in self.date_times],
            "domain": "g",
        }
