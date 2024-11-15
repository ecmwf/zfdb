import json
from dataclasses import KW_ONLY, asdict, dataclass

"""
This module contains generator for all dot files that can
be part of a zarr store: '.zarray', '.zgroup' and '.zattrs'
"""


@dataclass(frozen=True)
class DotZarrArray:
    _: KW_ONLY
    shape: list[int]
    chunks: list[int]
    # TODO(kkratz) Use enums for allowed types
    dtype: str | list[str]
    order: str
    fill_value: bool | int | float | None = 666
    filters: None = None
    compressor: None = None
    dimension_separator: str = "."
    zarr_format: int = 2

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    @staticmethod
    def from_mars_request(req):
        # TODO(kkratz): TO BE DONE
        DotZarrArray(shape=[1], chunks=[1], dtype=">i4", order="C")


@dataclass(frozen=True)
class DotZarrGroup:
    _: KW_ONLY
    zarr_format: int = 2

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")


@dataclass(frozen=True)
class DotZarrAttributes:
    _: KW_ONLY
    copyright: str = "ecmwf"

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")
