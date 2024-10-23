from collections import defaultdict

import eccodes
from pyeccodes import Reader


import cfgrib
import uuid
import numpy as np
from . import GridUtils as gu

import logging
from bitstring import BitArray

logger = logging.getLogger("GribTools")

EXTRA_PARAMETERS = [
    "forecastTime",
    "indicatorOfUnitOfTimeRange",
    "lengthOfTimeRange",
    "indicatorOfUnitForTimeRange",
    "productDefinitionTemplateNumber",
    "N",
    "timeRangeIndicator",
    "P1",
    "P2",
    "numberIncludedInAverage",
]

# according to http://www.cosmo-model.org/content/consortium/generalMeetings/general2014/wg6-pompa/grib2/grib/pdtemplate_4.41.htm
time_range_units = {
    0: 60,  # np.timedelta64(1, "m"),
    1: 60 * 60,  # np.timedelta64(1, "h"),
    2: 24 * 60 * 60,  # np.timedelta64(1, "D"),
    # 3   Month
    # 4   Year
    # 5   Decade (10 years)
    # 6   Normal (30 years)
    # 7   Century (100 years)
    # 8-9 Reserved
    10: 3 * 60 * 60,  # np.timedelta64(3, "h"),
    11: 6 * 60 * 60,  # np.timedelta64(6, "h"),
    12: 12 * 60 * 60,  # np.timedelta64(12, "h"),
    13: 1,  # np.timedelta64(1, "s"),
    # 14-191  Reserved
    # 192-254 Reserved for local use
    # 255 Missing
}

production_template_numbers = {
    0: {"forcastTime": True, "timeRange": False},
    1: {"forcastTime": True, "timeRange": False},
    2: {"forcastTime": True, "timeRange": False},
    3: {"forcastTime": True, "timeRange": False},
    4: {"forcastTime": True, "timeRange": False},
    5: {"forcastTime": True, "timeRange": False},
    6: {"forcastTime": True, "timeRange": False},
    7: {"forcastTime": True, "timeRange": False},
    15: {"forcastTime": True, "timeRange": False},
    32: {"forcastTime": True, "timeRange": False},
    33: {"forcastTime": True, "timeRange": False},
    40: {"forcastTime": True, "timeRange": False},
    41: {"forcastTime": True, "timeRange": False},
    44: {"forcastTime": True, "timeRange": False},
    45: {"forcastTime": True, "timeRange": False},
    48: {"forcastTime": True, "timeRange": False},
    51: {"forcastTime": True, "timeRange": False},
    53: {"forcastTime": True, "timeRange": False},
    54: {"forcastTime": True, "timeRange": False},
    55: {"forcastTime": True, "timeRange": False},
    56: {"forcastTime": True, "timeRange": False},
    57: {"forcastTime": True, "timeRange": False},
    58: {"forcastTime": True, "timeRange": False},
    60: {"forcastTime": True, "timeRange": False},
    1000: {"forcastTime": True, "timeRange": False},
    1002: {"forcastTime": True, "timeRange": False},
    1100: {"forcastTime": True, "timeRange": False},
    40033: {"forcastTime": True, "timeRange": False},
    40455: {"forcastTime": True, "timeRange": False},
    40456: {"forcastTime": True, "timeRange": False},
    20: {"forcastTime": False, "timeRange": False},
    30: {"forcastTime": False, "timeRange": False},
    31: {"forcastTime": False, "timeRange": False},
    254: {"forcastTime": False, "timeRange": False},
    311: {"forcastTime": False, "timeRange": False},
    2000: {"forcastTime": False, "timeRange": False},
    8: {"forcastTime": True, "timeRange": True},
    9: {"forcastTime": True, "timeRange": True},
    10: {"forcastTime": True, "timeRange": True},
    11: {"forcastTime": True, "timeRange": True},
    12: {"forcastTime": True, "timeRange": True},
    13: {"forcastTime": True, "timeRange": True},
    14: {"forcastTime": True, "timeRange": True},
    34: {"forcastTime": True, "timeRange": True},
    42: {"forcastTime": True, "timeRange": True},
    43: {"forcastTime": True, "timeRange": True},
    46: {"forcastTime": True, "timeRange": True},
    47: {"forcastTime": True, "timeRange": True},
    61: {"forcastTime": True, "timeRange": True},
    67: {"forcastTime": True, "timeRange": True},
    68: {"forcastTime": True, "timeRange": True},
    91: {"forcastTime": True, "timeRange": True},
    1001: {"forcastTime": True, "timeRange": True},
    1101: {"forcastTime": True, "timeRange": True},
    10034: {"forcastTime": True, "timeRange": True},
}


class MagicianBase:
    def variable_hook(self, key, info):
        pass

    def globals_hook(self, global_attrs):
        return global_attrs

    def coords_hook(self, name, coords):
        return {}, coords, {}, [name], None

    def m2key(self, meta):
        return tuple(meta[key] for key in self.varkeys), tuple(
            meta[key] for key in self.dimkeys
        )

    def m2dataset(self, meta):
        return (
            "atm3d"
            if meta["attrs"]["typeOfLevel"].startswith("generalVertical")
            else "atm2d"
        )

    def extra_coords(self, varinfo):
        return {}


class Magician(MagicianBase):
    varkeys = "param", "levtype"
    dimkeys = "posix_time", "level"

    def globals_hook(self, global_attrs):
        history = global_attrs.get("history", "")
        if len(history) > 0 and not history.endswith("\n"):
            history = history + "\r\n"
        history += "🪄🧙‍♂️🔮 magic dataset assembly provided by gribscan.Magician\r\n"
        return {**global_attrs, "history": history}

    def variable_hook(self, key, info):
        param, levtype = key
        name = param
        dims = info["dims"]

        if levtype == "generalVertical":
            name = param + "half" if param == "zg" else param
            dims = tuple("halflevel" if dim == "level" else dim for dim in dims)
        if levtype == "generalVerticalLayer":
            dims = tuple("fulllevel" if dim == "level" else dim for dim in dims)
        dims = tuple("time" if dim == "posix_time" else dim for dim in dims)

        return {
            "dims": dims,
            "name": name,
        }

    def coords_hook(self, name, coords):
        if "time" in name:
            attrs = {
                "units": "seconds since 1970-01-01T00:00:00",
                "calendar": "proleptic_gregorian",
            }
        else:
            attrs = {}
        return attrs, coords, {}, [name], None


def is_value(v):
    if v is None or v == "undef" or v == "unknown":
        return False
    else:
        return True


def arrays_to_list(o):
    try:
        return o.tolist()
    except AttributeError:
        return o


def get_time_offset(gribmessage, lean_towards="end"):
    """Calculate time offset based on GRIB definition.

    See: https://codes.ecmwf.int/grib/format/grib1/ctable/5/
    """
    offset = 0  # np.timedelta64(0, "s")
    edition = int(gribmessage["editionNumber"])
    if edition == 1:
        timeRangeIndicator = int(gribmessage["timeRangeIndicator"])
        if timeRangeIndicator == 0:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            offset += int(gribmessage["P1"]) * unit
        elif timeRangeIndicator == 1:
            pass
        elif timeRangeIndicator in [2, 3]:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            if lean_towards == "start":
                offset += int(gribmessage["P1"]) * unit
            elif lean_towards == "end":
                offset += int(gribmessage["P2"]) * unit
        elif timeRangeIndicator == 4:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            offset += int(gribmessage["P2"]) * unit
        elif timeRangeIndicator == 10:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            offset += (int(gribmessage["P1"]) * 256 + int(gribmessage["P2"])) * unit
        elif timeRangeIndicator == 123:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            if lean_towards == "end":
                N = int(gribmessage["numberIncludedInAverage"])
                offset += N * int(gribmessage["P2"]) * unit
        else:
            raise NotImplementedError(
                f"don't know how to handle timeRangeIndicator {timeRangeIndicator}"
            )
    else:
        try:
            options = production_template_numbers[
                int(gribmessage["productDefinitionTemplateNumber"])
            ]
        except KeyError:
            return offset
        if options["forcastTime"]:
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            offset += gribmessage.get("forecastTime", 0) * unit
        if options["timeRange"] and lean_towards == "end":
            unit = time_range_units[
                int(gribmessage.get("indicatorOfUnitOfTimeRange", 255))
            ]
            offset += gribmessage.get("lengthOfTimeRange", 0) * unit
    return offset


def find_stream(f, needle, buffersize=1024 * 1024):
    keep_going = True
    while keep_going:
        start = f.tell()
        buf = f.read(buffersize)
        if len(buf) < buffersize:
            keep_going = False
        try:
            idx = buf.index(needle)
        except ValueError:
            f.seek(-len(needle), 1)
            continue
        else:
            pos = start + idx
            f.seek(pos)
            return pos


def detect_large_grib1_special_coding(f, part_size):
    """
    This is from eccodes src/grib_io.c /* Special coding */ (couldn't find it in the specs...)
    """
    if part_size & 0x800000:  # this is a large grib, hacks are coming...
        start = f.tell()
        data = f.read(part_size)
        f.seek(start)
        assert data[7] == 1, "large grib mode only exists in Grib 1"

        s0len = 8
        s1start = s0len
        s1len = int.from_bytes(data[s1start : s1start + 3], "big")
        flags = data[s1start + 7]
        has_s2 = bool(flags & (1 << 7))
        has_s3 = bool(flags & (1 << 6))

        s2start = s1start + s1len
        if has_s2:
            s2len = int.from_bytes(data[s2start : s2start + 3], "big")
        else:
            s2len = 0

        s3start = s2start + s2len
        if has_s3:
            s3len = int.from_bytes(data[s3start : s3start + 3], "big")
        else:
            s3len = 0

        s4start = s3start + s3len

        s4len = int.from_bytes(data[s4start : s4start + 3], "big")
        if s4len < 120:
            return (part_size & 0x7FFFFF) * 120 - s4len + 4
        else:
            return part_size

    else:  # normal grib
        return part_size


# def _split_file(f, skip=0):
#     """
#     splits a gribfile into individual messages
#     """
#     if hasattr(f, "size"):
#         size = f.size
#     else:
#         f.seek(2)
#         size = f.tell()
#         f.seek(0)
#     part = 0
#
#     while f.tell() < size:
#         logger.debug(f"extract part {part + 1}")
#         start = f.tell()
#         indicator = f.read(16)
#         if indicator[:4] != b"GRIB":
#             logger.info(f"non-consecutive messages, searching for part {part + 1}")
#             start = find_stream(f, b"GRIB")
#             indicator = f.read(16)
#         if len(indicator) < 16:
#             return
#
#         grib_edition = indicator[7]
#
#         if grib_edition == 1:
#             part_size = int.from_bytes(indicator[4:7], "big")
#             part_size = detect_large_grib1_special_coding(f, part_size)
#         elif grib_edition == 2:
#             part_size = int.from_bytes(indicator[8:16], "big")
#         else:
#             raise ValueError(f"unknown grib edition: {grib_edition}")
#
#         data = f.read(part_size)
#         if data[-4:] != b"7777":
#             logger.warning(f"part {part + 1} is broken")
#             f.seek(start + 1)
#         else:
#             yield start, part_size, grib_edition, data
#
#         part += 1
#         if skip and part > skip:
#             break


def _split_file(data_retriever):
    iterator = Reader(data_retriever)

    for msg in iterator:
        yield msg


def scan_gribfile(data_retriever, **kwargs):
    for msg in _split_file(data_retriever):
        mid = eccodes.codes_new_from_message(bytes(msg._buffer))
        m = cfgrib.cfmessage.CfMessage(mid)
        t = eccodes.codes_get_native_type(m.codes_id, "values")
        s = eccodes.codes_get_size(m.codes_id, "values")

        global_attrs = {k: m[k] for k in cfgrib.dataset.GLOBAL_ATTRIBUTES_KEYS}
        for uuid_key in ["uuidOfHGrid", "uuidOfVGrid"]:
            try:
                global_attrs[uuid_key] = str(
                    uuid.UUID(eccodes.codes_get_string(mid, uuid_key))
                )
            except eccodes.KeyValueNotFoundError:
                pass

        yield {
            "globals": global_attrs,
            "attrs": {
                k: m.get(k, None)
                for k in cfgrib.dataset.DATA_ATTRIBUTES_KEYS
                + cfgrib.dataset.EXTRA_DATA_ATTRIBUTES_KEYS
            },
            "parameter_code": {
                k: m.get(k, None)
                for k in ["discipline", "parameterCategory", "parameterNumber"]
            },
            "posix_time": m["time"] + get_time_offset(m),
            "domain": m["globalDomain"],
            "time": f"{m['hour']:02d}{m['minute']:02d}",
            "date": f"{m['year']:04d}{m['month']:02d}{m['day']:02d}",
            "levtype": m.get("typeOfLevel", None),
            "level": m.get("level", None),
            "param": m.get("shortName", None),
            "type": m.get("dataType", None),
            "referenceTime": m["time"],
            "step": m["step"],
            "_offset": msg._offset,
            "_length": msg.count,
            "array": {
                "dtype": np.dtype(t).str,
                "shape": [s],
            },
            "extra": {
                k: arrays_to_list(m.get(k, None))
                for k in (EXTRA_PARAMETERS + gu.params_for_gridType(m["gridType"]))
            },
            **kwargs,
        }


def inspect_grib_indices(messages, magician=Magician()):
    coords_by_key = defaultdict(lambda: tuple(set() for _ in magician.dimkeys))
    size_by_key = defaultdict(set)
    attrs_by_key = {}
    extra_by_key = {}
    dtype_by_key = {}
    global_attrs = {}

    for msg in messages:
        varkey, coords = magician.m2key(msg)
        for existing, new in zip(coords_by_key[varkey], coords):
            existing.add(new)
        size_by_key[varkey].add(msg["array"]["shape"][0])
        attrs_by_key[varkey] = {k: v for k, v in msg["attrs"].items() if is_value(v)}
        extra_by_key[varkey] = {k: v for k, v in msg["extra"].items() if is_value(v)}
        dtype_by_key[varkey] = msg["array"]["dtype"]
        global_attrs = msg["globals"]

    for k, v in size_by_key.items():
        assert len(v) == 1, f"inconsistent shape of {k}"

    size_by_key = {k: list(v)[0] for k, v in size_by_key.items()}

    varinfo = {}
    for varkey, coords in coords_by_key.items():
        if all(len(c) == 1 for c in coords):
            dims = ()
            dim_id = ()
            shape = ()
        else:
            dims, dim_id, shape = map(
                tuple,
                zip(
                    *(
                        (dim, i, len(coords))
                        for i, (dim, coords) in enumerate(zip(magician.dimkeys, coords))
                        if len(coords) != 1
                    )
                ),
            )

        info = {
            "dims": dims,
            "shape": shape,
            "dim_id": dim_id,
            "coords": tuple(coords_by_key[varkey][i] for i in dim_id),
            "data_shape": [size_by_key[varkey]],
            "data_dims": ["cell"],
            "dtype": dtype_by_key[varkey],
            "attrs": attrs_by_key[varkey],
            "extra": extra_by_key[varkey],
        }
        varinfo[varkey] = {
            **info,
            **magician.variable_hook(varkey, info),
        }

    coords = defaultdict(set)
    for _, info in varinfo.items():
        for dim, cs in zip(info["dims"], info["coords"]):
            coords[dim] |= cs

    coords = {
        **{k: list(sorted(c)) for k, c in coords.items()},
        **magician.extra_coords(varinfo),
    }

    return global_attrs, coords, varinfo
