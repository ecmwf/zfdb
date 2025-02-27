# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def register(parent_parser):
    parser = parent_parser.add_parser(
        "replay",
        help="Replay file access",
    )
    parser.set_defaults(func=cmd)
    parser.add_argument("database", help="Database to use", type=Path, nargs=1)


def cmd(args):
    pass
