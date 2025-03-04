# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
import mmap
import re
import sqlite3
from pathlib import Path

from database import create_database
from tqdm import tqdm

log = logging.getLogger(__name__)


def register(parent_parser):
    parser = parent_parser.add_parser(
        "parse-strace",
        help="Parses strace output into a sqlite database for further analysis.",
    )
    parser.set_defaults(func=cmd)
    parser.add_argument("input", help="Strace file to parse", type=Path, nargs="+")
    parser.add_argument(
        "-o", "--output", help="Select output name", type=Path, default="out.sqlite"
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force overwrite of existing output database",
        action="store_true",
    )


def cmd(args):
    db = create_database(args.output, args.force)
    parse_strace_file_into_database(args.input, db, args.progress)


def count_bytes(files: list[Path]) -> int:
    return sum(file.stat().st_size for file in files)


def parse_strace_file_into_database(
    files: list[Path], database: sqlite3.Connection, show_progress: bool
):
    # Tool expects strace to be collected with the following flags:
    # strace -ff --decode-fds=path -ttt -qqq -T -s0  -o
    # '-ff'   
    #       Follow forks and output into seperate files, the seperate files part is important
    #       because otherwise calls can be interrupted in the output and have to be put back together
    # '--decode-fds=path'
    #       Decode fds to allow context free parsing of syscalls
    # '-ttt' 
    #       Use 'us' precision timestamps
    # '-qqq'
    #       Be quiet
    # '-T'
    #       Collect syscall duration
    # '-s0'
    #       Do not truncate lines
    #
    # Match: "<PID> <FRACTIONAL_UNITX_TIME_MICORSECONDS> <SYSCALL>(<ARGS>) = <RETURNCODE> <<DURATION_IN_SECONDS>>
    # Example: "1740472038.603714 close(3)       = 0 <0.000460>"
    # All whitespaces can be of arbitray length due to straces column aligned output and must be matched with ' +'
    matcher = re.compile(r"^(\d+\.\d+) +(\w+)\((.*)\) += +(-?\d+).*\<(\d+.\d+)\>$")
    pbar = tqdm(
        files,
        disable=not show_progress,
        total=count_bytes(files),
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        smoothing=0.7,
        mininterval=0.5,
    )
    for file in files:
        pid = file.suffix[1:]
        pbar.set_description(f"Processing {file}")
        with open(file, "r+") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmap_obj:
                for line in iter(mmap_obj.readline, b""):
                    pbar.update(len(line))
                    m = matcher.match(line.decode("utf-8"))
                    if not m:
                        log.debug(f"Cannot match line: '{line}'")
                    else:
                        database.execute(
                            "insert into syscall_events values(null,?,?,?,?,?,?)",
                            tuple([pid, *m.groups()]),
                        )
    database.commit()
