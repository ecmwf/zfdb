#!/usr/bin/env python
# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

from tqdm import tqdm


def create_database(file: Path, force: bool) -> sqlite3.Connection:
    if file.is_file():
        logger.debug(f"Database file already exists {file}")
        if force:
            logger.debug(f"Deleting file {file}")
            file.unlink()
        else:
            raise Exception(
                "Refusing to delete existing database, use -f/--force to overwrite"
            )
    if file.is_dir():
        raise Exception("Database path is a directory, aborting.")

    logger.debug(f"Creating database file {file}")
    con = sqlite3.connect(file)
    with con:
        con.execute("drop table if exists syscall_events")
        con.execute(
            """
            create table syscall_events(
                id integer primary key autoincrement,
                pid integer,
                unix_time real,
                syscall text,
                args text,
                return_code integer,
                duration real
            )
            """
        )
    return con


def parse_strace_file_into_database(
    file: Path, database: sqlite3.Connection, show_progress: bool
):
    # Match: "<PID> <FRACTIONAL_UNITX_TIME_MICORSECONDS> <SYSCALL>(<ARGS>) = <RETURNCODE> <<DURATION_IN_SECONDS>>
    # Example: "849700 1740472038.603714 close(3)       = 0 <0.000460>"
    # All whitespaces can be of arbitray length due to straces column aligned output and must be matched with ' +'
    matcher = re.compile(r"^(\d+) +(\d+\.\d+) +(\w+)\((.+)\) += +(\d+) +\<(\d+.\d+)\>$")

    logger.info(f"Importing {file}")
    with open(file) as f:
        for line in tqdm(f, disable=not show_progress):
            m = matcher.match(line)
            if not m:
                logger.debug(f"Cannot match line: {line}")
            else:
                database.execute(
                    "insert into syscall_events values(null,?,?,?,?,?,?)",
                    tuple(m.groups()),
                )
        database.commit()


def fixup_args(args):
    """
    Updates args with deduced output name if output is not set.
    """
    if not args.output:
        args.output = args.input.with_suffix(".sqlite")
    return args


def command_parse_strace_output(args):
    db = create_database(args.output, args.force)
    parse_strace_file_into_database(args.input, db, args.progress)


def add_opts_parse_strace_output(parent_parser):
    parser = parent_parser.add_parser(
        "parse-strace",
        help="Parses strace output into a sqlite database for further analysis.",
    )
    parser.set_defaults(func=command_parse_strace_output)
    parser.add_argument(
        "input",
        help="Strace file to parse",
        type=Path,
    )
    parser.add_argument(
        "-o", "--output", help="Select output name", type=Path, default=None
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force overwrite of existing output database",
        action="store_true",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Strace IO analysis and replay tool")
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, add -vv to get even more verbose output.",
        nargs="*",
        default=0,
    )
    parser.add_argument(
        "-p", "--progress", help="Enables progress output", action="store_true"
    )
    sub_parsers = parser.add_subparsers(dest="cmd", required=True)
    add_opts_parse_strace_output(sub_parsers)
    return parser.parse_args()


def main():
    args = parse_args()
    args = fixup_args(args)
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(
        format="[%(asctime)s] <%(levelname)s> %(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    global logger
    logger = logging.getLogger(__name__)
    try:
        args.func(args)
    except Exception as e:
        logger.error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
