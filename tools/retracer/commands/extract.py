# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
import re
import sqlite3
from pathlib import Path

from database import open_database
from tqdm import tqdm

log = logging.getLogger(__name__)


def register(parent_parser):
    parser = parent_parser.add_parser(
        "extract-io-syscalls",
        help="Extract read commands from events and create specific table",
    )
    parser.set_defaults(func=command_extract_io_events)
    parser.add_argument(
        "database",
        help="Databse with syscall events",
        type=Path,
    )


def extract_syscalls(con, handlers, show_progress):
    with con:
        for handler in handlers:
            log.info(f"Extracting {handler.call}")
            res = con.execute(
                "select id, pid, unix_time, syscall, return_code, duration, args from syscall_events where syscall = ?",
                (handler.call,),
            )
            for row in tqdm(
                res,
                desc=f"Extracting {handler.call} ",
                disable=not show_progress,
            ):
                args_str = row[-1]
                args = handler.handle(args_str)
                data = (*row[:-1], *args)
                con.execute(handler.insert(), data)


def create_tables(con: sqlite3.Connection, handlers):
    with con:
        for handler in handlers:
            con.execute(handler.delete_table())
            con.execute(handler.create_table())
        con.commit()


def command_extract_io_events(args):
    con = open_database(args.database)
    handlers = make_handlers()
    create_tables(con, handlers)
    extract_syscalls(con, handlers, args.progress)


class SyscallHandler:
    def __init__(self, *, call: str, expected_args: list[str], regex: str):
        self.call = call
        self.table = f"{call}_calls"
        self.expected_args = expected_args
        self.regex = re.compile(regex)
        num_values = 6 + len(expected_args)
        part = ",".join(["?"] * num_values)
        self.insert_smt = f"insert into {self.table} values(null, {part})"

    def handle(self, args: str):
        m = self.regex.match(args)
        if not m:
            raise Exception(f"Cannot match {self.call} args: {args}")
        return m.groups()

    def create_table(self) -> str:
        return f"""
            create table {self.table}(
                id integer primary key autoincrement,
                syscall_events_id integer,
                pid integer,
                unix_time real,
                syscall text,
                return_code integer,
                duration real,
                {",".join([x + " text" for x in self.expected_args])}
            )
            """

    def delete_table(self):
        return f"drop table if exists {self.table}"

    def insert(self):
        return self.insert_smt


def make_handlers() -> list[SyscallHandler]:
    return [
        SyscallHandler(call=c, expected_args=a, regex=r)
        for (c, a, r) in [
            ("openat", ["path", "flags"], r"^.*\"(.*)\", (.*)$"),
            ("read", ["path", "size"], r"^.*\<(.*)\>,.*, +(\d+)$"),
            ("access", ["path", "flags"], r"^.*\"(.*)\", (.*)$"),
            ("close", ["path"], r"^.*\<(.*)\>.*$"),
            ("flock", ["path", "flags"], r"^.*\<(.*)\>, (.*)$"),
            ("fstat", ["path"], r"^.*\<(.*)\>.*$"),
            ("getdents64", ["path"], r"^.*\<(.*)\>.*$"),
            ("lseek", ["path", "offset", "whence"], r"^.*\<(.*)\>, +(-?\d+), +(.+)$"),
            ("lstat", ["path"], r"^.*\"(.*)\".*$"),
            ("stat", ["path"], r"^.*\"(.*)\".*$"),
        ]
    ]
