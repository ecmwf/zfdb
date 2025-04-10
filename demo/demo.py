#! /usr/bin/env python
# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import argparse
import logging
import math
import os
import random
import shutil
import sys
import time
from collections import namedtuple
from dataclasses import KW_ONLY, dataclass
from pathlib import Path

import eccodes
import numpy as np
import pyfdb
import pygribjump
import tqdm
import yaml
import zarr

import zfdb


def be_quiet_stdout_and_stderr():
    """
    Redirects current stdout/stderr to /dev/null and then recreates stdout and
    stderr. This lets libfdb and libgribjump continue to use the redirected
    FDs, i.e. send their output to /dev/null while the rest of the application
    can use stdout stderr normally.
    """
    redirect_file = open("/dev/null")

    stdout_fd = sys.stdout.fileno()
    orig_stdout_fd = os.dup(stdout_fd)
    sys.stdout.close()
    os.dup2(redirect_file.fileno(), stdout_fd)
    sys.stdout = os.fdopen(orig_stdout_fd, "w")

    stderr_fd = sys.stderr.fileno()
    orig_stderr_fd = os.dup(stderr_fd)
    sys.stderr.close()
    os.dup2(redirect_file.fileno(), stderr_fd)
    sys.stderr = os.fdopen(orig_stderr_fd, "w")


def print_in_closest_unit(val_in_ns) -> str:
    """
    Prints a nano second time value as human readable string.
    10 -> '10ns'
    1000 -> '10μs'
    1000000 - > '10ms'

    raises ValueError on values >= 1e12
    """
    units = ["ns", "μs", "ms", "s"]
    exp = int(math.log10(val_in_ns)) // 3
    if exp < len(units):
        return f"{val_in_ns / 10 ** (exp * 3)}{units[exp]}"
    raise ValueError


def as_multibyte_str(val) -> str:
    base_2_exp = math.log(val, 2)
    if base_2_exp < 10:
        return f"{val}B"
    if base_2_exp < 20:
        return f"{(val // 2**10):.2f}KiB"
    if base_2_exp < 30:
        return f"{(val // 2**20):.2f}MiB"
    if base_2_exp < 40:
        return f"{(val // 2**30):.2f}GiB"
    if base_2_exp < 50:
        return f"{(val // 2**40):.2f}TiB"
    if base_2_exp < 60:
        return f"{(val // 2**50):.2f}PiB"
    return val


@dataclass(frozen=True)
class AnemoiExampleDataSet:
    _: KW_ONLY
    name: str
    grib_file: Path
    recipe: Path
    anemoi_dataset: Path


@dataclass(frozen=True)
class ForecastExampleDataSet:
    _: KW_ONLY
    name: str
    grib_file: Path
    requests: list[zfdb.Request]


def example_datasets() -> list[AnemoiExampleDataSet | ForecastExampleDataSet]:
    data_location = Path(__file__).parent / "demo-data"

    return [
        AnemoiExampleDataSet(
            name="aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january",
            grib_file=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.grib",
            recipe=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml",
            anemoi_dataset=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.zarr",
        ),
        ForecastExampleDataSet(
            name="Forecast data example",
            grib_file=data_location / "fc_example.grib",
            requests=[
                zfdb.Request(
                    request={
                        "levtype": "sfc",
                        "steps": list(range(0, 48)),
                        "date": np.datetime64("2025-01-01"),
                        "time": "0000",
                        "params": [
                            "165",
                            "166",
                            "168",
                            "167",
                            "172",
                            "151",
                            "160",
                            "235",
                            "163",
                            "134",
                            "136",
                            "129",
                        ],
                    },
                    chunk_axis=zfdb.ChunkAxisType.Step,
                ),
                zfdb.Request(
                    request={
                        "levtype": "pl",
                        "steps": list(range(0, 48)),
                        "date": np.datetime64("2025-01-01"),
                        "time": "0000",
                        "params": ["133", "130", "131", "132", "135", "129"],
                        "level": [
                            "50",
                            "100",
                            "150",
                            "200",
                            "250",
                            "300",
                            "400",
                            "500",
                            "600",
                            "700",
                            "850",
                            "925",
                            "1000",
                        ],
                    },
                    chunk_axis=zfdb.ChunkAxisType.Step,
                ),
            ],
        ),
    ]


def create_database(demo_data_path: Path, path):
    db_store_path = path / "db_store"
    logger.info(f"creating db configuration at {db_store_path}")
    if db_store_path.exists():
        shutil.rmtree(db_store_path)
    db_store_path.mkdir(parents=True, exist_ok=True)

    schema_path = path / "schema"
    shutil.copy(demo_data_path / "schema", schema_path)
    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "handler": "Default",
                "roots": [
                    {"path": str(db_store_path)},
                ],
            }
        ],
    }
    fdb_config_path = path / "fdb_config.yaml"
    fdb_config_path.write_text(yaml.dump(fdb_config))
    gj_config = {"plugin": {"select": "class=(ea|od),stream=(enfo|oper),expver=(00..)"}}
    gj_config_path = path / "gribjump_config.yaml"
    gj_config_path.write_text(yaml.dump(gj_config))
    return namedtuple("ConfigFiles", "fdb_config_path gribjump_config_path")(
        fdb_config_path, gj_config_path
    )


def open_gribjump(config_path: Path):
    logger.info(f"Opening gribjump with config {config_path}")
    os.environ["GRIBJUMP_CONFIG_FILE"] = str(config_path)
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"
    return pygribjump.GribJump()


def open_database(config_path: Path):
    logger.info(f"Opening fdb with config {config_path}")
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)
    os.environ["FDB_ENABLE_GRIBJUMP"] = "1"
    return pyfdb.FDB()


def import_example_data(
    fdb,
    datasets: list[AnemoiExampleDataSet | ForecastExampleDataSet],
    show_progress: bool,
    flush_every_nth_message=256,
):
    logger.info("Importing data into fdb")
    for dataset in datasets:
        import_grib_file(
            fdb,
            dataset.grib_file.expanduser().resolve(),
            show_progress,
            flush_every_nth_message,
        )


def import_grib_file(fdb, grib_file, show_progress, flush_every_nth_message=256):
    logger.info(f"Archiving {grib_file}")
    grib_file = eccodes.FileReader(grib_file)
    for idx, msg in enumerate(tqdm.tqdm(grib_file, disable=not show_progress)):
        fdb.archive(msg.get_buffer())
        if (idx + 1) % flush_every_nth_message == 0:
            fdb.flush()
    fdb.flush()


def create_callgraph(
    fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: AnemoiExampleDataSet
):
    print("Creating profile_stats.txt with python profiling information.")
    fdb_view = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(dataset.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
        ),
        mode="r",
        zarr_format=3,
    )
    import cProfile

    with cProfile.Profile() as pr:
        [x for x in fdb_view["data"]]
        pr.dump_stats("profile_stats.txt")


def demo_performance_comparison(
    fdb: pyfdb.FDB,
    gribjump: pygribjump.GribJump,
    dataset: AnemoiExampleDataSet,
    show_progress: bool,
) -> None:
    if not dataset.anemoi_dataset:
        raise Exception(
            "Cannot compare performance without anemoi dataset as baseline."
        )
    print(f"Running performance comparison on {dataset.name}")
    print("Opening fdb view")
    fdb_view = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(dataset.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
        ),
        mode="r",
        zarr_format=3,
    )

    print("Opening matching anemoi dataset")
    anemoi = zarr.open_group(dataset.anemoi_dataset, mode="r")

    def time_compute_field_sum_full_time_range(stores, idx, iterations):
        timings = [[] for _ in stores]
        for _ in tqdm.tqdm(range(iterations), disable=not show_progress):
            for idx, data in enumerate(stores):
                t0 = time.perf_counter_ns()
                for chunk_idx in range(data["data"].chunks[0]):
                    x = data["data"][chunk_idx][idx][0]
                    np.sum(x)
                t1 = time.perf_counter_ns()
                timings[idx].append(t1 - t0)
        return timings

    def report_timings(timings):
        mean = np.mean(timings)
        minimum = np.min(timings)
        maximum = np.max(timings)
        median = np.median(timings)
        return f"Access time per chunk:\n\tMin {print_in_closest_unit(minimum)}\n\tMean {print_in_closest_unit(mean)}\n\tMax {print_in_closest_unit(maximum)}\n\tMedian {print_in_closest_unit(median)}"

    iterations = 128
    print(
        f"Summing 10u over {fdb_view['data'].chunks[0]} datetimes dataset / fdb multiple times (n={iterations})"
    )
    timings = time_compute_field_sum_full_time_range([anemoi, fdb_view], 0, iterations)

    print("IO times for reading zarr from disk")
    print(report_timings(timings[0]))
    print("IO times for reading from fdb")
    print(report_timings(timings[1]))


def demo_aggeration(
    fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: AnemoiExampleDataSet
) -> None:
    print(f"Running aggregation example on {dataset.name}")
    print("Opening fdb view")
    fdb_view = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(dataset.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
        ),
        mode="r",
        zarr_format=3,
    )
    variable_names = fdb_view["data"].attrs["variables"]
    means_per_sample = []
    print(
        f"Computing means for {len(variable_names)} variables on {fdb_view['data'].shape[0]} dates with {fdb_view['data'].shape[3]} values per field"
    )
    print(fdb_view["data"])
    for sample in tqdm.tqdm(fdb_view["data"]):
        means_per_variable = np.mean(sample, axis=2).squeeze()
        means_per_sample.append(means_per_variable)
    means = np.mean(means_per_sample, axis=0)
    print("Means for each vaiable:")
    print("\n".join([f"\t{name} = {val}" for name, val in zip(variable_names, means)]))


def compute_mean_per_field(store) -> None:
    logger.info(
        f"Computing means over {store['data'].shape[0]} dates with {store['data'].shape[3]} values per field"
    )
    means_per_sample = []

    for sample in tqdm.tqdm(store["data"]):
        means_per_variable = np.mean(sample, axis=2).squeeze()
        means_per_sample.append(means_per_variable)
    means = np.mean(means_per_sample, axis=0)


def create_db_cmd(args):
    logger.info("Creating demo database")
    configs = create_database(Path(__file__).parent / "demo-data", args.path)
    if not args.empty:
        fdb = open_database(configs.fdb_config_path)
        datasets = example_datasets()
        import_example_data(fdb, datasets, args.progress)


def import_data_cmd(args):
    logger.info("Importing data into demo database")
    fdb_config_path = args.database / "fdb_config.yaml"
    fdb = open_database(fdb_config_path)
    for p in args.path:
        p = p.expanduser().resolve()
        if p.is_dir():
            for f in p.iterdir():
                import_grib_file(fdb, f, args.progress)

        elif p.is_file():
            import_grib_file(fdb, p, args.progress)


def profile_cmd(args):
    if args.source == "fdb-era5":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, AnemoiExampleDataSet)
        ][0]
        fdb = open_database(args.database / "fdb_config.yaml")
        gribjump = open_gribjump(args.database / "gribjump_config.yaml")
        store = zarr.open_group(
            zfdb.make_anemoi_dataset_like_view(
                recipe=yaml.safe_load(dataset.recipe.read_text()),
                fdb=fdb,
                gribjump=gribjump,
            ),
            mode="r",
            zarr_format=3,
        )
    elif args.source == "fdb-fc":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, ForecastExampleDataSet)
        ][0]
        fdb = open_database(args.database / "fdb_config.yaml")
        gribjump = open_gribjump(args.database / "gribjump_config.yaml")
        store = zarr.open_group(
            zfdb.make_forecast_data_view(
                request=dataset.requests,
                fdb=fdb,
                gribjump=gribjump,
            ),
            mode="r",
            zarr_format=3,
        )
    elif args.source == "zarr-era5":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, AnemoiExampleDataSet)
        ][0]
        store = zarr.open_group(dataset.anemoi_dataset, mode="r")
    else:
        logger.error(f"Unknown datasource {args.source}. Aborting.")
        sys.exit(-1)

    for _ in range(128):
        t0 = time.perf_counter_ns()
        compute_mean_per_field(store)
        t1 = time.perf_counter_ns()
        logger.info(f"Computation took {print_in_closest_unit(t1 - t0)}")


def simulate_training_cmd(args):
    fdb = open_database(args.database / "fdb_config.yaml")
    gribjump = open_gribjump(args.database / "gribjump_config.yaml")
    store = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(args.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
            extractor=args.extractor,
        ),
        mode="r",
        zarr_format=3,
    )

    data = store["data"]
    dates = data.shape[0]
    base_date_access_order = list(range(0, dates - 2))
    random.shuffle(base_date_access_order)

    for idx in tqdm.tqdm(base_date_access_order, disable=not args.progress):
        logger.info(f"Processing chunks[{idx}, {idx + 1}, {idx + 2}]")
        np.mean(data[idx], axis=2).squeeze()
        np.mean(data[idx + 1], axis=2).squeeze()
        np.mean(data[idx + 2], axis=2).squeeze()


def simulate_training_cmd2(args):
    store = zarr.open_group(args.zstore)

    data = store["data"]
    dates = data.shape[0]
    base_date_access_order = list(range(0, dates - 2))[: 31 * 4]
    random.shuffle(base_date_access_order)

    for idx in tqdm.tqdm(base_date_access_order, disable=not args.progress):
        logger.info(f"Processing chunks[{idx}, {idx + 1}, {idx + 2}]")
        np.mean(data[idx], axis=2).squeeze()
        np.mean(data[idx + 1], axis=2).squeeze()
        np.mean(data[idx + 2], axis=2).squeeze()


def dump_zarr_cmd(args):
    fdb = open_database(args.database / "fdb_config.yaml")
    gribjump = open_gribjump(args.database / "gribjump_config.yaml")
    store = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(args.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
            extractor=args.extractor,
        ),
        mode="r",
        zarr_format=3,
    )
    zarr_path = args.out
    ls = zarr.storage.LocalStore(zarr_path)
    zarr_store = zarr.create_group(store=ls)
    zarr_store.create_array(
        "data", shape=store["data"].shape, chunks=store["data"].chunks, dtype="float32"
    )

    for idx, data in enumerate(store["data"]):
        zarr_store["data"][idx] = data


def throughput_test_cmd(args):
    zarr_path = Path("dump.zarr")
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    fdb = open_database(args.database / "fdb_config.yaml")
    gribjump = open_gribjump(args.database / "gribjump_config.yaml")
    fdb_store = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(args.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
            extractor="eccodes",
        ),
        mode="r",
        zarr_format=3,
    )
    logger.info(f"Dumping zarr store to {zarr_path}")
    ls = zarr.storage.LocalStore(zarr_path)
    zarr_store = zarr.create_group(store=ls)
    zarr_store.create_array(
        "data",
        shape=fdb_store["data"].shape,
        chunks=fdb_store["data"].chunks,
        dtype="float32",
    )

    for idx, data in enumerate(fdb_store["data"]):
        zarr_store["data"][idx] = data

    total_bytes = math.prod([*zarr_store["data"].shape, 4])
    chunk_bytes = math.prod([*zarr_store["data"].chunks, 4])
    logger.info(
        f"Troughput Zarr [Chunk {as_multibyte_str(chunk_bytes)} / Total {as_multibyte_str(total_bytes)}]"
    )

    with tqdm.tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        smoothing=0.7,
        mininterval=0.01,
    ) as pbar:
        for _ in zarr_store["data"]:
            pbar.update(chunk_bytes)

    total_bytes = math.prod([*fdb_store["data"].shape, 4])
    chunk_bytes = math.prod([*fdb_store["data"].chunks, 4])
    logger.info(
        f"Troughput FDB [Chunk {as_multibyte_str(chunk_bytes)} / Total {as_multibyte_str(total_bytes)}]"
    )
    with tqdm.tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        smoothing=0.7,
        mininterval=0.01,
    ) as pbar:
        for _ in fdb_store["data"]:
            pbar.update(chunk_bytes)


def parse_cli_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-v", "--verbose", help="Enables verbose output", action="store_true"
    )
    parser.add_argument(
        "-p", "--progress", help="Show progress output", action="store_true"
    )
    sub_parsers = parser.add_subparsers(dest="cmd", required=True)

    create_db_parser = sub_parsers.add_parser(
        "create-db",
        help="Creates a new fdb and gribjump configuration.",
    )
    create_db_parser.set_defaults(func=create_db_cmd)
    create_db_parser.add_argument(
        "path",
        help="Path where the database will be created",
        type=Path,
        nargs="?",
        default=Path.cwd(),
    )
    create_db_parser.add_argument(
        "--empty",
        help="Do not import example data, just create an empty database",
        action="store_true",
    )

    import_data_parser = sub_parsers.add_parser(
        "import-data",
        help="Import data into FDB, can point to a single grib file or a directory",
    )
    import_data_parser.set_defaults(func=import_data_cmd)
    import_data_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    import_data_parser.add_argument(
        "path",
        help="Path from where to read grib file or files",
        type=Path,
        nargs="+",
    )

    profile_parser = sub_parsers.add_parser(
        "profile",
        help="Run example computation on zarr or fdb zarr. "
        "Requires an existing prepopulated database. "
        "Create the database with 'create-db'.",
    )
    profile_parser.set_defaults(func=profile_cmd)
    profile_parser.add_argument(
        "source",
        choices=["zarr-era5", "fdb-era5", "fdb-fc"],
        default="fdb-era5",
        nargs="?",
    )
    profile_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    simulate_training_parser = sub_parsers.add_parser(
        "simulate-training-zfdb",
        help="Simulates data access similar to anemoi training",
    )
    simulate_training_parser.set_defaults(func=simulate_training_cmd)
    simulate_training_parser.add_argument(
        "recipe",
        help="path to anemoi like recipe.yaml describing the training data",
        type=Path,
    )
    simulate_training_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    simulate_training_parser.add_argument(
        "-e",
        "--extractor",
        choices=["eccodes", "gribjump"],
        default="eccodes",
        help="Select how fields are extracted",
        nargs="?",
    )

    simulate_training_parser2 = sub_parsers.add_parser(
        "simulate-training-anemoi-dataset",
        help="Simulates data access similar to anemoi training",
    )
    simulate_training_parser2.set_defaults(func=simulate_training_cmd2)
    simulate_training_parser2.add_argument(
        "zstore",
        help="path to .zarr store",
        type=Path,
    )

    dump_zarr_parser = sub_parsers.add_parser(
        "dump-zarr", help="Copy zfdb view into zarr store"
    )
    dump_zarr_parser.set_defaults(func=dump_zarr_cmd)
    dump_zarr_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    dump_zarr_parser.add_argument(
        "-e",
        "--extractor",
        choices=["eccodes", "gribjump"],
        default="eccodes",
        help="Select how fields are extracted",
        nargs="?",
    )
    dump_zarr_parser.add_argument(
        "-o",
        "--out",
        help="Output path",
        type=Path,
        nargs="?",
        default=Path("dump.zarr"),
    )
    dump_zarr_parser.add_argument(
        "recipe",
        help="path to anemoi like recipe.yaml describing the training data",
        type=Path,
    )

    troughput_test_parser = sub_parsers.add_parser(
        "throughput-test",
        help="Create  view into fdb and dump it to disk, then compare read troughput",
    )
    troughput_test_parser.set_defaults(func=throughput_test_cmd)
    troughput_test_parser.add_argument(
        "recipe",
        help="path to anemoi like recipe.yaml describing the training data",
        type=Path,
    )
    troughput_test_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )

    return parser.parse_args()


def initialize_logger():
    global logger
    logger = logging.getLogger(__name__)
    logger.info("Begin")


def main():
    args = parse_cli_args()
    if not args.verbose:
        be_quiet_stdout_and_stderr()
    logging.basicConfig(
        format="%(asctime)s %(message)s", stream=sys.stdout, level=logging.INFO
    )
    initialize_logger()
    args.func(args)


if __name__ == "__main__":
    main()
