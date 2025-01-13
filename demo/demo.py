#! /usr/bin/env python

import argparse
import math
import os
import shutil
import sys
import time
from dataclasses import KW_ONLY, dataclass
from pathlib import Path

import numpy as np
import pyfdb
import pygrib
import pygribjump
import tqdm
import yaml
import zarr

import zfdb


def be_quiet_stdout_and_stderr():
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
        return f"{val_in_ns/10**(exp*3)}{units[exp]}"
    raise ValueError


@dataclass(frozen=True)
class ExampleDataSet:
    _: KW_ONLY
    name: str
    grib_file: Path
    recipe: Path
    anemoi_dataset: Path | None = None


def make_example_datasets() -> list[ExampleDataSet]:
    data_location = Path(__file__).parent / "demo-data"

    return [
        ExampleDataSet(
            name="aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january",
            grib_file=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.grib",
            recipe=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml",
            anemoi_dataset=data_location
            / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.zarr",
        ),
    ]


def setup_system_database():
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"
    return pyfdb.FDB(), pygribjump.GribJump()


def setup_database(
    datasets: list[ExampleDataSet], path: Path = Path.cwd()
) -> tuple[pyfdb.FDB, pygribjump.GribJump]:
    print("Setting up database")
    db_store_path = path / "db_store"
    if db_store_path.exists():
        shutil.rmtree(db_store_path)
    db_store_path.mkdir(parents=True, exist_ok=True)
    schema_path = path / "schema"
    shutil.copy(Path(__file__).parent / "demo-data" / "schema", schema_path)
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
    os.environ["FDB5_CONFIG_FILE"] = str(fdb_config_path)

    gj_config = {"plugin": {"select": "class=(ea),stream=(enfo|oper),expver=(00..)"}}
    gj_config_path = path / "gj_config.yaml"
    gj_config_path.write_text(yaml.dump(gj_config))
    os.environ["GRIBJUMP_CONFIG_FILE"] = str(gj_config_path)
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"
    os.environ["FDB_ENABLE_GRIBJUMP"] = "1"

    fdb = pyfdb.FDB()
    for dataset in datasets:
        print(f"Archiving {dataset.grib_file.name}")
        grib_file = pygrib.open(dataset.grib_file)
        for idx, msg in enumerate(tqdm.tqdm(grib_file)):
            fdb.archive(msg.tostring())
            if (idx + 1) % 256 == 0:
                fdb.flush()
        fdb.flush()
    print("Setting up database - Finished")
    return fdb, pygribjump.GribJump()


def create_callgraph(
    fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: ExampleDataSet
):
    print("Creating profile_stats.txt with python profiling information.")
    fdb_view = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(dataset.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
        )
    )
    import cProfile

    with cProfile.Profile() as pr:
        [x for x in fdb_view["data"]]
        pr.dump_stats("profile_stats.txt")


def demo_performance_comparison(
    fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: ExampleDataSet
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
        )
    )

    print("Opening matching anemoi dataset")
    anemoi = zarr.open_group(dataset.anemoi_dataset, mode="r")

    def time_compute_field_sum_full_time_range(stores, idx, iterations):
        timings = [[] for _ in stores]
        for _ in tqdm.tqdm(range(iterations)):
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
    fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: ExampleDataSet
) -> None:
    print(f"Running aggregation example on {dataset.name}")
    print("Opening fdb view")
    fdb_view = zarr.open_group(
        zfdb.make_anemoi_dataset_like_view(
            recipe=yaml.safe_load(dataset.recipe.read_text()),
            fdb=fdb,
            gribjump=gribjump,
        )
    )
    variable_names = fdb_view["data"].attrs["variables"]
    means_per_sample = []
    print(
        f"Computing means for {len(variable_names)} variables on {fdb_view['data'].shape[0]} dates with {fdb_view['data'].shape[3]} values per field"
    )
    for sample in tqdm.tqdm(fdb_view["data"]):
        means_per_variable = np.mean(sample, axis=2).squeeze()
        means_per_sample.append(means_per_variable)
    means = np.mean(means_per_sample, axis=0)
    print("Means for each vaiable:")
    print("\n".join([f"\t{name} = {val}" for name, val in zip(variable_names, means)]))


def main(args):
    print("Begin Demo")
    datasets = make_example_datasets()
    if args.use_system_fdb:
        fdb, gribjump = setup_system_database()
    else:
        fdb, gribjump = setup_database(datasets)
    for dataset in datasets:
        if dataset.anemoi_dataset:
            # create_callgraph(fdb, gribjump, dataset)
            # demo_performance_comparison(fdb, gribjump, dataset)
            pass
        demo_aggeration(fdb, gribjump, dataset)


def parse_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", help="Enables verbose output", action="store_true"
    )
    parser.add_argument(
        "--use-system-fdb",
        help="Use the fdb instance provided by the system. No test data will be imported in this case.",
        action="store_true",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()
    if not args.verbose:
        be_quiet_stdout_and_stderr()
    main(args)
