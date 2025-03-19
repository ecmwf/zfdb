from pathlib import Path
from types import SimpleNamespace

from demo.demo import initialize_logger, simulate_training_cmd


def test_demo_eccodes():
    initialize_logger()

    simulate_training_cmd(
        SimpleNamespace(
            {
                "recipe": Path(
                    "data/aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml"
                ),
                "database": Path("demo"),
                "extractor": "eccodes",
                "progress": True,
            }
        )
    )


def test_demo_gribjump():
    initialize_logger()

    simulate_training_cmd(
        SimpleNamespace(
            {
                "recipe": Path(
                    "data/aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml"
                ),
                "database": Path("demo"),
                "extractor": "gribjump",
                "progress": True,
            }
        )
    )
