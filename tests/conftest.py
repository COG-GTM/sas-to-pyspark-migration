"""Shared pytest fixtures and helpers for the SAS-to-PySpark parity suite.

This module centralizes the SparkSession/boilerplate that was previously
duplicated across every ``pyspark/*.py`` stage script and in the test file's
``setUpClass``. Later per-stage migration PRs (and the current tests) reuse:

* the session-scoped ``spark`` fixture,
* the ``home_equity_df`` fixture (loads ``data/home_equity.csv`` once),
* :func:`get_data_path` to resolve data files relative to the project root,
* :func:`assert_frames_equal` for order-insensitive DataFrame equality, and
* :func:`load_stage_module` to import a ``pyspark/NN_*.py`` script as a module
  (used by later phases without shadowing the installed ``pyspark`` library).
"""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession

# Project layout: this file lives in <root>/tests/conftest.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STAGE_DIR = PROJECT_ROOT / "pyspark"


def get_data_path(filename: str = "home_equity.csv") -> str:
    """Return the absolute path to a data file, relative to the project root."""
    return str(DATA_DIR / filename)


def create_spark_session(app_name: str = "HomeEquity_Tests") -> SparkSession:
    """Build a local SparkSession configured for fast, deterministic tests."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_home_equity(spark: SparkSession) -> DataFrame:
    """Load the raw home_equity dataset (header + inferred schema)."""
    return spark.read.csv(get_data_path(), header=True, inferSchema=True)


def assert_frames_equal(
    actual: DataFrame,
    expected: DataFrame,
    ignore_row_order: bool = True,
    ignore_column_order: bool = True,
) -> None:
    """Assert two DataFrames are equal, ignoring row/column order by default."""
    assert_df_equality(
        actual,
        expected,
        ignore_row_order=ignore_row_order,
        ignore_column_order=ignore_column_order,
    )


def load_stage_module(stage_filename: str) -> ModuleType:
    """Import a ``pyspark/NN_*.py`` stage script as a module by file path.

    The stage directory is named ``pyspark``, which collides with the installed
    ``pyspark`` library, and the filenames start with digits. Both make a normal
    ``import`` impossible, so we load the file directly under a safe module name.
    """
    path = STAGE_DIR / stage_filename
    module_name = "stage_" + path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load stage module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession shared across the whole test run."""
    session = create_spark_session()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def home_equity_df(spark: SparkSession) -> DataFrame:
    """Session-scoped raw home_equity DataFrame, loaded once."""
    return load_home_equity(spark)
