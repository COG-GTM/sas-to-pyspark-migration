"""
Parity test: run the semantics-preserving PySpark conversion of
sas/03_aggregation_reporting.sas against data/home_equity.csv and diff every
step's output against the expected SAS output in tests/expected_sas_output/.

Numeric columns are compared with a small absolute tolerance because Spark
sums partitions in a different order than a single-threaded SAS data pass.
"""

import csv
import math
import os
import sys
import unittest

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TEST_DIR)
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession  # noqa: E402

from parity import sas_03_aggregation_reporting as sas03  # noqa: E402

EXPECTED_DIR = os.path.join(TEST_DIR, "expected_sas_output")
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

# step name -> (fixture file, key columns identifying a row)
FIXTURES = {
    "01_freq_tables": ("03_step1_freq_tables.csv", ["VARIABLE", "VALUE"]),
    "02_means_by_outcome": ("03_step2_means_by_outcome.csv", ["LOAN_OUTCOME", "VARIABLE"]),
    "03_tabulate_default_rates": ("03_step3_tabulate_default_rates.csv", ["JOB", "REGION"]),
    "04_sql_top_states": ("03_step4_sql_top_states.csv", ["STATE"]),
    "05_means_by_reason_outcome": (
        "03_step5_means_by_reason_outcome.csv",
        ["REASON", "LOAN_OUTCOME", "VARIABLE"],
    ),
}

ABS_TOL = 1e-6


def read_expected(name):
    with open(os.path.join(EXPECTED_DIR, name), newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = [[None if v == "" else v for v in row] for row in reader]
    return header, rows


def normalize(value):
    """Coerce Spark/CSV cell values to a comparable form."""
    if value is None:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) else value
    if isinstance(value, int):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def cells_equal(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=0.0, abs_tol=ABS_TOL)
    return a == b


def diff_tables(header, expected_rows, actual_rows, key_cols):
    """Return a list of human-readable differences (empty means parity)."""
    key_idx = [header.index(k) for k in key_cols]

    def key(row):
        return tuple(row[i] for i in key_idx)

    expected = {key(r): r for r in expected_rows}
    actual = {key(r): r for r in actual_rows}
    problems = []

    for k in expected.keys() - actual.keys():
        problems.append(f"missing in PySpark output: {dict(zip(key_cols, k))}")
    for k in actual.keys() - expected.keys():
        problems.append(f"unexpected in PySpark output: {dict(zip(key_cols, k))}")
    for k in expected.keys() & actual.keys():
        for col, e, a in zip(header, expected[k], actual[k]):
            if not cells_equal(e, a):
                problems.append(f"{dict(zip(key_cols, k))} {col}: SAS={e!r} PySpark={a!r}")

    if [key(r) for r in expected_rows] != [key(r) for r in actual_rows]:
        problems.append("row order differs from SAS output")
    return problems


class TestParity03AggregationReporting(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Parity_03_AggregationReporting")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.results = sas03.run_all(cls.spark, DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def assert_step_matches(self, step):
        fixture, key_cols = FIXTURES[step]
        header, expected_rows = read_expected(fixture)
        expected_rows = [[normalize(v) for v in r] for r in expected_rows]

        df = self.results[step]
        self.assertEqual(df.columns, header, f"{step}: column layout differs from SAS output")
        actual_rows = [[normalize(v) for v in row] for row in df.collect()]

        problems = diff_tables(header, expected_rows, actual_rows, key_cols)
        self.assertEqual(
            problems, [],
            f"{step}: PySpark output differs from expected SAS output:\n  " + "\n  ".join(problems),
        )
        self.assertEqual(len(actual_rows), len(expected_rows))

    def test_home_equity_final_row_count(self):
        """DATA step chain from 02 must yield the same observation count as SAS."""
        final = sas03.build_home_equity_final(sas03.load_home_equity(self.spark, DATA_PATH))
        with open(os.path.join(EXPECTED_DIR, "03_home_equity_final_nobs.txt")) as fh:
            expected_nobs = int(fh.read().strip())
        self.assertEqual(final.count(), expected_nobs)

    def test_step1_proc_freq(self):
        self.assert_step_matches("01_freq_tables")

    def test_step2_proc_means_by_outcome(self):
        self.assert_step_matches("02_means_by_outcome")

    def test_step3_proc_tabulate(self):
        self.assert_step_matches("03_tabulate_default_rates")

    def test_step4_proc_sql_top_states(self):
        self.assert_step_matches("04_sql_top_states")

    def test_step5_proc_means_by_reason_outcome(self):
        self.assert_step_matches("05_means_by_reason_outcome")

    def test_sas_missing_comparison_semantics(self):
        """`. < 5` is true and `. > 0` is false in SAS; the helpers must agree."""
        from pyspark.sql import Row
        from pyspark.sql import functions as F

        df = self.spark.createDataFrame([Row(x=None), Row(x=-1.0), Row(x=2.0), Row(x=7.0)])
        lt = sorted(r.x for r in df.filter(sas03.sas_lt(F.col("x"), 5)).collect() if r.x is not None)
        self.assertEqual(df.filter(sas03.sas_lt(F.col("x"), 5)).count(), 3)  # None, -1, 2
        self.assertEqual(lt, [-1.0, 2.0])
        self.assertEqual(df.filter(sas03.sas_gt(F.col("x"), 0)).count(), 2)  # 2, 7

    def test_sas_propcase_delimiters(self):
        self.assertEqual(sas03.sas_propcase("winston-salem"), "Winston-Salem")
        self.assertEqual(sas03.sas_propcase("o'fallon/st. louis"), "O'fallon/St. Louis")
        self.assertIsNone(sas03.sas_propcase(None))


if __name__ == "__main__":
    unittest.main()
