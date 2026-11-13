"""
Parity tests for pyspark/02_data_cleaning.py against an independent
pure-Python implementation of sas/02_data_cleaning.sas.
"""

import csv
import importlib.util
import math
import os
import statistics
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "pyspark", "02_data_cleaning.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("data_cleaning_02", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _num(value):
    return float(value) if value not in ("", None) else None


def _sas_pctl(values, p):
    """SAS PCTLDEF=5 reference implementation."""
    xs = sorted(values)
    n = len(xs)
    j = n * p
    if j == int(j):
        j = int(j)
        return (xs[max(j, 1) - 1] + xs[min(j + 1, n) - 1]) / 2
    return xs[math.ceil(j) - 1]


class TestDataCleaning02(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("HomeEquity_DataCleaning_Tests") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        cls.mod = _load_module()
        cls.out = cls.mod.run(cls.spark, DATA_PATH, verbose=False)

        with open(DATA_PATH, encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
        for r in rows:
            for k in ["BAD", "LOAN", "MORTDUE", "VALUE", "DEBTINC", "CLAGE", "DEROG", "DELINQ"]:
                r[k] = _num(r[k])
            v, m = r["VALUE"], r["MORTDUE"]
            r["LTV"] = m / v if v is not None and m is not None and v > 0 else None
        cls.raw = rows
        cls.filtered = [r for r in rows
                        if r["LOAN"] is not None and r["VALUE"] is not None and r["BAD"] is not None]
        cls.final = [r for r in cls.filtered
                     if r["LTV"] is not None and 0 < r["LTV"] < 5 and r["LOAN"] > 0 and r["VALUE"] > 0]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_row_counts_per_step(self):
        self.assertEqual(self.out["home_equity"].count(), len(self.raw))
        self.assertEqual(self.out["home_equity_filtered"].count(), len(self.filtered))
        self.assertEqual(self.out["home_equity_final"].count(), len(self.final))

    def test_derived_columns(self):
        clean = self.out["home_equity_clean"]
        self.assertEqual(clean.filter(F.col("LTV").isNotNull()).count(),
                         sum(r["LTV"] is not None for r in self.raw))
        outcomes = {r["LOAN_OUTCOME"]: r["count"]
                    for r in clean.groupBy("LOAN_OUTCOME").count().collect()}
        self.assertEqual(outcomes.get("Paid"), sum(r["BAD"] == 0 for r in self.raw))
        self.assertEqual(outcomes.get("Default"), sum(r["BAD"] == 1 for r in self.raw))
        self.assertEqual(clean.schema["LTV"].metadata.get("label"), "Loan to Value Ratio")

    def test_propcase_matches_sas_delimiters(self):
        df = self.spark.createDataFrame(
            [("winston-salem",), ("ST. LOUIS",), ("new york",), (None,)], "CITY string")
        got = [r[0] for r in df.select(self.mod.propcase(F.col("CITY"))).collect()]
        self.assertEqual(got, ["Winston-Salem", "St. Louis", "New York", None])

    def test_missing_flags(self):
        imputed = self.out["home_equity_imputed"]
        for v in self.mod.IMPUTE_VARS:
            flagged = imputed.filter(F.col(f"{v}_MISS") == 1).count()
            self.assertEqual(flagged, imputed.filter(F.col(v).isNull()).count(), v)
            self.assertEqual(
                imputed.filter(~F.col(f"{v}_MISS").isin(0, 1)).count(), 0, v)

    def test_outlier_proc_means_parity(self):
        stats = {r["Variable"]: r for r in self.out["outlier_stats"].collect()}
        for v in self.mod.OUTLIER_VARS:
            vals = [r[v] for r in self.filtered if r[v] is not None]
            s = stats[v]
            self.assertEqual(s["N"], len(vals), v)
            self.assertEqual(s["NMiss"], len(self.filtered) - len(vals), v)
            self.assertAlmostEqual(s["Mean"], statistics.mean(vals), places=6)
            self.assertAlmostEqual(s["StdDev"], statistics.stdev(vals), places=6)
            self.assertAlmostEqual(s["Min"], min(vals), places=9)
            self.assertAlmostEqual(s["Max"], max(vals), places=9)
            for name, p in self.mod.PERCENTILES:
                self.assertAlmostEqual(s[name], _sas_pctl(vals, p), places=9,
                                       msg=f"{v} {name}")

    def test_final_dataset_constraints(self):
        final = self.out["home_equity_final"]
        bad = final.filter(
            (F.col("LTV") <= 0) | (F.col("LTV") >= 5) | F.col("LTV").isNull() |
            (F.col("LOAN") <= 0) | (F.col("VALUE") <= 0))
        self.assertEqual(bad.count(), 0)
        stats = {r["Variable"]: r for r in self.out["final_stats"].collect()}
        for v in self.mod.SUMMARY_VARS:
            vals = [r[v] for r in self.final if r[v] is not None]
            self.assertEqual(stats[v]["N"], len(vals), v)
            self.assertAlmostEqual(stats[v]["Mean"], statistics.mean(vals), places=6)


if __name__ == "__main__":
    unittest.main()
