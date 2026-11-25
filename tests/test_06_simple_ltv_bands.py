"""
Parity test for sas/06_simple_ltv_bands.sas -> pyspark/06_simple_ltv_bands.py.

SAS-expected output (expected/06_ltv_band_summary_sas_expected.csv) is produced by
tests/sas_reference.py, a plain-Python translation of the SAS program using SAS
missing-value semantics; no SAS runtime was used.
"""

import csv
import importlib.util
import os
import sys
import tempfile
import unittest

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sas_reference import EXPECTED_PATH, ltv_band_rows, ltv_band_summary, row_key  # noqa: E402

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "pyspark", "06_simple_ltv_bands.py")

_spec = importlib.util.spec_from_file_location("simple_ltv_bands", SCRIPT_PATH)
simple_ltv_bands = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(simple_ltv_bands)


class TestSimpleLtvBandsParity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("HomeEquity_SimpleLtvBands_Tests") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        df = cls.spark.read.csv(simple_ltv_bands.DATA_PATH, header=True, inferSchema=True)
        cls.bands = simple_ltv_bands.build_ltv_bands(df)
        cls.summary = [r.asDict() for r in simple_ltv_bands.summarize_ltv_bands(cls.bands).collect()]
        with open(EXPECTED_PATH, newline="") as f:
            cls.expected = list(csv.DictReader(f))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_expected_file_matches_reference(self):
        """Committed expected CSV is reproducible from the SAS reference logic."""
        regenerated = ltv_band_summary(ltv_band_rows())
        self.assertEqual([r["LTV_BAND"] for r in regenerated], [r["LTV_BAND"] for r in self.expected])
        for got, exp in zip(regenerated, self.expected):
            self.assertEqual(got["N_LOANS"], int(exp["N_LOANS"]))
            self.assertAlmostEqual(got["AVG_LOAN"], float(exp["AVG_LOAN"]), places=9)
            self.assertAlmostEqual(got["DEFAULT_RATE"], float(exp["DEFAULT_RATE"]), places=9)

    def test_row_level_parity(self):
        """Every banded input row matches the SAS reference (count, LTV, band)."""
        reference = sorted(ltv_band_rows(), key=row_key)
        spark_rows = sorted((r.asDict() for r in self.bands.collect()), key=row_key)
        self.assertEqual(len(spark_rows), len(reference))
        for got, exp in zip(spark_rows, reference):
            for column in ("BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "LTV_BAND"):
                self.assertEqual(got[column], exp[column], column)
            if exp["LTV"] is None:
                self.assertIsNone(got["LTV"])
            else:
                self.assertAlmostEqual(got["LTV"], exp["LTV"], places=12)

    def test_filter_and_band_boundaries(self):
        """Missing LOAN/VALUE and VALUE <= 0 are dropped; LTV 0.8 is MEDIUM and 1.0 is HIGH."""
        cases = [
            ("1", "", "100", "200", "dropped"),       # LOAN missing
            ("1", "1000", "100", "", "dropped"),      # VALUE missing
            ("1", "1000", "100", "0", "dropped"),     # VALUE = 0
            ("1", "1000", "100", "-5", "dropped"),    # VALUE < 0
            ("0", "1000", "", "200", "UNKNOWN"),      # MORTDUE missing
            ("0", "1000", "159", "200", "LOW"),       # 0.795
            ("0", "1000", "160", "200", "MEDIUM"),    # exactly 0.8
            ("0", "1000", "199", "200", "MEDIUM"),    # 0.995
            ("0", "1000", "200", "200", "HIGH"),      # exactly 1.0
        ]
        expected = sorted(((float(c[2]) if c[2] else None, c[4]) for c in cases if c[4] != "dropped"),
                          key=lambda pair: (pair[0] is not None, pair[0] or 0))
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cases.csv")
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["BAD", "LOAN", "MORTDUE", "VALUE", "REASON"])
                writer.writerows([c[:4] + ("HomeImp",) for c in cases])
            df = self.spark.read.csv(path, header=True, inferSchema=True)
            spark_bands = [(r["MORTDUE"], r["LTV_BAND"]) for r in sorted(
                (r.asDict() for r in simple_ltv_bands.build_ltv_bands(df).collect()), key=row_key)]
            reference_bands = [(r["MORTDUE"], r["LTV_BAND"]) for r in sorted(ltv_band_rows(path), key=row_key)]
        self.assertEqual(spark_bands, expected)
        self.assertEqual(reference_bands, expected)

    def test_summary_parity(self):
        """PySpark PROC MEANS replacement matches the SAS-expected summary."""
        self.assertEqual([r["LTV_BAND"] for r in self.summary], [r["LTV_BAND"] for r in self.expected])
        for got, exp in zip(self.summary, self.expected):
            self.assertEqual(got["N_LOANS"], int(exp["N_LOANS"]))
            self.assertAlmostEqual(got["AVG_LOAN"], float(exp["AVG_LOAN"]), places=9)
            self.assertAlmostEqual(got["DEFAULT_RATE"], float(exp["DEFAULT_RATE"]), places=9)

    def test_exported_csv_matches_expected(self):
        """The CSV written by the PySpark script matches the SAS-expected CSV."""
        bands = simple_ltv_bands.summarize_ltv_bands(self.bands)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "summary.csv")
            simple_ltv_bands.write_summary_csv(bands, path)
            with open(path, newline="") as f:
                exported = list(csv.DictReader(f))
        self.assertEqual(list(exported[0].keys()), list(self.expected[0].keys()))
        self.assertEqual([r["LTV_BAND"] for r in exported], [r["LTV_BAND"] for r in self.expected])
        for got, exp in zip(exported, self.expected):
            self.assertEqual(int(got["N_LOANS"]), int(exp["N_LOANS"]))
            self.assertAlmostEqual(float(got["AVG_LOAN"]), float(exp["AVG_LOAN"]), places=9)
            self.assertAlmostEqual(float(got["DEFAULT_RATE"]), float(exp["DEFAULT_RATE"]), places=9)

    def test_band_counts_add_up(self):
        """Summary N_LOANS totals the filtered row count."""
        self.assertEqual(sum(r["N_LOANS"] for r in self.summary), self.bands.count())


if __name__ == "__main__":
    unittest.main()
