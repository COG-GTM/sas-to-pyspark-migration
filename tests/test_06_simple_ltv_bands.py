"""
Parity test for sas/06_simple_ltv_bands.sas -> pyspark/06_simple_ltv_bands.py.

SAS-expected output (expected/06_ltv_band_summary_sas_expected.csv) is produced by
tests/sas_reference.py, a plain-Python translation of the SAS program using SAS
missing-value semantics; no SAS runtime was used.
"""

import csv
import importlib.util
import os
import tempfile
import unittest

from pyspark.sql import SparkSession

from sas_reference import EXPECTED_PATH, ltv_band_rows, ltv_band_summary

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
        reference = ltv_band_rows()
        spark_rows = [r.asDict() for r in self.bands.collect()]
        self.assertEqual(len(spark_rows), len(reference))
        for got, exp in zip(spark_rows, reference):
            for column in ("BAD", "LOAN", "MORTDUE", "VALUE", "LTV_BAND"):
                self.assertEqual(got[column], exp[column], column)
            if exp["LTV"] is None:
                self.assertIsNone(got["LTV"])
            else:
                self.assertAlmostEqual(got["LTV"], exp["LTV"], places=12)

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
