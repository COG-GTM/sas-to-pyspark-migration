"""
Parity tests for pyspark/03_aggregation_reporting.py.

Each PySpark report is checked against an independent pure-Python oracle
that applies the SAS semantics of sas/03_aggregation_reporting.sas
(missing CLASS levels excluded, PROC FREQ percents over non-missing rows,
QNTLDEF=5 median, VARDEF=DF std, PROC SQL outobs=10).
"""

import csv
import importlib.util
import math
import os
import statistics
import unittest
from collections import defaultdict

from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "pyspark", "03_aggregation_reporting.py")
NUMERIC = {"BAD", "LOAN", "MORTDUE", "VALUE", "DEBTINC"}


def loadScript():
    spec = importlib.util.spec_from_file_location("aggregation_reporting", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def oracleFinalRows():
    """work.home_equity_final computed with plain Python."""
    rows = []
    with open(DATA_PATH, newline="", encoding="utf-8-sig") as f:
        for raw in csv.DictReader(f):
            r = {k: (None if v == "" else v) for k, v in raw.items()}
            for k in NUMERIC:
                r[k] = None if r[k] is None else float(r[k])
            if r["LOAN"] is None or r["VALUE"] is None or r["BAD"] is None:
                continue
            ltv = r["MORTDUE"] / r["VALUE"] if r["MORTDUE"] is not None and r["VALUE"] > 0 else None
            if ltv is None or not (0 < ltv < 5) or r["LOAN"] <= 0 or r["VALUE"] <= 0:
                continue
            r["LTV"] = ltv
            r["LOAN_OUTCOME"] = "Paid" if r["BAD"] == 0 else "Default"
            rows.append(r)
    return rows


def groupBy(rows, keys):
    groups = defaultdict(list)
    for r in rows:
        k = tuple(r[c] for c in keys)
        if all(v is not None for v in k):
            groups[k].append(r)
    return groups


def values(rows, var):
    return [r[var] for r in rows if r[var] is not None]


class TestAggregationReportingParity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("HomeEquity_03_Tests") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        cls.mod = loadScript()
        cls.df = cls.mod.buildHomeEquityFinal(cls.spark, DATA_PATH).cache()
        cls.rows = oracleFinalRows()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def assertClose(self, actual, expected, places=6):
        if expected is None:
            self.assertIsNone(actual)
        else:
            self.assertTrue(math.isclose(actual, expected, rel_tol=10 ** -places, abs_tol=1e-9),
                            f"{actual} != {expected}")

    def test_final_dataset_row_count(self):
        self.assertEqual(self.df.count(), len(self.rows))
        self.assertEqual(self.df.count(), 5337)

    def test_step1_frequency_tables(self):
        for column in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
            table, missing = self.mod.frequencyTable(self.df, column)
            nonMissing = [r[column] for r in self.rows if r[column] is not None]
            self.assertEqual(missing, len(self.rows) - len(nonMissing), column)
            actual = {r[column]: (r["Frequency"], r["Percent"]) for r in table.collect()}
            expectedCounts = defaultdict(int)
            for v in nonMissing:
                expectedCounts[v] += 1
            self.assertEqual(set(actual), set(expectedCounts), column)
            for level, n in expectedCounts.items():
                self.assertEqual(actual[level][0], n)
                self.assertAlmostEqual(actual[level][1], round(n * 100 / len(nonMissing), 2), places=2)

    def checkClassSummary(self, classVars, analysisVars, stats):
        result = self.mod.classSummary(self.df, classVars, analysisVars, stats).collect()
        expected = groupBy(self.rows, classVars)
        self.assertEqual(len(result), len(expected))
        fns = {"N": len, "Mean": statistics.fmean, "Median": statistics.median,
               "Std": statistics.stdev, "Min": min, "Max": max}
        for row in result:
            group = expected[tuple(row[c] for c in classVars)]
            self.assertEqual(row["N_Obs"], len(group))
            for var in analysisVars:
                vals = values(group, var)
                for stat in stats:
                    self.assertClose(float(row[f"{var}_{stat}"]), float(fns[stat](vals)))

    def test_step2_summary_by_loan_outcome(self):
        self.checkClassSummary(["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
                               ["N", "Mean", "Median", "Std", "Min", "Max"])

    def test_step3_default_rate_tabulate(self):
        result = {r["JOB"]: r for r in self.mod.defaultRateTabulate(self.df).collect()}
        base = [r for r in self.rows if r["JOB"] is not None and r["REGION"] is not None]
        regions = sorted({r["REGION"] for r in base})
        jobs = sorted({r["JOB"] for r in base})
        self.assertEqual(list(result), jobs + ["Total"])
        for job in jobs + ["Total"]:
            jobRows = base if job == "Total" else [r for r in base if r["JOB"] == job]
            for region in regions:
                cell = [r["BAD"] for r in jobRows if r["REGION"] == region]
                self.assertEqual(result[job][f"{region}_N"], len(cell))
                self.assertClose(result[job][f"{region}_Mean"], statistics.fmean(cell))
            self.assertEqual(result[job]["Total_N"], len(jobRows))
            self.assertClose(result[job]["Total_Mean"], statistics.fmean([r["BAD"] for r in jobRows]))

    def test_step4_top_states(self):
        result = self.mod.topStatesByAvgLoan(self.spark, self.df).collect()
        groups = defaultdict(list)
        for r in self.rows:
            groups[r["STATE"]].append(r)
        expected = sorted(
            ((s, len(g), statistics.fmean(values(g, "LOAN")), statistics.fmean(values(g, "VALUE")),
              statistics.fmean(values(g, "BAD"))) for s, g in groups.items() if len(g) >= 10),
            key=lambda t: -t[2],
        )[:10]
        self.assertEqual(len(result), 10)
        for row, (state, n, avgLoan, avgValue, rate) in zip(result, expected):
            self.assertEqual(row["STATE"], state)
            self.assertEqual(row["num_loans"], n)
            self.assertClose(row["avg_loan"], avgLoan)
            self.assertClose(row["avg_property_value"], avgValue)
            self.assertClose(row["default_rate"], rate)

    def test_step5_summary_by_reason_and_outcome(self):
        self.checkClassSummary(["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"],
                               ["N", "Mean", "Std", "Median"])


if __name__ == "__main__":
    unittest.main()
