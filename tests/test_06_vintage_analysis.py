"""
Test Suite: test_06_vintage_analysis.py
Purpose: Validate pyspark/06_vintage_analysis.py against the SAS semantics
of sas/06_vintage_analysis.sas.

Expected values are computed independently in pure Python from
data/home_equity.csv (mirroring the sequential SAS DATA step), so the tests
check the Spark output rather than re-implementing it with Spark.
"""

import csv
import importlib.util
import os
import unittest
from collections import defaultdict

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TEST_DIR)
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "pyspark", "06_vintage_analysis.py")


def load_script_module():
    """Module name starts with a digit, so import it by path."""
    spec = importlib.util.spec_from_file_location("vintage_analysis", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def to_num(value):
    return float(value) if value not in ("", None) else None


def sas_cohort(yoj):
    """Mirror of the SAS IF/ELSE IF ladder (missing YOJ treated as 0)."""
    yoj = 0.0 if yoj is None else yoj
    if yoj < 2:
        return "V0-2"
    if yoj < 5:
        return "V2-5"
    if yoj < 10:
        return "V5-10"
    return "V10+"


def expected_from_csv():
    """
    Replays the SAS program row by row on the raw CSV:
      - work.he2 derivations (COHORT, LTV)
      - PROC SORT by COHORT LOAN (stable) + DATA step RETAIN / first. / last.
      - PROC SQL cohort summary and the %delinq_sweep tables
    """
    with open(DATA_PATH, encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    he2 = []
    for idx, r in enumerate(rows):
        value = to_num(r["VALUE"])
        mortdue = to_num(r["MORTDUE"])
        ltv = None if value in (None, 0) or mortdue is None else mortdue / value
        he2.append({
            "_ROW_ID": idx,
            "COHORT": sas_cohort(to_num(r["YOJ"])),
            "LOAN": float(r["LOAN"]),
            "BAD": float(r["BAD"]),
            "LTV": ltv,
            "DEBTINC": to_num(r["DEBTINC"]),
            "DELINQ": to_num(r["DELINQ"]),
        })

    # proc sort data=work.he2; by COHORT LOAN;  (stable on ties)
    he2.sort(key=lambda r: (r["COHORT"], r["LOAN"], r["_ROW_ID"]))

    # data work.he3 ... retain CUM_LOAN 0 SEQ 0; by COHORT;
    he3_last = {}
    running = []
    cum_loan, seq, prev = 0.0, 0, None
    for i, r in enumerate(he2):
        first = r["COHORT"] != prev
        last = i + 1 == len(he2) or he2[i + 1]["COHORT"] != r["COHORT"]
        if first:
            cum_loan, seq = 0.0, 0
        cum_loan += r["LOAN"]
        seq += 1
        running.append((r["COHORT"], r["LOAN"], r["_ROW_ID"], cum_loan, seq, int(first), int(last)))
        if last:
            he3_last[r["COHORT"]] = {"LOAN": r["LOAN"], "CUM_LOAN": cum_loan, "SEQ": seq,
                                     "IS_FIRST": int(first), "IS_LAST": int(last)}
        prev = r["COHORT"]

    # proc sql cohort_summary
    grouped = defaultdict(list)
    for r in he2:
        grouped[r["COHORT"]].append(r)

    def mean_nonmissing(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    summary = {}
    for cohort, rs in grouped.items():
        n = len(rs)
        n_bad = sum(r["BAD"] for r in rs)
        summary[cohort] = {
            "N_LOANS": n,
            "N_BAD": n_bad,
            "DEFAULT_RATE": n_bad / n,
            "AVG_LOAN": mean_nonmissing([r["LOAN"] for r in rs]),
            "AVG_LTV": mean_nonmissing([r["LTV"] for r in rs]),
            "AVG_DTI": mean_nonmissing([r["DEBTINC"] for r in rs]),
        }

    # %delinq_sweep: where not missing(DELINQ)
    sweep = {}
    for thr in (1, 2, 3):
        sweep[thr] = {}
        for cohort, rs in grouped.items():
            rs_delinq = [r for r in rs if r["DELINQ"] is not None]
            n = len(rs_delinq)
            n_over = sum(1 for r in rs_delinq if r["DELINQ"] >= thr)
            sweep[thr][cohort] = {"N_OVER": n_over, "N": n, "PCT_OVER": n_over / n}

    return {"he2": he2, "running": running, "he3_last": he3_last, "summary": summary, "sweep": sweep}


class TestVintageAnalysis(unittest.TestCase):
    """Parity tests for the vintage / cohort report migration."""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("HomeEquity_VintageAnalysis_Tests") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        cls.va = load_script_module()
        cls.expected = expected_from_csv()

        dfRaw = cls.spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        cls.dfHe2 = cls.va.derive_cohorts(dfRaw).cache()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------
    # work.he2: cohort derivation and clean-up
    # ------------------------------------------------------------------
    def test_row_count_per_cohort(self):
        """Row counts per cohort match a row-by-row replay of the SAS DATA step."""
        actual = {r["COHORT"]: r["count"] for r in self.dfHe2.groupBy("COHORT").count().collect()}
        expected = {c: s["N_LOANS"] for c, s in self.expected["summary"].items()}
        self.assertEqual(actual, expected)
        self.assertEqual(sum(actual.values()), 5960, "every input row must land in exactly one cohort")
        self.assertEqual(set(actual), {"V0-2", "V2-5", "V5-10", "V10+"})

    def test_missing_yoj_falls_into_first_cohort(self):
        """`if YOJ = . then YOJ = 0` means missing YOJ rows are all V0-2."""
        dfRaw = self.spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        nMissingYoj = dfRaw.filter(col("YOJ").isNull()).count()
        self.assertGreater(nMissingYoj, 0, "fixture should contain missing YOJ")
        self.assertEqual(self.dfHe2.filter(col("YOJ").isNull()).count(), 0)
        # Rows with YOJ == 0 in he2 must include every originally-missing row
        self.assertGreaterEqual(self.dfHe2.filter(col("YOJ") == 0).count(), nMissingYoj)
        self.assertEqual(self.dfHe2.filter((col("YOJ") == 0) & (col("COHORT") != "V0-2")).count(), 0)

    def test_reason_clean(self):
        """missing(REASON) -> 'UNKNOWN', otherwise upcase(REASON)."""
        reasons = {r["REASON_CLEAN"] for r in self.dfHe2.select("REASON_CLEAN").distinct().collect()}
        self.assertEqual(reasons, {"UNKNOWN", "DEBTCON", "HOMEIMP"})
        self.assertEqual(
            self.dfHe2.filter(col("REASON").isNull()).count(),
            self.dfHe2.filter(col("REASON_CLEAN") == "UNKNOWN").count(),
        )

    def test_ltv_missing_when_value_missing_or_zero(self):
        """`if VALUE in (., 0) then LTV = .` and SAS missing propagation."""
        bad = self.dfHe2.filter(
            (col("VALUE").isNull() | (col("VALUE") == 0) | col("MORTDUE").isNull()) & col("LTV").isNotNull()
        ).count()
        self.assertEqual(bad, 0)
        # LTV never becomes Infinity/NaN (Spark's default for x / 0)
        self.assertEqual(self.dfHe2.filter(col("LTV").isNull() == False).filter(
            (col("LTV") == float("inf")) | col("LTV").isNaN()).count(), 0)
        # Where computable, LTV equals MORTDUE / VALUE
        sample = self.dfHe2.filter((col("MORTDUE") == 25860) & (col("VALUE") == 39025)).first()
        self.assertAlmostEqual(sample["LTV"], 25860 / 39025, places=6)

    # ------------------------------------------------------------------
    # work.he3: RETAIN running total, first./last. flags, OUTPUT on last.
    # ------------------------------------------------------------------
    def test_retain_running_total_every_row(self):
        """CUM_LOAN / SEQ on every row equal the sequential RETAIN replay."""
        actual = sorted(
            (r["COHORT"], r["LOAN"], r["CUM_LOAN"], r["SEQ"])
            for r in self.va.running_exposure(self.dfHe2)
            .select("COHORT", "LOAN", "CUM_LOAN", "SEQ").collect()
        )
        expected = sorted((c, loan, cum, seq) for c, loan, _, cum, seq, _, _ in self.expected["running"])
        self.assertEqual(len(actual), 5960)
        self.assertEqual(actual, expected)

    def test_last_row_per_cohort_carries_cohort_total(self):
        """work.he3 has one row per cohort and CUM_LOAN == sum(LOAN) for the cohort."""
        he3 = {r["COHORT"]: r for r in self.va.cohort_last_rows(self.dfHe2).collect()}
        self.assertEqual(set(he3), set(self.expected["he3_last"]))
        for cohort, exp in self.expected["he3_last"].items():
            with self.subTest(cohort=cohort):
                self.assertAlmostEqual(he3[cohort]["CUM_LOAN"], exp["CUM_LOAN"], places=6)
                self.assertEqual(he3[cohort]["SEQ"], exp["SEQ"])
                self.assertEqual(he3[cohort]["LOAN"], exp["LOAN"], "last row must be the max LOAN in cohort")
                self.assertEqual(he3[cohort]["IS_LAST"], 1)
                self.assertEqual(he3[cohort]["IS_FIRST"], 0, "multi-row cohorts: last row is not also first")

        # Cross-check against the independent cohort_summary aggregate
        summary = {r["COHORT"]: r for r in self.va.cohort_summary(self.dfHe2).collect()}
        for cohort in he3:
            self.assertEqual(he3[cohort]["SEQ"], summary[cohort]["N_LOANS"])

    def test_first_last_flags_exactly_one_per_cohort(self):
        """first.COHORT / last.COHORT are set on exactly one row per cohort."""
        flags = self.va.running_exposure(self.dfHe2).groupBy("COHORT").agg(
            {"IS_FIRST": "sum", "IS_LAST": "sum"}
        ).collect()
        self.assertEqual(len(flags), 4)
        for r in flags:
            self.assertEqual(r["sum(IS_FIRST)"], 1, r["COHORT"])
            self.assertEqual(r["sum(IS_LAST)"], 1, r["COHORT"])

    def test_first_last_logic_on_synthetic_by_groups(self):
        """
        Edge cases of BY-group processing:
          - single-row group -> first. and last. both 1 on the same row
          - ties on the sort key accumulate one row at a time (stable order)
          - CUM_LOAN resets at each new BY group
        """
        df = self.spark.createDataFrame([
            Row(COHORT="A", LOAN=100.0),
            Row(COHORT="B", LOAN=50.0),
            Row(COHORT="B", LOAN=50.0),
            Row(COHORT="B", LOAN=20.0),
        ])
        rows = self.va.running_exposure(df).orderBy("COHORT", "SEQ").collect()
        got = [(r["COHORT"], r["LOAN"], r["CUM_LOAN"], r["SEQ"], r["IS_FIRST"], r["IS_LAST"]) for r in rows]
        self.assertEqual(got, [
            ("A", 100.0, 100.0, 1, 1, 1),
            ("B", 20.0, 20.0, 1, 1, 0),
            ("B", 50.0, 70.0, 2, 0, 0),
            ("B", 50.0, 120.0, 3, 0, 1),
        ])
        last = self.va.cohort_last_rows(df).orderBy("COHORT").collect()
        self.assertEqual([(r["COHORT"], r["CUM_LOAN"], r["SEQ"]) for r in last],
                         [("A", 100.0, 1), ("B", 120.0, 3)])

    # ------------------------------------------------------------------
    # work.cohort_summary
    # ------------------------------------------------------------------
    def test_cohort_summary(self):
        """PROC SQL aggregates match the pure-Python replay (nulls excluded from means)."""
        rows = self.va.cohort_summary(self.dfHe2).collect()
        self.assertEqual([r["COHORT"] for r in rows], ["V0-2", "V10+", "V2-5", "V5-10"],
                         "ORDER BY COHORT is lexical")
        for r in rows:
            exp = self.expected["summary"][r["COHORT"]]
            with self.subTest(cohort=r["COHORT"]):
                self.assertEqual(r["N_LOANS"], exp["N_LOANS"])
                self.assertEqual(r["N_BAD"], exp["N_BAD"])
                self.assertAlmostEqual(r["DEFAULT_RATE"], exp["DEFAULT_RATE"], places=9)
                self.assertAlmostEqual(r["AVG_LOAN"], exp["AVG_LOAN"], places=6)
                self.assertAlmostEqual(r["AVG_LTV"], exp["AVG_LTV"], places=9)
                self.assertAlmostEqual(r["AVG_DTI"], exp["AVG_DTI"], places=9)

    # ------------------------------------------------------------------
    # %delinq_sweep macro loop + data work.delinq_all
    # ------------------------------------------------------------------
    def test_macro_loop_produces_one_table_per_threshold(self):
        """%do i = 1 %to countw(&thresholds) -> one table per threshold, named like SAS."""
        tables = self.va.delinq_sweep(self.dfHe2)
        self.assertEqual(list(tables), ["delinq_ge_1", "delinq_ge_2", "delinq_ge_3"])
        for thr, name in zip((1, 2, 3), tables):
            rows = tables[name].collect()
            with self.subTest(table=name):
                self.assertEqual(len(rows), 4)
                self.assertTrue(all(r["THRESHOLD"] == thr for r in rows))
                for r in rows:
                    exp = self.expected["sweep"][thr][r["COHORT"]]
                    self.assertEqual(r["N_OVER"], exp["N_OVER"])
                    self.assertEqual(r["N"], exp["N"])
                    self.assertAlmostEqual(r["PCT_OVER"], exp["PCT_OVER"], places=9)

    def test_sweep_excludes_missing_delinq(self):
        """`where not missing(DELINQ)`: N per cohort is the non-missing DELINQ count."""
        table = self.va.delinq_over_threshold(self.dfHe2, 1)
        n_by_cohort = {r["COHORT"]: r["N"] for r in table.collect()}
        expected = {
            r["COHORT"]: r["count"]
            for r in self.dfHe2.filter(col("DELINQ").isNotNull()).groupBy("COHORT").count().collect()
        }
        self.assertEqual(n_by_cohort, expected)
        self.assertLess(sum(n_by_cohort.values()), 5960)

    def test_sweep_is_monotone_in_threshold(self):
        """N_OVER at threshold t+1 can never exceed N_OVER at threshold t."""
        tables = self.va.delinq_sweep(self.dfHe2)
        by_thr = {
            thr: {r["COHORT"]: r["N_OVER"] for r in tables[f"delinq_ge_{thr}"].collect()}
            for thr in (1, 2, 3)
        }
        for cohort in by_thr[1]:
            self.assertGreaterEqual(by_thr[1][cohort], by_thr[2][cohort])
            self.assertGreaterEqual(by_thr[2][cohort], by_thr[3][cohort])

    def test_delinq_all_stacks_all_sweep_tables(self):
        """data work.delinq_all; set delinq_ge_1 delinq_ge_2 delinq_ge_3;"""
        tables = self.va.delinq_sweep(self.dfHe2)
        stacked = self.va.stack_sweep(tables)
        self.assertEqual(stacked.count(), 12)
        self.assertEqual(stacked.columns, ["COHORT", "THRESHOLD", "N_OVER", "N", "PCT_OVER"])
        self.assertEqual(
            {(r["THRESHOLD"], r["COHORT"]) for r in stacked.collect()},
            {(t, c) for t in (1, 2, 3) for c in ("V0-2", "V2-5", "V5-10", "V10+")},
        )


if __name__ == "__main__":
    unittest.main()
