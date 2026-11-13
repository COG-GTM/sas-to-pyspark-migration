"""
Parity tests for pyspark/04_risk_segmentation.py.

The expected values come from a plain-Python, row-by-row reimplementation of
the SAS DATA steps in sas/02_data_cleaning.sas (home_equity_final) and
sas/04_risk_segmentation.sas (home_equity_risk), using SAS missing-value
semantics. The Spark outputs must match it exactly.
"""

import csv
import importlib.util
import math
import os
from collections import Counter, defaultdict

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "pyspark", "04_risk_segmentation.py")


def _load_script():
    spec = importlib.util.spec_from_file_location("risk_segmentation_04", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rs = _load_script()


# ------------------------------------------------------------------
# SAS DATA step oracle
# ------------------------------------------------------------------
def _num(v):
    return float(v) if v not in ("", None) else None


def _sas_risk_row(r):
    ltv, dti, delinq, derog = r["LTV"], r["DEBTINC"], r["DELINQ"], r["DEROG"]

    ltvCat = dtiCat = delinqCat = None
    if ltv is not None:
        ltvCat = "Low" if ltv < 0.60 else "Medium" if ltv < 0.80 else "High"
    if dti is not None:
        dtiCat = ("Low" if dti < 30 else "Medium" if dti < 40
                  else "High" if dti < 50 else "Very High")
    if delinq is not None:
        delinqCat = ("None" if delinq == 0 else "Low" if delinq == 1
                     else "Medium" if delinq <= 3 else "High")

    score = 0.0
    if ltv is not None:
        if ltv >= 0.80:
            score += 3
        elif ltv >= 0.60:
            score += 1.5
    if dti is not None:
        if dti >= 50:
            score += 3
        elif dti >= 40:
            score += 2
        elif dti >= 30:
            score += 1
    if delinq is not None:
        if delinq >= 4:
            score += 2
        elif delinq >= 2:
            score += 1.5
        elif delinq == 1:
            score += 0.5
    if derog is not None:
        if derog >= 3:
            score += 2
        elif derog >= 1:
            score += 1

    segment = ("Low Risk" if score < 3 else "Medium Risk" if score < 5
               else "High Risk" if score < 7 else "Very High Risk")
    return dict(r, LTV_RISK_CAT=ltvCat, DTI_RISK_CAT=dtiCat,
                DELINQ_RISK_CAT=delinqCat, RISK_SCORE=score, RISK_SEGMENT=segment)


@pytest.fixture(scope="module")
def expected_rows():
    rows = []
    with open(DATA_PATH, newline="", encoding="utf-8-sig") as fh:
        for raw in csv.DictReader(fh):
            r = {k: _num(raw[k]) for k in
                 ("BAD", "LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG")}
            v, m = r["VALUE"], r["MORTDUE"]
            r["LTV"] = m / v if (v is not None and m is not None and v > 0) else None
            # 02 Step 3 (filtered) + Step 5 (final); SAS missing < any number
            if r["LOAN"] is None or r["VALUE"] is None or r["BAD"] is None:
                continue
            if r["LTV"] is None or not (0 < r["LTV"] < 5):
                continue
            if not (r["LOAN"] > 0 and r["VALUE"] > 0):
                continue
            rows.append(_sas_risk_row(r))
    return rows


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("HomeEquity_RiskSegmentation_Tests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def df_risk(spark):
    return rs.add_risk_segments(rs.load_home_equity_final(spark, DATA_PATH)).cache()


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------
def test_home_equity_final_row_count(df_risk, expected_rows):
    assert df_risk.count() == len(expected_rows) == 5337


def test_risk_columns_present(df_risk):
    for c in rs.RISK_LABELS:
        assert c in df_risk.columns


def test_risk_score_range_and_steps(df_risk):
    scores = {r["RISK_SCORE"] for r in df_risk.select("RISK_SCORE").distinct().collect()}
    assert min(scores) >= 0 and max(scores) <= 10
    assert all((s * 2).is_integer() for s in scores)


@pytest.mark.parametrize(
    "var", ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]
)
def test_proc_freq_matches_sas(df_risk, expected_rows, var):
    expected = Counter(r[var] for r in expected_rows if r[var] is not None)
    actual = {r[var]: r["Frequency"] for r in rs.one_way_freq(df_risk, var).collect()}
    assert actual == dict(expected)

    total = sum(expected.values())
    pct = {r[var]: r["Percent"] for r in rs.one_way_freq(df_risk, var).collect()}
    for k, n in expected.items():
        assert pct[k] == pytest.approx(round(n * 100.0 / total, 2))


def test_frequency_missing_matches_sas(df_risk, expected_rows):
    for var in ["LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        expected = sum(1 for r in expected_rows if r[var] is None)
        assert df_risk.filter(df_risk[var].isNull()).count() == expected


def test_proc_means_by_segment_matches_sas(df_risk, expected_rows):
    groups = defaultdict(list)
    for r in expected_rows:
        groups[r["RISK_SEGMENT"]].append(r)

    actual = {r["RISK_SEGMENT"]: r for r in rs.means_by_segment(df_risk).collect()}
    assert set(actual) == set(groups)
    for seg, rows in groups.items():
        assert actual[seg]["N_Obs"] == len(rows)
        for v in rs.MEANS_VARS:
            vals = [r[v] for r in rows if r[v] is not None]
            n = len(vals)
            mean = sum(vals) / n
            std = math.sqrt(sum((x - mean) ** 2 for x in vals) / (n - 1))
            assert actual[seg][f"{v}_N"] == n
            assert actual[seg][f"{v}_Mean"] == pytest.approx(mean, abs=1e-4)
            assert actual[seg][f"{v}_Std"] == pytest.approx(std, abs=1e-4)


def test_ltv_dti_bad_crosstab_matches_sas(df_risk, expected_rows):
    expected = Counter(
        (r["LTV_RISK_CAT"], r["DTI_RISK_CAT"], int(r["BAD"]))
        for r in expected_rows
        if r["LTV_RISK_CAT"] is not None and r["DTI_RISK_CAT"] is not None
    )
    rows = rs.ltv_dti_crosstab(df_risk).collect()
    for row in rows:
        key = (row["LTV_RISK_CAT"], row["DTI_RISK_CAT"])
        assert row["BAD_0"] == expected[key + (0,)]
        assert row["BAD_1"] == expected[key + (1,)]
        assert row["Total"] == row["BAD_0"] + row["BAD_1"]
    assert sum(r["Total"] for r in rows) == sum(expected.values())


def test_format_boundaries(spark):
    df = spark.createDataFrame(
        [(0.5999, 29.99, 0.0, 0.0), (0.60, 30.0, 1.0, 1.0), (0.80, 50.0, 4.0, 3.0),
         (None, None, None, None)],
        "LTV double, DEBTINC double, DELINQ double, DEROG double",
    )
    out = rs.add_risk_segments(df).collect()
    assert [r["LTV_RISK_CAT"] for r in out] == ["Low", "Medium", "High", None]
    assert [r["DTI_RISK_CAT"] for r in out] == ["Low", "Medium", "Very High", None]
    assert [r["DELINQ_RISK_CAT"] for r in out] == ["None", "Low", "High", None]
    assert [r["RISK_SCORE"] for r in out] == [0.0, 4.0, 10.0, 0.0]
    assert [r["RISK_SEGMENT"] for r in out] == [
        "Low Risk", "Medium Risk", "Very High Risk", "Low Risk"]
