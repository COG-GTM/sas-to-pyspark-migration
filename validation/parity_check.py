"""
Independent parity check for the SAS -> PySpark migration.

The PySpark scripts under pyspark/ are executed as black boxes (or their saved
terminal logs are read with --logs) and the numbers they PRINT are parsed back.
Every number is then recomputed a second way, with pandas / scikit-learn
straight off data/home_equity.csv, following the semantics of the SAS programs
under sas/ (not the PySpark code). The two sides are compared and a
metric / independent / PySpark / match table is printed.

Usage (from the repo root):
    python validation/parity_check.py                 # runs pyspark/01..05 itself
    python validation/parity_check.py --logs validation/logs   # reads 0N_*.txt logs
    python validation/parity_check.py --out validation/parity_results.md
"""

import argparse
import glob
import os
import re
import subprocess
import sys
from decimal import ROUND_HALF_UP, Decimal

import numpy as np
import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(REPO_ROOT, "data", "home_equity.csv")
SCRIPTS = {
    "01": "pyspark/01_data_loading.py",
    "02": "pyspark/02_data_cleaning.py",
    "03": "pyspark/03_aggregation_reporting.py",
    "04": "pyspark/04_risk_segmentation.py",
    "05": "pyspark/05_logistic_regression.py",
}

# Documented HMEQ facts, used as a sanity anchor for the raw load.
HMEQ_ROWS = 5960
HMEQ_BAD_RATE = 0.1995


# ----------------------------------------------------------------------------
# Output parsing helpers (DataFrame.show() tables and printed scalars)
# ----------------------------------------------------------------------------
def find_line(lines, marker, start=0):
    for i in range(start, len(lines)):
        if marker in lines[i]:
            return i
    raise ValueError(f"marker not found in script output: {marker!r}")


def parse_show_table(lines, start, nth=1):
    """Parse the nth DataFrame.show() table at or after `start`.

    Relies on the ASCII layout DataFrame.show() has used since Spark 1.x
    (+---+ borders, |-separated cells, NULL for missing). A table that is
    absent or truncated (e.g. the script died mid-run) surfaces as a
    ValueError naming the marker rather than a bare IndexError.
    """
    try:
        return _parse_show_table(lines, start, nth)
    except IndexError:
        raise ValueError(
            f"DataFrame.show() table #{nth} not found after output line {start}: "
            f"{lines[start].strip()!r}"
        ) from None


def _parse_show_table(lines, start, nth):
    i = start
    for _ in range(nth - 1):
        while not lines[i].startswith("+-"):
            i += 1
        i += 3
        while not lines[i].startswith("+-"):
            i += 1
        i += 1
    while not lines[i].startswith("+-"):
        i += 1
    header = [c.strip() for c in lines[i + 1].strip().strip("|").split("|")]
    i += 3  # skip header row and its separator
    rows = []
    while not lines[i].startswith("+-"):
        cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
        rows.append(dict(zip(header, cells)))
        i += 1
    return rows


def parse_scalar(text, pattern, cast=float):
    m = re.search(pattern, text)
    if not m:
        raise ValueError(f"pattern not found in script output: {pattern!r}")
    return cast(m.group(1))


def to_num(s):
    # DataFrame.show() renders SQL NULL as the literal string NULL
    if s is None or s in ("NULL", ""):
        return None
    return float(s)


def key_of(s):
    return None if s == "NULL" else s


def rnd(x, places):
    """Round half away from zero, like SAS ROUND() and Spark SQL ROUND()."""
    q = Decimal(1).scaleb(-places)
    return float(Decimal(repr(float(x))).quantize(q, rounding=ROUND_HALF_UP))


def load_script_output(tag, logs_dir):
    if logs_dir:
        matches = glob.glob(os.path.join(logs_dir, f"{tag}_*.txt"))
        if not matches:
            raise FileNotFoundError(f"no log for script {tag} in {logs_dir}")
        with open(matches[0], encoding="utf-8", errors="replace") as f:
            return f.read()
    print(f"  running {SCRIPTS[tag]} ...", file=sys.stderr)
    proc = subprocess.run(
        [sys.executable, SCRIPTS[tag]], cwd=REPO_ROOT,
        capture_output=True, text=True, check=True,
    )
    return proc.stdout


# ----------------------------------------------------------------------------
# Comparison bookkeeping
# ----------------------------------------------------------------------------
class Report:
    """Two kinds of checks are kept apart so the headline total is honest:

    * parity checks   - one side is parsed from the output of the pyspark/
                        script under test, the other is pandas / SAS semantics;
    * supplementary   - neither side is script output (dataset sanity anchors,
                        Spark recomputations of numbers the scripts do not
                        print, internal-consistency checks). Reported
                        separately and never counted in the parity total.
    """

    def __init__(self):
        self.rows = []
        self.sections = []
        self._supplementary = False

    def section(self, title, supplementary=False):
        self.sections.append((title, len(self.rows), supplementary))
        self._supplementary = supplementary

    def fmt(self, v):
        if v is None:
            return "null"
        if isinstance(v, bool):
            return str(v)
        if isinstance(v, float):
            return f"{v:,.4f}".rstrip("0").rstrip(".") if abs(v) < 1e6 else f"{v:,.2f}"
        return f"{v:,}" if isinstance(v, int) else str(v)

    def check(self, metric, independent, pyspark, tol=0.0, note=""):
        if independent is None or pyspark is None:
            ok = independent is None and pyspark is None
        elif isinstance(independent, str) or isinstance(pyspark, str):
            ok = str(independent) == str(pyspark)
        else:
            ok = abs(float(independent) - float(pyspark)) <= tol
        self.rows.append((metric, self.fmt(independent), self.fmt(pyspark), ok, note, self._supplementary))
        return ok

    def parity_rows(self):
        return [r for r in self.rows if not r[5]]

    def supplementary_rows(self):
        return [r for r in self.rows if r[5]]

    def render(self):
        out = []
        bounds = [s[1] for s in self.sections] + [len(self.rows)]
        for (title, start, supplementary), end in zip(self.sections, bounds[1:]):
            out.append(f"\n### {title}\n")
            if supplementary:
                out.append("_Supplementary: neither column is pyspark/ script output; "
                           "not counted in the parity total._\n")
                out.append("| Metric | Independent (pandas / SAS semantics) | Comparison value | Match |")
            else:
                out.append("| Metric | Independent (pandas / SAS semantics) | PySpark (script output) | Match |")
            out.append("|---|---|---|---|")
            for metric, a, b, ok, note, _ in self.rows[start:end]:
                flag = "yes" if ok else "**NO**"
                if note:
                    flag += f" ({note})"
                out.append(f"| {metric} | {a} | {b} | {flag} |")
        par, sup = self.parity_rows(), self.supplementary_rows()
        out.append(f"\n**{sum(1 for r in par if r[3])} / {len(par)} script-parity checks matched** "
                   f"(one side parsed from pyspark/ script output).")
        out.append(f"\n{sum(1 for r in sup if r[3])} / {len(sup)} supplementary checks matched "
                   f"(dataset sanity / Spark recomputation / internal consistency; not script output).")
        return "\n".join(out)


# ----------------------------------------------------------------------------
# Independent (pandas) implementation of the SAS programs
# ----------------------------------------------------------------------------
def build_frames():
    raw = pd.read_csv(DATA_PATH, encoding="utf-8-sig")

    # sas/02: DATA step derived columns
    clean = raw.copy()
    ltv_ok = clean["VALUE"].notna() & clean["MORTDUE"].notna() & (clean["VALUE"] > 0)
    clean["LTV"] = np.where(ltv_ok, clean["MORTDUE"] / clean["VALUE"], np.nan)
    clean["LOAN_OUTCOME"] = clean["BAD"].map({0: "Paid", 1: "Default"})

    # sas/02: work.home_equity_filtered
    filtered = clean[clean["LOAN"].notna() & clean["VALUE"].notna() & clean["BAD"].notna()]

    # sas/02: work.home_equity_final  (missing LTV fails "LTV > 0" in SAS too)
    final = filtered[
        (filtered["LTV"] > 0) & (filtered["LTV"] < 5)
        & (filtered["LOAN"] > 0) & (filtered["VALUE"] > 0)
    ].copy()

    # sas/04: risk categories and composite score
    ltv, dti, dq, dg = final["LTV"], final["DEBTINC"], final["DELINQ"], final["DEROG"]
    final["LTV_RISK_CAT"] = np.select(
        [ltv.isna(), ltv < 0.60, ltv < 0.80], [None, "Low", "Medium"], "High")
    final["DTI_RISK_CAT"] = np.select(
        [dti.isna(), dti < 30, dti < 40, dti < 50], [None, "Low", "Medium", "High"], "Very High")
    final["DELINQ_RISK_CAT"] = np.select(
        [dq.isna(), dq == 0, dq == 1, dq <= 3], [None, "None", "Low", "Medium"], "High")
    score = (
        np.select([ltv.isna(), ltv >= 0.80, ltv >= 0.60], [0, 3, 1.5], 0)
        + np.select([dti.isna(), dti >= 50, dti >= 40, dti >= 30], [0, 3, 2, 1], 0)
        + np.select([dq.isna(), dq >= 4, dq >= 2, dq == 1], [0, 2, 1.5, 0.5], 0)
        + np.select([dg.isna(), dg >= 3, dg >= 1], [0, 2, 1], 0)
    )
    final["RISK_SCORE"] = score
    final["RISK_SEGMENT"] = np.select(
        [score < 3, score < 5, score < 7], ["Low Risk", "Medium Risk", "High Risk"], "Very High Risk")
    return raw, filtered, final


def sklearn_auc(final):
    """Plain scikit-learn logistic regression on the same features as pyspark/05.

    Mirrors the SAS order of operations: the DATA step keeps rows complete on
    the six key predictors, PROC SURVEYSELECT draws an (unstratified) 70% SRS
    from that population, and only then does PROC LOGISTIC drop rows with a
    missing DEROG / NINQ / JOB / REASON from the training set (scored
    validation rows with a missing predictor get a missing pred_prob and fall
    out of the confusion matrix the same way).
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
    cat = ["JOB", "REASON"]
    key_predictors = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]
    model_data = final.dropna(subset=key_predictors)          # SAS work.model_data
    train, valid = train_test_split(model_data, test_size=0.3, random_state=42)  # SRS, unstratified
    train = train.dropna(subset=numeric + cat)                # PROC LOGISTIC complete cases
    valid = valid.dropna(subset=numeric + cat)                # scored rows with pred_prob
    model = model_data.dropna(subset=numeric + cat)
    X, y = model[numeric + cat], model["BAD"].astype(int)

    pipe = Pipeline([
        ("prep", ColumnTransformer([
            ("num", StandardScaler(), numeric),
            # ref levels match SAS: JOB(ref='Other') REASON(ref='HomeImp')
            ("cat", OneHotEncoder(drop=np.array(["Other", "HomeImp"], dtype=object)), cat),
        ])),
        ("lr", LogisticRegression(C=1e6, max_iter=5000)),  # ~unpenalised MLE, like PROC LOGISTIC
    ])
    pipe.fit(train[numeric + cat], train["BAD"].astype(int))
    holdout_auc = roc_auc_score(valid["BAD"].astype(int), pipe.predict_proba(valid[numeric + cat])[:, 1])
    cv = cross_val_score(pipe, X, y, cv=StratifiedKFold(5, shuffle=True, random_state=42),
                         scoring="roc_auc")
    return len(model_data), len(model), holdout_auc, cv.mean(), cv.std()


def spark_group_stats(final_index_filter):
    """Spark recomputation of mean/median LOAN & DEBTINC by REASON and by JOB.

    pyspark/03 does not print single-factor REASON / JOB summaries, so they are
    computed here with the same load + filter the scripts use, then compared
    against pandas.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, mean, median, when

    spark = SparkSession.builder.appName("parity_check").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(os.path.join(REPO_ROOT, "data/home_equity.csv"), header=True, inferSchema=True)
    df = df.withColumn(
        "LTV", when(col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
                    col("MORTDUE") / col("VALUE"))
    ).filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
        & (col("LOAN") > 0) & (col("VALUE") > 0) & (col("LTV") > 0) & (col("LTV") < 5)
    )
    out = {}
    for g in ["REASON", "JOB"]:
        rows = df.groupBy(g).agg(
            count("*").alias("N"),
            mean("LOAN").alias("mean_LOAN"), median("LOAN").alias("median_LOAN"),
            mean("DEBTINC").alias("mean_DEBTINC"), median("DEBTINC").alias("median_DEBTINC"),
        ).collect()
        out[g] = {r[g]: r.asDict() for r in rows}
    spark.stop()
    return out


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", help="directory of saved script logs (0N_*.txt) instead of re-running")
    ap.add_argument("--out", help="write the markdown report to this file as well")
    ap.add_argument("--skip-spark-groups", action="store_true",
                    help="skip the in-script Spark recomputation of REASON/JOB group stats")
    args = ap.parse_args()

    rep = Report()
    raw, filtered, final = build_frames()
    out = {tag: load_script_output(tag, args.logs) for tag in SCRIPTS}
    L = {tag: txt.splitlines() for tag, txt in out.items()}

    # ---------------- 01 data loading ----------------
    rep.section("01 - Data loading")
    rep.check("Row count (HMEQ documented: 5,960)", len(raw),
              parse_scalar(out["01"], r"Number of rows: (\d+)", int))
    rep.check("Column count", raw.shape[1], parse_scalar(out["01"], r"Number of columns: (\d+)", int))
    sample = parse_show_table(L["01"], find_line(L["01"], "First 20 Observations"))
    rep.check("PROC PRINT obs=20 sample: rows printed", 20, len(sample))
    for i, r in enumerate(sample):
        rep.check(f"obs {i + 1}: BAD / LOAN / MORTDUE / VALUE as loaded",
                  f"{int(raw.iloc[i]['BAD'])} / {int(raw.iloc[i]['LOAN'])} / "
                  f"{raw.iloc[i]['MORTDUE']} / {raw.iloc[i]['VALUE']}",
                  f"{int(r['BAD'])} / {int(r['LOAN'])} / "
                  f"{to_num(r['MORTDUE']) if to_num(r['MORTDUE']) is not None else float('nan')} / "
                  f"{to_num(r['VALUE']) if to_num(r['VALUE']) is not None else float('nan')}")

    rep.section("Dataset sanity anchors (pandas vs documented HMEQ facts; no script prints the raw BAD rate)",
                supplementary=True)
    bad_rate = raw["BAD"].mean()
    rep.check("Raw row count vs HMEQ documented 5,960", len(raw), HMEQ_ROWS)
    rep.check("Raw BAD default rate % vs HMEQ documented ~19.95%", round(bad_rate * 100, 2),
              round(HMEQ_BAD_RATE * 100, 2), tol=0.01)
    assert len(raw) == HMEQ_ROWS and abs(bad_rate - HMEQ_BAD_RATE) < 0.0005, "raw CSV is not HMEQ"

    # ---------------- 02 data cleaning ----------------
    rep.section("02 - Data cleaning (row base and outlier filter)")
    rep.check("Original rows", len(raw), parse_scalar(out["02"], r"Original rows: (\d+)", int))
    rep.check("After LOAN/VALUE/BAD not-missing filter", len(filtered),
              parse_scalar(out["02"], r"After filtering: (\d+)", int))
    rep.check("Final clean rows (0 < LTV < 5, LOAN > 0, VALUE > 0) = SAS work.home_equity_final",
              len(final), parse_scalar(out["02"], r"Final clean rows: (\d+)", int))
    desc = parse_show_table(L["02"], find_line(L["02"], "Final clean rows"))
    desc = {r["summary"]: r for r in desc}
    for c in ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]:
        s = final[c]
        rep.check(f"{c} n (non-missing)", int(s.count()), int(float(desc["count"][c])))
        rep.check(f"{c} mean", round(float(s.mean()), 4), round(to_num(desc["mean"][c]), 4), tol=1e-4)
        rep.check(f"{c} std (sample)", round(float(s.std(ddof=1)), 4), round(to_num(desc["stddev"][c]), 4), tol=1e-4)
        rep.check(f"{c} min", float(s.min()), to_num(desc["min"][c]), tol=1e-9)
        rep.check(f"{c} max", float(s.max()), to_num(desc["max"][c]), tol=1e-9)
    # LTV derived column: recompute MORTDUE / VALUE for every row the script prints
    shown = parse_show_table(L["02"], find_line(L["02"], "Final clean rows"), nth=2)
    rep.check("LTV sample table: rows printed (show(10))", 10, len(shown))
    for r in shown:
        mortdue, value = to_num(r["MORTDUE"]), to_num(r["VALUE"])
        rep.check(f"LTV = MORTDUE / VALUE for MORTDUE={mortdue:g}, VALUE={value:g}",
                  round(mortdue / value, 6), round(to_num(r["LTV"]), 6), tol=1e-6)

    # ---------------- 03 aggregation ----------------
    rep.section("03 - Frequency tables (SAS PROC FREQ on work.home_equity_final; "
                "missing excluded from table and percent base, reported as Frequency Missing)")
    for c in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        start = find_line(L["03"], f"--- {c} ---")
        rows = parse_show_table(L["03"], start)
        tbl = {key_of(r[c]): r for r in rows}
        vc = final[c].value_counts(dropna=True)
        n_nonmissing = int(vc.sum())
        for k, v in vc.items():
            r = tbl.get(k)
            rep.check(f"{c} = {k}: frequency", int(v), None if r is None else int(r["Frequency"]))
            rep.check(f"{c} = {k}: percent", rnd(v / n_nonmissing * 100, 2),
                      None if r is None else to_num(r["Percent"]), tol=0.001)
        rep.check(f"{c}: rows in table (missing excluded)", len(vc), len(rows))
        m = re.search(r"Frequency Missing = (\d+)", "\n".join(L["03"][start:start + 60]))
        rep.check(f"{c}: Frequency Missing", int(final[c].isna().sum()), int(m.group(1)) if m else None)

    rep.section("03 - Summary statistics by LOAN_OUTCOME (PROC MEANS n mean median; var LOAN MORTDUE VALUE DEBTINC)")
    tbl = {r["LOAN_OUTCOME"]: r for r in
           parse_show_table(L["03"], find_line(L["03"], "Summary Statistics by Loan Outcome"))}
    for k, g in final.groupby("LOAN_OUTCOME"):
        r = tbl[k]
        rep.check(f"{k}: N", len(g), int(r["N"]))
        for v in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
            rep.check(f"{k}: mean {v}", rnd(g[v].mean(), 2), to_num(r[f"Mean_{v}"]), tol=0.001)
            rep.check(f"{k}: median {v}", rnd(g[v].median(), 2), to_num(r.get(f"Median_{v}")), tol=0.001)
        rep.check(f"{k}: std LOAN", rnd(g["LOAN"].std(ddof=1), 2), to_num(r["Std_LOAN"]), tol=0.001)
        rep.check(f"{k}: min / max LOAN", f"{g['LOAN'].min():g} / {g['LOAN'].max():g}",
                  f"{to_num(r['Min_LOAN']):g} / {to_num(r['Max_LOAN']):g}")

    rep.section("03 - LOAN / LTV / DEBTINC by REASON x LOAN_OUTCOME "
                "(PROC MEANS class REASON LOAN_OUTCOME; missing REASON excluded by CLASS)")
    rows = parse_show_table(L["03"], find_line(L["03"], "Loan Distribution by Reason and Outcome"))
    tbl = {(key_of(r["REASON"]), r["LOAN_OUTCOME"]): r for r in rows}
    groups = final.groupby(["REASON", "LOAN_OUTCOME"])
    rep.check("Class groups in table (missing REASON excluded)", groups.ngroups, len(rows))
    rep.check("Rows with missing REASON dropped by CLASS", int(final["REASON"].isna().sum()),
              len(final) - sum(int(r["N"]) for r in rows))
    for (reason, outcome), g in groups:
        r = tbl.get((reason, outcome), {})
        label = f"{reason}/{outcome}"
        rep.check(f"{label}: N", len(g), to_num(r.get("N")))
        rep.check(f"{label}: mean LOAN", rnd(g["LOAN"].mean(), 2), to_num(r.get("Mean_LOAN")), tol=0.001)
        rep.check(f"{label}: median LOAN", rnd(g["LOAN"].median(), 2), to_num(r.get("Median_LOAN")), tol=0.001)
        rep.check(f"{label}: std LOAN", rnd(g["LOAN"].std(ddof=1), 2), to_num(r.get("Std_LOAN")), tol=0.001)
        rep.check(f"{label}: mean LTV", rnd(g["LTV"].mean(), 4), to_num(r.get("Mean_LTV")), tol=0.00001)
        rep.check(f"{label}: median LTV", rnd(g["LTV"].median(), 4), to_num(r.get("Median_LTV")), tol=0.00001)
        rep.check(f"{label}: mean DEBTINC", rnd(g["DEBTINC"].mean(), 2), to_num(r.get("Mean_DEBTINC")), tol=0.001)
        rep.check(f"{label}: median DEBTINC", rnd(g["DEBTINC"].median(), 2), to_num(r.get("Median_DEBTINC")),
                  tol=0.001)

    rep.section("03 - Default rate by JOB x REGION (PROC TABULATE class JOB REGION; missing JOB excluded by CLASS)")
    rows = parse_show_table(L["03"], find_line(L["03"], "Cross-tabulation: Default Rate by JOB x REGION"), nth=2)
    tbl = {(key_of(r["JOB"]), r["REGION"]): r for r in rows}
    groups = final.groupby(["JOB", "REGION"])
    rep.check("Class groups in table (missing JOB excluded)", groups.ngroups, len(rows))
    rep.check("Rows with missing JOB dropped by CLASS", int(final["JOB"].isna().sum()),
              len(final) - sum(int(r["N"]) for r in rows))
    for (job, region), g in groups:
        r = tbl.get((job, region), {})
        label = f"{job} / {region}"
        rep.check(f"{label}: N", len(g), to_num(r.get("N")))
        rep.check(f"{label}: default %", rnd(g["BAD"].mean() * 100, 2), to_num(r.get("Default_Rate_Pct")),
                  tol=0.001)

    rep.section("03 - Top 10 states by average loan (PROC SQL)")
    tbl = parse_show_table(L["03"], find_line(L["03"], "Top 10 States by Average Loan Amount"))
    st = final.groupby("STATE").agg(num_loans=("LOAN", "size"), avg_loan=("LOAN", "mean"),
                                    avg_value=("VALUE", "mean"), default_rate=("BAD", "mean"))
    st = st[st["num_loans"] >= 10].sort_values("avg_loan", ascending=False).head(10)
    rep.check("Top-10 state order", " > ".join(st.index), " > ".join(r["STATE"] for r in tbl))
    for (state, s), r in zip(st.iterrows(), tbl):
        rep.check(f"{state}: n / avg LOAN / avg VALUE / default %",
                  f"{int(s.num_loans)} / {s.avg_loan:.2f} / {s.avg_value:.2f} / {s.default_rate*100:.2f}",
                  f"{int(r['num_loans'])} / {to_num(r['avg_loan']):.2f} / "
                  f"{to_num(r['avg_property_value']):.2f} / {to_num(r['default_rate_pct']):.2f}")

    # ---------------- 04 risk segmentation ----------------
    rep.section("04 - Risk bucket counts (PROC FREQ on work.home_equity_risk; "
                "missing excluded from table and percent base, reported as Frequency Missing)")
    rep.check("Rows in risk dataset", len(final),
              sum(int(r["Frequency"]) for r in parse_show_table(L["04"], find_line(L["04"], "--- RISK_SEGMENT ---"))))
    for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        start = find_line(L["04"], f"--- {c} ---")
        rows = parse_show_table(L["04"], start)
        tbl = {key_of(r[c]): r for r in rows}
        vc = final[c].value_counts(dropna=True)
        n_nonmissing = int(vc.sum())
        for k, v in vc.items():
            r = tbl.get(k)
            rep.check(f"{c} = {k}: frequency", int(v), None if r is None else int(r["Frequency"]))
            rep.check(f"{c} = {k}: percent", rnd(v / n_nonmissing * 100, 2),
                      None if r is None else to_num(r["Percent"]), tol=0.001)
        rep.check(f"{c}: rows in table (missing excluded)", len(vc), len(rows))
        m = re.search(r"Frequency Missing = (\d+)", "\n".join(L["04"][start:start + 40]))
        rep.check(f"{c}: Frequency Missing", int(final[c].isna().sum()), int(m.group(1)) if m else None)

    rep.section("04 - Default rate by LTV_RISK_CAT x DTI_RISK_CAT (PROC FREQ tables LTV*DTI*BAD; missing excluded)")
    rows = parse_show_table(L["04"], find_line(L["04"], "Default Rates by LTV Risk and DTI Risk"))
    tbl = {(key_of(r["LTV_RISK_CAT"]), key_of(r["DTI_RISK_CAT"])): r for r in rows}
    groups = final.groupby(["LTV_RISK_CAT", "DTI_RISK_CAT"])
    rep.check("Cells in table (missing excluded)", groups.ngroups, len(rows))
    for (ltv_cat, dti_cat), g in groups:
        r = tbl.get((ltv_cat, dti_cat), {})
        rep.check(f"LTV {ltv_cat} x DTI {dti_cat}: N", len(g), to_num(r.get("N")))
        rep.check(f"LTV {ltv_cat} x DTI {dti_cat}: default %", rnd(g["BAD"].mean() * 100, 2),
                  to_num(r.get("Default_Rate_Pct")), tol=0.001)

    rep.section("04 - Default rate by RISK_SEGMENT (PROC MEANS class RISK_SEGMENT)")
    tbl = {r["RISK_SEGMENT"]: r for r in
           parse_show_table(L["04"], find_line(L["04"], "Average Default Rate by Risk Segment"))}
    for k, g in final.groupby("RISK_SEGMENT"):
        r = tbl[k]
        rep.check(f"{k}: N", len(g), int(r["N"]))
        rep.check(f"{k}: default %", rnd(g["BAD"].mean() * 100, 2), to_num(r["Default_Rate_Pct"]), tol=0.001)
        rep.check(f"{k}: avg LOAN", rnd(g["LOAN"].mean(), 2), to_num(r["Avg_LOAN"]), tol=0.001)
        rep.check(f"{k}: avg LTV", rnd(g["LTV"].mean(), 4), to_num(r["Avg_LTV"]), tol=0.00001)
        rep.check(f"{k}: avg DEBTINC", rnd(g["DEBTINC"].mean(), 2), to_num(r["Avg_DEBTINC"]), tol=0.001)

    # ---------------- 03/04 extra: single-factor REASON / JOB stats ----------------
    if not args.skip_spark_groups:
        rep.section("Mean / median LOAN and DEBTINC by REASON and by JOB "
                    "(no SAS/PySpark step prints these; Spark recomputed here vs pandas)", supplementary=True)
        sg = spark_group_stats(final)
        for g in ["REASON", "JOB"]:
            for k, grp in final.groupby(g, dropna=False):
                kk = None if pd.isna(k) else k
                r = sg[g][kk]
                label = f"{g}={'missing' if kk is None else kk}"
                rep.check(f"{label}: N", len(grp), int(r["N"]))
                rep.check(f"{label}: mean LOAN", round(grp["LOAN"].mean(), 4), round(r["mean_LOAN"], 4), tol=1e-4)
                rep.check(f"{label}: median LOAN", float(grp["LOAN"].median()), float(r["median_LOAN"]), tol=1e-9)
                rep.check(f"{label}: mean DEBTINC", round(grp["DEBTINC"].mean(), 4),
                          round(r["mean_DEBTINC"], 4), tol=1e-4)
                rep.check(f"{label}: median DEBTINC", round(float(grp["DEBTINC"].median()), 6),
                          round(float(r["median_DEBTINC"]), 6), tol=1e-6)

    # ---------------- 05 logistic regression ----------------
    rep.section("05 - Logistic regression (PROC LOGISTIC vs Spark ML; sklearn as independent reference)")
    n_model_data, n_model, auc_holdout, auc_cv, auc_cv_sd = sklearn_auc(final)
    n_spark = parse_scalar(out["05"], r"Modeling dataset size: (\d+) rows", int)
    n_train = parse_scalar(out["05"], r"Training set: (\d+) rows", int)
    n_valid = parse_scalar(out["05"], r"Validation set: (\d+) rows", int)
    rep.check("Complete-case modelling rows (rows PROC LOGISTIC would actually use)", n_model, n_spark,
              note=f"SAS work.model_data has {n_model_data:,} rows before PROC LOGISTIC drops incomplete ones")
    rep.check("Train + validation rows", n_model, n_train + n_valid)
    rep.check("Train share (SAS SRS samprate=0.7 is exact; Spark randomSplit is Bernoulli)", 0.70,
              round(n_train / n_spark, 3), tol=0.03, note="approximate by design")
    spark_auc = parse_scalar(out["05"], r"AUC \(Area Under ROC\): ([\d.]+)")
    rep.check("Validation AUC (sklearn, SAS split order, 70/30 holdout vs Spark 70/30 holdout)",
              round(auc_holdout, 4), spark_auc, tol=0.05, note="plausibility: same neighbourhood")
    rep.check(f"Validation AUC vs sklearn 5-fold CV mean (sd {auc_cv_sd:.3f})", round(auc_cv, 4),
              spark_auc, tol=0.05, note="plausibility: same neighbourhood")
    cm = parse_show_table(L["05"], find_line(L["05"], "Confusion Matrix - Validation Set"))
    cm_total = sum(int(r["count"]) for r in cm)
    rep.check("Confusion-matrix cells sum to validation rows", n_valid, cm_total)
    acc = parse_scalar(out["05"], r"accuracy: ([\d.]+)")
    correct = sum(int(r["count"]) for r in cm if r["label"] == r["prediction"])
    rep.check("Accuracy recomputed from confusion matrix", round(correct / cm_total, 4), acc, tol=0.0001)

    rep.section("05 - Predicted probability by actual outcome (PROC MEANS n mean std min p25 median p75 max class BAD; "
                "model-specific, so checked for internal consistency only)", supplementary=True)
    pp = {r["label"]: r for r in
          parse_show_table(L["05"], find_line(L["05"], "Predicted Probability Distribution by Actual Outcome"))}
    for lab in sorted(pp):
        r = pp[lab]
        n_label = sum(int(c["count"]) for c in cm if c["label"] == lab)
        rep.check(f"label {lab}: N equals confusion-matrix row total", n_label, to_num(r.get("N")))
        q = [to_num(r.get(k)) for k in ("Min", "P25", "Median", "P75", "Max")]
        mean_pp = to_num(r.get("Mean"))
        if None in q or mean_pp is None:
            rep.check(f"label {lab}: n mean std min p25 median p75 max all printed", True, None)
            continue
        rep.check(f"label {lab}: min <= p25 <= median <= p75 <= max, all in [0, 1]", True,
                  all(a <= b for a, b in zip(q, q[1:])) and 0 <= q[0] and q[-1] <= 1)
        rep.check(f"label {lab}: mean within [min, max]", True, q[0] <= mean_pp <= q[-1])

    md = rep.render()
    print(md)
    if args.out:
        with open(args.out, "w") as f:
            f.write("# Parity check results\n\nGenerated by `python validation/parity_check.py`.\n" + md + "\n")
    failed = [r for r in rep.rows if not r[3]]
    if failed:
        print(f"\n{len(failed)} MISMATCH(ES):", file=sys.stderr)
        for m, a, b, _, _, supplementary in failed:
            kind = "supplementary" if supplementary else "parity"
            print(f"  - [{kind}] {m}: independent={a} pyspark={b}", file=sys.stderr)
        sys.exit(1)
    print("\nALL CHECKS MATCHED", file=sys.stderr)


if __name__ == "__main__":
    main()
