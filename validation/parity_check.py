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
    """Parse the nth DataFrame.show() table at or after `start`."""
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
    def __init__(self):
        self.rows = []
        self.sections = []

    def section(self, title):
        self.sections.append((title, len(self.rows)))

    def fmt(self, v):
        if v is None:
            return "null"
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
        self.rows.append((metric, self.fmt(independent), self.fmt(pyspark), ok, note))
        return ok

    def render(self):
        out = []
        bounds = [s[1] for s in self.sections] + [len(self.rows)]
        for (title, start), end in zip(self.sections, bounds[1:]):
            out.append(f"\n### {title}\n")
            out.append("| Metric | Independent (pandas / SAS semantics) | PySpark (script output) | Match |")
            out.append("|---|---|---|---|")
            for metric, a, b, ok, note in self.rows[start:end]:
                flag = "yes" if ok else "**NO**"
                if note:
                    flag += f" ({note})"
                out.append(f"| {metric} | {a} | {b} | {flag} |")
        n_ok = sum(1 for r in self.rows if r[3])
        out.append(f"\n**{n_ok} / {len(self.rows)} checks matched.**")
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
    """Plain scikit-learn logistic regression on the same features as pyspark/05."""
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
    cat = ["JOB", "REASON"]
    model = final.dropna(subset=numeric + cat)
    X, y = model[numeric + cat], model["BAD"].astype(int)

    pipe = Pipeline([
        ("prep", ColumnTransformer([
            ("num", StandardScaler(), numeric),
            # ref levels match SAS: JOB(ref='Other') REASON(ref='HomeImp')
            ("cat", OneHotEncoder(drop=np.array(["Other", "HomeImp"], dtype=object)), cat),
        ])),
        ("lr", LogisticRegression(C=1e6, max_iter=5000)),  # ~unpenalised MLE, like PROC LOGISTIC
    ])
    Xtr, Xva, ytr, yva = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
    pipe.fit(Xtr, ytr)
    holdout_auc = roc_auc_score(yva, pipe.predict_proba(Xva)[:, 1])
    cv = cross_val_score(pipe, X, y, cv=StratifiedKFold(5, shuffle=True, random_state=42),
                         scoring="roc_auc")
    return len(model), holdout_auc, cv.mean(), cv.std()


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
    bad_rate = raw["BAD"].mean()
    rep.check("BAD default rate (HMEQ documented: ~19.95%)", round(bad_rate * 100, 2),
              round(HMEQ_BAD_RATE * 100, 2), tol=0.01, note="pandas vs documented HMEQ")
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
    ltv_row = final.loc[(final["MORTDUE"] == 25860) & (final["VALUE"] == 39025), "LTV"]
    rep.check("LTV spot check MORTDUE=25860 / VALUE=39025", round(25860 / 39025, 6),
              round(float(ltv_row.iloc[0]), 6), tol=1e-6, note="pandas derived column")

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

    rep.section("03 - Summary statistics by LOAN_OUTCOME (PROC MEANS n mean median)")
    tbl = {r["LOAN_OUTCOME"]: r for r in
           parse_show_table(L["03"], find_line(L["03"], "Summary Statistics by Loan Outcome"))}
    for k, g in final.groupby("LOAN_OUTCOME"):
        r = tbl[k]
        rep.check(f"{k}: N", len(g), int(r["N"]))
        rep.check(f"{k}: mean LOAN", rnd(g["LOAN"].mean(), 2), to_num(r["Mean_LOAN"]), tol=0.001)
        rep.check(f"{k}: median LOAN", rnd(g["LOAN"].median(), 2), to_num(r.get("Median_LOAN")), tol=0.001)
        rep.check(f"{k}: mean MORTDUE", rnd(g["MORTDUE"].mean(), 2), to_num(r["Mean_MORTDUE"]), tol=0.001)
        rep.check(f"{k}: mean VALUE", rnd(g["VALUE"].mean(), 2), to_num(r["Mean_VALUE"]), tol=0.001)
        rep.check(f"{k}: mean DEBTINC", rnd(g["DEBTINC"].mean(), 2), to_num(r["Mean_DEBTINC"]), tol=0.001)
        rep.check(f"{k}: median DEBTINC", rnd(g["DEBTINC"].median(), 2), to_num(r.get("Median_DEBTINC")), tol=0.001)

    rep.section("03 - LOAN / DEBTINC by REASON x LOAN_OUTCOME (PROC MEANS class REASON LOAN_OUTCOME)")
    tbl = {(key_of(r["REASON"]), r["LOAN_OUTCOME"]): r for r in
           parse_show_table(L["03"], find_line(L["03"], "Loan Distribution by Reason and Outcome"))}
    for (reason, outcome), g in final.groupby(["REASON", "LOAN_OUTCOME"], dropna=False):
        key = (None if pd.isna(reason) else reason, outcome)
        r = tbl[key]
        label = f"{'missing' if key[0] is None else key[0]}/{outcome}"
        rep.check(f"{label}: N", len(g), int(r["N"]))
        rep.check(f"{label}: mean LOAN", rnd(g["LOAN"].mean(), 2), to_num(r["Mean_LOAN"]), tol=0.001)
        rep.check(f"{label}: median LOAN", rnd(g["LOAN"].median(), 2), to_num(r.get("Median_LOAN")), tol=0.001)
        rep.check(f"{label}: mean LTV", rnd(g["LTV"].mean(), 4), to_num(r["Mean_LTV"]), tol=0.00001)
        rep.check(f"{label}: mean DEBTINC", rnd(g["DEBTINC"].mean(), 2), to_num(r["Mean_DEBTINC"]), tol=0.001)
        rep.check(f"{label}: median DEBTINC", rnd(g["DEBTINC"].median(), 2), to_num(r.get("Median_DEBTINC")), tol=0.001)

    rep.section("03 - Default rate by JOB x REGION (PROC TABULATE)")
    tbl = {(key_of(r["JOB"]), r["REGION"]): r for r in
           parse_show_table(L["03"], find_line(L["03"], "Cross-tabulation: Default Rate by JOB x REGION"), nth=2)}
    for (job, region), g in final.groupby(["JOB", "REGION"], dropna=False):
        key = (None if pd.isna(job) else job, region)
        r = tbl[key]
        label = f"{'missing' if key[0] is None else key[0]} / {region}"
        rep.check(f"{label}: N", len(g), int(r["N"]))
        rep.check(f"{label}: default %", rnd(g["BAD"].mean() * 100, 2), to_num(r["Default_Rate_Pct"]), tol=0.001)

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
    rep.section("04 - Risk bucket counts (PROC FREQ on work.home_equity_risk)")
    rep.check("Rows in risk dataset", len(final),
              sum(int(r["Frequency"]) for r in parse_show_table(L["04"], find_line(L["04"], "--- RISK_SEGMENT ---"))))
    for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        tbl = {key_of(r[c]): int(r["Frequency"]) for r in
               parse_show_table(L["04"], find_line(L["04"], f"--- {c} ---"))}
        vc = final[c].value_counts(dropna=False)
        for k, v in vc.items():
            kk = None if pd.isna(k) else k
            rep.check(f"{c} = {'missing' if kk is None else kk}", int(v), tbl.get(kk))
        extra = set(tbl) - {None if pd.isna(k) else k for k in vc.index}
        if extra:
            rep.check(f"{c}: categories only in PySpark", "", ", ".join(map(str, extra)))

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
        rep.section("Mean / median LOAN and DEBTINC by REASON and by JOB (Spark recomputed vs pandas)")
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
    n_model, auc_holdout, auc_cv, auc_cv_sd = sklearn_auc(final)
    n_spark = parse_scalar(out["05"], r"Modeling dataset size: (\d+) rows", int)
    n_train = parse_scalar(out["05"], r"Training set: (\d+) rows", int)
    n_valid = parse_scalar(out["05"], r"Validation set: (\d+) rows", int)
    rep.check("Complete-case modelling rows", n_model, n_spark)
    rep.check("Train + validation rows", n_model, n_train + n_valid)
    rep.check("Train share (SAS SRS samprate=0.7 is exact; Spark randomSplit is Bernoulli)", 0.70,
              round(n_train / n_spark, 3), tol=0.03, note="approximate by design")
    spark_auc = parse_scalar(out["05"], r"AUC \(Area Under ROC\): ([\d.]+)")
    rep.check("Validation AUC (sklearn 70/30 holdout vs Spark 70/30 holdout)", round(auc_holdout, 4),
              spark_auc, tol=0.05, note="plausibility: same neighbourhood")
    rep.check(f"Validation AUC vs sklearn 5-fold CV mean (sd {auc_cv_sd:.3f})", round(auc_cv, 4),
              spark_auc, tol=0.05, note="plausibility: same neighbourhood")
    cm = parse_show_table(L["05"], find_line(L["05"], "Confusion Matrix - Validation Set"))
    cm_total = sum(int(r["count"]) for r in cm)
    rep.check("Confusion-matrix cells sum to validation rows", n_valid, cm_total)
    acc = parse_scalar(out["05"], r"accuracy: ([\d.]+)")
    correct = sum(int(r["count"]) for r in cm if r["label"] == r["prediction"])
    rep.check("Accuracy recomputed from confusion matrix", round(correct / cm_total, 4), acc, tol=0.0001)

    md = rep.render()
    print(md)
    if args.out:
        with open(args.out, "w") as f:
            f.write("# Parity check results\n\nGenerated by `python validation/parity_check.py`.\n" + md + "\n")
    failed = [r for r in rep.rows if not r[3]]
    if failed:
        print(f"\n{len(failed)} MISMATCH(ES):", file=sys.stderr)
        for m, a, b, _, _ in failed:
            print(f"  - {m}: independent={a} pyspark={b}", file=sys.stderr)
        sys.exit(1)
    print("\nALL CHECKS MATCHED", file=sys.stderr)


if __name__ == "__main__":
    main()
