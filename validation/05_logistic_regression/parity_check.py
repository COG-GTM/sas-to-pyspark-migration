"""
Parity check for pyspark/05_logistic_regression.py vs sas/05_logistic_regression.sas.

Independently recomputes (pandas / plain Python, scikit-learn only for a model
baseline) every number the PySpark stage prints, using the SAS semantics of the
upstream programs (02_data_cleaning.sas -> 04_risk_segmentation.sas ->
05_logistic_regression.sas), parses the stage log and compares.

Row base used by the SAS program (work.home_equity_risk -> work.model_data):
  02 step 1 : LTV = MORTDUE / VALUE when VALUE ne . and MORTDUE ne . and VALUE > 0, else .
  02 step 3 : LOAN ne . and VALUE ne . and BAD ne .
  02 step 5 : 0 < LTV < 5 (missing LTV fails 'LTV > 0'), LOAN > 0, VALUE > 0
  04        : no row filter (only derived columns)
  05 step 1 : LOAN, MORTDUE, VALUE, DEBTINC, DELINQ, CLAGE all non-missing
              (DEROG, NINQ, JOB, REASON may still be missing)
  05 fit    : PROC LOGISTIC silently drops rows with any missing model variable
  05 score  : PROC PLM returns missing pred_prob for such rows; the DATA step then
              assigns PREDICTED_BAD = 0 ('. >= 0.5' is false), so PROC FREQ's
              confusion matrix covers every validation row while PROC MEANS
              (n mean std min p25 median p75 max, class BAD) drops missing pred_prob.

SURVEYSELECT (srs, samprate=0.7, seed=42) and Spark randomSplit draw different
samples, so split-dependent quantities (sizes, coefficients, confusion cells,
AUC, score distribution) are checked for plausibility / internal consistency,
not equality. Split-independent quantities are checked exactly.

Usage (from repo root):
    python validation/05_logistic_regression/parity_check.py \
        [--log validation/05_logistic_regression/logs/run.txt] \
        [--out validation/05_logistic_regression/parity_results.md]
"""

import argparse
import os
import re
import sys

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join("data", "home_equity.csv")
DEFAULT_LOG = os.path.join(HERE, "logs", "run.txt")
DEFAULT_OUT = os.path.join(HERE, "parity_results.md")

NUMERIC = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
MODEL_DATA_REQUIRED = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]
FIT_REQUIRED = NUMERIC + ["JOB", "REASON"]


# ----------------------------------------------------------------------------
# SAS-semantics recomputation in pandas
# ----------------------------------------------------------------------------
def sas_row_base(path=DATA_PATH):
    df = pd.read_csv(path, encoding="utf-8-sig")
    ltv_ok = df["VALUE"].notna() & df["MORTDUE"].notna() & (df["VALUE"] > 0)
    df["LTV"] = (df["MORTDUE"] / df["VALUE"]).where(ltv_ok)
    # 02 step 3
    df = df[df["LOAN"].notna() & df["VALUE"].notna() & df["BAD"].notna()]
    # 02 step 5 (missing LTV is dropped because '. > 0' is false in SAS)
    df = df[(df["LTV"] > 0) & (df["LTV"] < 5) & (df["LOAN"] > 0) & (df["VALUE"] > 0)]
    # 05 step 1
    model_data = df.dropna(subset=MODEL_DATA_REQUIRED)
    return model_data.reset_index(drop=True)


def design_matrix(df):
    """param=ref coding with JOB(ref='Other') REASON(ref='HomeImp'), like PROC LOGISTIC."""
    x = df[NUMERIC].astype(float).copy()
    for lvl in sorted(df["JOB"].dropna().unique()):
        if lvl != "Other":
            x[f"JOB_{lvl}"] = (df["JOB"] == lvl).astype(float)
    for lvl in sorted(df["REASON"].dropna().unique()):
        if lvl != "HomeImp":
            x[f"REASON_{lvl}"] = (df["REASON"] == lvl).astype(float)
    return x


def sklearn_baseline(model_data, seed=42):
    """Unpenalised logistic regression baseline (SAS-like complete-case fit) on a 70/30 split."""
    complete = model_data.dropna(subset=FIT_REQUIRED)
    train, valid = train_test_split(complete, test_size=0.3, random_state=seed)
    x_train, x_valid = design_matrix(train), design_matrix(valid)
    y_train, y_valid = train["BAD"].astype(int).values, valid["BAD"].astype(int).values
    clf = make_pipeline(StandardScaler(), LogisticRegression(C=np.inf, max_iter=5000))
    clf.fit(x_train, y_train)
    prob = clf.predict_proba(x_valid)[:, 1]
    coef = dict(zip(x_train.columns, clf[-1].coef_[0]))
    return {
        "complete_rows": len(complete),
        "auc": roc_auc_score(y_valid, prob),
        "aupr": average_precision_score(y_valid, prob),
        "coef": coef,
        "mean_prob_by_bad": {b: float(prob[y_valid == b].mean()) for b in (0, 1)},
    }


# ----------------------------------------------------------------------------
# Log parsing
# ----------------------------------------------------------------------------
def parse_spark_tables(lines):
    """Return list of (header_line_index, [dict rows]) for every +---+ delimited table."""
    tables = []
    i = 0
    while i < len(lines):
        if re.match(r"^\+[-+]+\+$", lines[i].strip()) and i + 2 < len(lines) and lines[i + 1].startswith("|"):
            header = [h.strip() for h in lines[i + 1].strip().strip("|").split("|")]
            j = i + 3
            rows = []
            while j < len(lines) and lines[j].startswith("|"):
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                rows.append(dict(zip(header, cells)))
                j += 1
            tables.append((i, rows))
            i = j + 1
        else:
            i += 1
    return tables


def to_float(s):
    if s is None or s in ("", "NULL", "null", "None"):
        return None
    return float(s.replace(",", ""))


def parse_log(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = [ln.rstrip("\n") for ln in fh]
    text = "\n".join(lines)
    out = {"tables": parse_spark_tables(lines), "lines": lines}

    def grab(pattern, cast=float):
        m = re.search(pattern, text)
        return cast(m.group(1)) if m else None

    out["model_rows"] = grab(r"Modeling dataset size:\s*(\d+)", int)
    out["train_rows"] = grab(r"Training set:\s*(\d+)", int)
    out["valid_rows"] = grab(r"Validation set:\s*(\d+)", int)
    out["fit_rows"] = grab(r"Training rows used for fit[^:]*:\s*(\d+)", int)
    out["valid_unscored"] = grab(r"Validation rows with missing predictors[^:]*:\s*(\d+)", int)
    out["intercept"] = grab(r"Intercept:\s*(-?[\d.]+)")
    out["n_features"] = grab(r"Number of features:\s*(\d+)", int)
    out["coef"] = {int(i): float(v) for i, v in re.findall(r"Feature (\d+)[^:]*:\s*(-?[\d.]+)", text)}
    out["auc"] = grab(r"AUC \(Area Under ROC\):\s*([\d.]+)")
    out["aupr"] = grab(r"AUPR \(Area Under PR Curve\):\s*([\d.]+)")
    for m in ("accuracy", "weightedPrecision", "weightedRecall", "f1"):
        out[m] = grab(rf"^{m}:\s*([\d.]+)".replace("^", r"(?m)^"))

    # confusion matrix: table with label/prediction/count
    out["confusion"] = None
    for _, rows in out["tables"]:
        if rows and {"label", "prediction", "count"} <= set(rows[0]):
            out["confusion"] = {(int(float(r["label"])), int(float(r["prediction"]))): int(r["count"]) for r in rows}
            break

    # score distribution by BAD.  Either a single PROC MEANS style table
    # (label, n, mean, std, min, p25, median, p75, max) or the legacy
    # groupBy().agg() table + per-class describe() tables.
    dist = {0: {}, 1: {}}
    out["dist_table_has_n"] = False
    header_idx = next((k for k, ln in enumerate(lines)
                       if "Predicted Probability Distribution by Actual Outcome" in ln), None)
    if header_idx is not None:
        following = [(idx, rows) for idx, rows in out["tables"] if idx > header_idx]
        if following:
            first_rows = following[0][1]
            cols = set(first_rows[0]) if first_rows else set()
            out["dist_table_has_n"] = bool(cols & {"n", "count", "count(pred_prob)", "N"})
        for _, rows in following:
            if rows and "label" in rows[0]:
                for r in rows:
                    b = int(float(r["label"]))
                    for k, v in r.items():
                        if k == "label":
                            continue
                        key = {"avg(pred_prob)": "mean", "count(pred_prob)": "n", "count": "n",
                               "stddev": "std", "std": "std", "50%": "median", "25%": "p25",
                               "75%": "p75"}.get(k, k)
                        dist[b][key] = to_float(v)
        # describe()/summary() blocks: "Actual BAD = b:" followed by summary table
        for k, ln in enumerate(lines):
            m = re.match(r"Actual BAD = (\d):", ln.strip())
            if m:
                b = int(m.group(1))
                tbl = next((rows for idx, rows in out["tables"] if idx > k), None)
                if tbl and "summary" in tbl[0]:
                    for r in tbl:
                        key = {"count": "n", "stddev": "std", "25%": "p25", "50%": "median",
                               "75%": "p75"}.get(r["summary"], r["summary"])
                        dist[b][key] = to_float(r.get("pred_prob"))
    out["dist"] = dist
    return out


# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
class Checker:
    def __init__(self):
        self.rows = []

    def add(self, name, expected, actual, ok, kind, note=""):
        self.rows.append({"name": name, "expected": expected, "actual": actual,
                          "ok": bool(ok), "kind": kind, "note": note})

    def exact(self, name, expected, actual, note=""):
        self.add(name, expected, actual, actual is not None and actual == expected, "exact", note)

    def close(self, name, expected, actual, tol, note=""):
        ok = actual is not None and expected is not None and abs(actual - expected) <= tol
        self.add(name, f"{expected:.4f} +/- {tol:g}" if expected is not None else None,
                 None if actual is None else f"{actual:.4f}", ok, "tolerance", note)

    def present(self, name, actual, cond=True, note=""):
        self.add(name, "printed", actual, actual is not None and cond, "presence", note)


def fmt(v):
    if v is None:
        return "MISSING"
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def run_checks(log, base, model_data):
    c = Checker()
    n_model = len(model_data)
    bad_rate = model_data["BAD"].mean()
    complete_share = base["complete_rows"] / n_model

    # --- Step 1: modeling dataset (split independent, exact) -----------------
    c.exact("Step 1 modeling dataset rows (work.model_data)", n_model, log["model_rows"],
            "02 filters (LOAN/VALUE/BAD non-missing, 0<LTV<5, LOAN>0, VALUE>0) + 05 complete cases on "
            "LOAN MORTDUE VALUE DEBTINC DELINQ CLAGE")

    # --- Step 2: split ---------------------------------------------------------
    tr, va = log["train_rows"], log["valid_rows"]
    c.exact("Step 2 train + valid == model rows", n_model, None if tr is None or va is None else tr + va)
    c.close("Step 2 training share (SURVEYSELECT samprate=0.7)", 0.7, None if tr is None else tr / n_model, 0.025,
            f"SAS srs would draw exactly round(0.7*{n_model})={round(0.7 * n_model)}; randomSplit is Bernoulli")
    c.close("Step 2 validation share", 0.3, None if va is None else va / n_model, 0.025)

    # --- Step 3: model structure / coefficients ---------------------------------
    n_job = model_data["JOB"].dropna().nunique()
    n_reason = model_data["REASON"].dropna().nunique()
    # StringIndexer(handleInvalid='keep') adds one bucket, OneHotEncoder drops the last -> nlevels columns each
    n_sas_params = len(NUMERIC) + n_job - 1 + n_reason - 1
    c.exact("Step 3 number of assembled features", len(NUMERIC) + n_job + n_reason, log["n_features"],
            f"8 numeric + {n_job} JOB + {n_reason} REASON dummies (SAS param=ref has {n_sas_params})")
    fit_share = None if log["fit_rows"] is None or tr is None else log["fit_rows"] / tr
    c.close("Step 3 complete-case share of train used by fit (PROC LOGISTIC 'Number of Observations Used')",
            complete_share, fit_share, 0.03,
            f"SAS drops rows with missing DEROG/NINQ/JOB/REASON at fit time; overall share {complete_share:.4f}")
    sign_notes, sign_ok = [], True
    for i, name in enumerate(NUMERIC):
        if i in log["coef"]:
            sk = base["coef"][name]
            same = np.sign(log["coef"][i]) == np.sign(sk)
            sign_ok &= bool(same)
            flag = "" if same else " <-- sign differs"
            sign_notes.append(f"{name}: spark {log['coef'][i]:+.4f} / sklearn {sk:+.4f}{flag}")
    c.add("Step 3 non-zero numeric coefficient signs agree with unpenalised sklearn fit",
          "same signs", "; ".join(sign_notes) or "no numeric coefficients printed",
          sign_ok and bool(sign_notes), "plausibility",
          "L1 (elasticNet) fit vs stepwise MLE: magnitudes not comparable")

    # --- Step 5/6: confusion matrix --------------------------------------------
    cm = log["confusion"] or {}
    total = sum(cm.values()) if cm else None
    row1 = cm.get((1, 0), 0) + cm.get((1, 1), 0) if cm else None
    row0 = cm.get((0, 0), 0) + cm.get((0, 1), 0) if cm else None
    c.exact("Step 6 confusion matrix total == validation rows (PROC FREQ keeps every valid row)", va, total,
            "SAS: missing pred_prob -> PREDICTED_BAD=0, so no row is dropped from the table")
    c.close("Step 6 validation BAD=1 share vs modeling base rate", bad_rate,
            None if not cm else row1 / total, 0.03, f"model_data BAD rate {bad_rate:.4f}")

    # --- Step 7: metrics ---------------------------------------------------------
    if cm:
        tn, fp, fn, tp = cm.get((0, 0), 0), cm.get((0, 1), 0), cm.get((1, 0), 0), cm.get((1, 1), 0)
        acc = (tn + tp) / total
        pred0, pred1 = tn + fn, fp + tp
        prec0 = tn / pred0 if pred0 else 0.0
        prec1 = tp / pred1 if pred1 else 0.0
        rec0 = tn / row0 if row0 else 0.0
        rec1 = tp / row1 if row1 else 0.0
        w0, w1 = row0 / total, row1 / total
        wprec = w0 * prec0 + w1 * prec1
        wrec = w0 * rec0 + w1 * rec1

        def f1c(p, r):
            return 2 * p * r / (p + r) if (p + r) else 0.0
        wf1 = w0 * f1c(prec0, rec0) + w1 * f1c(prec1, rec1)
    else:
        acc = wprec = wrec = wf1 = None
    c.close("Step 7 accuracy recomputed from confusion matrix", acc, log["accuracy"], 5e-4)
    c.close("Step 7 weightedPrecision recomputed from confusion matrix", wprec, log["weightedPrecision"], 5e-4)
    c.close("Step 7 weightedRecall recomputed from confusion matrix", wrec, log["weightedRecall"], 5e-4)
    c.close("Step 7 f1 (weighted) recomputed from confusion matrix", wf1, log["f1"], 5e-4)
    c.close("Step 7 AUC vs sklearn complete-case baseline (SAS c-statistic proxy)", base["auc"], log["auc"], 0.06,
            "different split + regularisation; plausibility only")
    c.close("Step 7 AUPR vs sklearn baseline", base["aupr"], log["aupr"], 0.10, "plausibility only")

    # --- Step 8: PROC MEANS pred_prob by BAD -------------------------------------
    d = log["dist"]
    c.present("Step 8 N statistic present in by-outcome summary table (agg dict key collision)",
              "yes" if log["dist_table_has_n"] else None,
              note="PROC MEANS n; {'pred_prob': 'count', 'pred_prob': 'mean'} keeps only the last key")
    n0, n1 = d[0].get("n"), d[1].get("n")
    unscored = log["valid_unscored"] or 0
    c.exact("Step 8 N(BAD=0)+N(BAD=1)+unscored == validation rows (PROC MEANS drops missing pred_prob)", va,
            None if n0 is None or n1 is None else int(n0 + n1 + unscored),
            f"unscored (missing predictors) rows printed: {log['valid_unscored']}")
    m0, m1 = d[0].get("mean"), d[1].get("mean")
    c.add("Step 8 mean pred_prob: BAD=1 > BAD=0", "mean1 > mean0",
          None if m0 is None or m1 is None else f"{m0:.4f} < {m1:.4f}",
          m0 is not None and m1 is not None and m1 > m0, "plausibility",
          f"sklearn baseline means: {base['mean_prob_by_bad'][0]:.4f} / {base['mean_prob_by_bad'][1]:.4f}")
    if None not in (m0, m1, n0, n1) and cm:
        pooled = (m0 * n0 + m1 * n1) / (n0 + n1)
        c.close("Step 8 pooled mean pred_prob ~ validation BAD rate (calibration)", row1 / total, pooled, 0.03)
    else:
        c.close("Step 8 pooled mean pred_prob ~ validation BAD rate (calibration)", None, None, 0.03)
    for b in (0, 1):
        s = d[b]
        order_ok = None not in (s.get("min"), s.get("p25"), s.get("median"), s.get("p75"), s.get("max")) and \
            0.0 <= s["min"] <= s["p25"] <= s["median"] <= s["p75"] <= s["max"] <= 1.0
        c.add(f"Step 8 BAD={b}: min <= p25 <= median <= p75 <= max within [0,1]",
              "all printed & ordered",
              ", ".join(f"{k}={fmt(s.get(k))}" for k in ("min", "p25", "median", "p75", "max")),
              order_ok, "presence",
              "SAS PROC MEANS reports p25 median p75 (PCTLDEF=5); Spark percentile() interpolates linearly")
        std_ok = s.get("std") is not None and 0.0 < s["std"] < 0.5
        c.add(f"Step 8 BAD={b}: std printed and in (0, 0.5)", "printed", fmt(s.get("std")), std_ok, "presence")
    return c


def write_results(path, checker, log_path, base, model_data):
    matched = sum(r["ok"] for r in checker.rows)
    total = len(checker.rows)
    lines = [
        "# Parity results: 05_logistic_regression",
        "",
        f"Log checked: `{os.path.relpath(log_path)}`",
        "",
        f"**{matched} / {total} checks matched**",
        "",
        "## SAS row base (recomputed in pandas)",
        "",
        f"- work.model_data rows: {len(model_data)}",
        f"- complete cases for all 10 model variables (PROC LOGISTIC 'Observations Used' share): "
        f"{base['complete_rows']} ({base['complete_rows'] / len(model_data):.4f})",
        f"- BAD rate in model_data: {model_data['BAD'].mean():.4f}",
        f"- sklearn unpenalised baseline (70/30 split, seed 42): AUC {base['auc']:.4f}, AUPR {base['aupr']:.4f}",
        "",
        "## Checks",
        "",
        "| # | Check | Kind | Expected (SAS semantics) | Actual (PySpark log) | Result | Note |",
        "|---|-------|------|--------------------------|----------------------|--------|------|",
    ]
    for i, r in enumerate(checker.rows, 1):
        lines.append(f"| {i} | {r['name']} | {r['kind']} | {fmt(r['expected'])} | {fmt(r['actual'])} | "
                     f"{'MATCH' if r['ok'] else 'MISMATCH'} | {r['note']} |")
    lines.append("")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    return matched, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()
    if not os.path.exists(args.log):
        print(f"log not found: {args.log}", file=sys.stderr)
        return 2
    model_data = sas_row_base()
    base = sklearn_baseline(model_data)
    log = parse_log(args.log)
    checker = run_checks(log, base, model_data)
    matched, total = write_results(args.out, checker, args.log, base, model_data)
    for i, r in enumerate(checker.rows, 1):
        status = "MATCH   " if r["ok"] else "MISMATCH"
        print(f"[{status}] {i:2d}. {r['name']}: expected={fmt(r['expected'])} actual={fmt(r['actual'])}")
    print(f"\n{matched} / {total} checks matched")
    print(f"results written to {os.path.relpath(args.out)}")
    return 0 if matched == total else 1


if __name__ == "__main__":
    sys.exit(main())
