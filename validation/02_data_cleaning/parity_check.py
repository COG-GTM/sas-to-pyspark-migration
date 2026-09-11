"""
Parity check for pyspark/02_data_cleaning.py against sas/02_data_cleaning.sas.

Independent recomputation (pandas / plain Python, no Spark) of every number the
PySpark stage prints, implemented from the SAS semantics:

  * DATA step 1: LTV = MORTDUE / VALUE only when VALUE and MORTDUE are non-missing
    and VALUE > 0, otherwise missing; LOAN_OUTCOME from BAD.
  * DATA step 3 (row base for PROC MEANS #1): LOAN, VALUE and BAD non-missing.
  * PROC MEANS (no CLASS): every VAR statistic is computed over that variable's own
    non-missing values; N and NMISS are per variable; STD uses VARDEF=DF (n-1);
    percentiles use SAS default PCTLDEF=5 (empirical distribution with averaging).
  * DATA step 5 (row base for PROC MEANS #2 / PROC PRINT): 0 < LTV < 5, LOAN > 0,
    VALUE > 0. A missing LTV fails "LTV > 0" in SAS, exactly as NULL does in Spark.
  * PROC PRINT obs=10: first ten observations of the final dataset in file order.

Usage (from repo root):
    python validation/02_data_cleaning/parity_check.py \
        [--log validation/02_data_cleaning/logs/run.txt] \
        [--out validation/02_data_cleaning/parity_results.md]
"""

import argparse
import math
import re
import sys

import numpy as np
import pandas as pd

DATA_PATH = "data/home_equity.csv"
DEFAULT_LOG = "validation/02_data_cleaning/logs/run.txt"
DEFAULT_OUT = "validation/02_data_cleaning/parity_results.md"

OUTLIER_VARS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
FINAL_VARS = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]
PCTS = [("p1", 0.01), ("p5", 0.05), ("p25", 0.25), ("median", 0.50),
        ("p75", 0.75), ("p95", 0.95), ("p99", 0.99)]
PRINT_COLS = ["BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"]

REL_TOL_MOMENT = 1e-6      # mean / stddev
REL_TOL_EXACT = 1e-9       # min / max / printed row values
REL_TOL_PCTL = 0.01        # percentiles: Spark exact-rank vs SAS PCTLDEF=5 averaging


# ----------------------------------------------------------------------------
# SAS-semantics recomputation
# ----------------------------------------------------------------------------
def sas_percentile(values, p):
    """SAS PCTLDEF=5: np = j + g; g == 0 -> (x_j + x_{j+1}) / 2 ; g > 0 -> x_{j+1}."""
    x = np.sort(np.asarray(values, dtype=float))
    n = len(x)
    np_ = n * p
    j = int(math.floor(np_ + 1e-12))
    g = np_ - j
    if g < 1e-12:
        if j <= 0:
            return float(x[0])
        if j >= n:
            return float(x[-1])
        return float((x[j - 1] + x[j]) / 2.0)
    return float(x[min(j, n - 1)])


def build_datasets():
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    ltv_ok = df["VALUE"].notna() & df["MORTDUE"].notna() & (df["VALUE"] > 0)
    df["LTV"] = np.where(ltv_ok, df["MORTDUE"] / df["VALUE"], np.nan)
    df["LOAN_OUTCOME"] = np.select(
        [df["BAD"] == 0, df["BAD"] == 1], ["Paid", "Default"], default=None
    )
    filtered = df[df["LOAN"].notna() & df["VALUE"].notna() & df["BAD"].notna()]
    final = filtered[
        (filtered["LTV"] > 0) & (filtered["LTV"] < 5)
        & (filtered["LOAN"] > 0) & (filtered["VALUE"] > 0)
    ]
    return df, filtered, final


def proc_means(frame, var):
    s = frame[var]
    nonmiss = s.dropna()
    return {
        "count": float(nonmiss.shape[0]),
        "nmiss": float(s.isna().sum()),
        "mean": float(nonmiss.mean()),
        "stddev": float(nonmiss.std(ddof=1)),
        "min": float(nonmiss.min()),
        "max": float(nonmiss.max()),
    }


def expected_values():
    df, filtered, final = build_datasets()
    exp = {}
    exp["rows.original"] = float(len(df))
    exp["rows.filtered"] = float(len(filtered))
    exp["rows.final"] = float(len(final))
    for var in OUTLIER_VARS:
        for stat, val in proc_means(filtered, var).items():
            exp[f"outlier.{var}.{stat}"] = val
        vals = filtered[var].dropna().values
        for name, p in PCTS:
            exp[f"pctl.{var}.{name}"] = sas_percentile(vals, p)
    for var in FINAL_VARS:
        for stat, val in proc_means(final, var).items():
            exp[f"final.{var}.{stat}"] = val
    top10 = final[PRINT_COLS].head(10).reset_index(drop=True)
    return exp, top10


# ----------------------------------------------------------------------------
# Stage log parsing
# ----------------------------------------------------------------------------
def parse_pipe_table(lines, start):
    """Parse a Spark show() table whose header row is at lines[start]. Returns (header, rows, next_idx)."""
    header = [c.strip() for c in lines[start].strip().strip("|").split("|")]
    rows = []
    i = start + 1
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if line.startswith("+"):
            i += 1
            if rows:
                break
            continue
        if not line.startswith("|"):
            break
        rows.append([c.strip() for c in line.strip().strip("|").split("|")])
        i += 1
    return header, rows, i


def parse_log(path):
    with open(path, encoding="utf-8") as fh:
        lines = fh.readlines()

    actual = {}
    top10 = None
    section = None
    describe_seen = 0
    pctl_re = re.compile(r"^\s*([A-Z_]+):\s*(p1=.*)$")

    i = 0
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if "Summary Statistics for Outlier Detection" in line:
            section = "outlier"
        elif "Clean Dataset Summary" in line:
            section = "final"

        m = re.match(r"^Original rows:\s*(\d+)", line)
        if m:
            actual["rows.original"] = float(m.group(1))
        m = re.match(r"^After filtering:\s*(\d+)", line)
        if m:
            actual["rows.filtered"] = float(m.group(1))
        m = re.match(r"^Final clean rows:\s*(\d+)", line)
        if m:
            actual["rows.final"] = float(m.group(1))

        m = pctl_re.match(line)
        if m and section == "outlier":
            var = m.group(1)
            for kv in m.group(2).split(","):
                key, val = kv.strip().split("=")
                actual[f"pctl.{var}.{key.strip()}"] = float(val)

        if line.startswith("|summary|"):
            header, rows, i = parse_pipe_table(lines, i)
            describe_seen += 1
            prefix = "outlier" if describe_seen == 1 else "final"
            for row in rows:
                stat = row[0]
                for var, val in zip(header[1:], row[1:]):
                    if val not in ("NULL", "null", ""):
                        actual[f"{prefix}.{var}.{stat}"] = float(val)
            continue

        if line.startswith("|BAD|LOAN|"):
            header, rows, i = parse_pipe_table(lines, i)
            top10 = pd.DataFrame(rows, columns=header)
            continue
        i += 1

    return actual, top10


# ----------------------------------------------------------------------------
# Comparison
# ----------------------------------------------------------------------------
def close(exp, act, rel):
    if act is None or (isinstance(act, float) and math.isnan(act)):
        return False
    if exp == 0:
        return abs(act) <= 1e-9
    return abs(exp - act) <= rel * abs(exp)


def tolerance_for(key):
    stat = key.split(".")[-1]
    if key.startswith("rows.") or stat in ("count", "nmiss"):
        return 0.0, "exact"
    if key.startswith("pctl."):
        return REL_TOL_PCTL, "rel 1% (PCTLDEF=5 vs exact rank)"
    if stat in ("mean", "stddev"):
        return REL_TOL_MOMENT, "rel 1e-6"
    return REL_TOL_EXACT, "rel 1e-9"


def fmt(v):
    if v is None:
        return "NOT PRINTED"
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e12:
        return str(int(v))
    return f"{v:.10g}"


def compare_scalar_checks(exp, act):
    results = []
    for key, e in exp.items():
        a = act.get(key)
        rel, tol_label = tolerance_for(key)
        if rel == 0.0:
            ok = a is not None and a == e
        else:
            ok = close(e, a, rel)
        results.append((key, fmt(e), fmt(a), tol_label, ok))
    return results


def norm_cell(val):
    if val is None or (isinstance(val, float) and math.isnan(val)) or val in ("NULL", "null", "None"):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return str(val)


def cells_match(e, a):
    e, a = norm_cell(e), norm_cell(a)
    if e is None or a is None:
        return e is None and a is None
    if isinstance(e, float) and isinstance(a, float):
        return close(e, a, REL_TOL_EXACT)
    return e == a


def compare_top10(exp_df, act_df):
    results = []
    for r in range(10):
        key = f"print.obs{r + 1}"
        if act_df is None or r >= len(act_df):
            results.append((key, "|".join(fmt_cell(exp_df.iloc[r][c]) for c in PRINT_COLS),
                            "NOT PRINTED", "exact per field", False))
            continue
        ok = all(cells_match(exp_df.iloc[r][c], act_df.iloc[r][c]) for c in PRINT_COLS)
        results.append((key,
                        "|".join(fmt_cell(exp_df.iloc[r][c]) for c in PRINT_COLS),
                        "|".join(str(act_df.iloc[r][c]) for c in PRINT_COLS),
                        "exact per field", ok))
    return results


def fmt_cell(v):
    n = norm_cell(v)
    if n is None:
        return "NULL"
    if isinstance(n, float):
        return fmt(n)
    return str(n)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    exp, exp_top10 = expected_values()
    act, act_top10 = parse_log(args.log)

    results = compare_scalar_checks(exp, act) + compare_top10(exp_top10, act_top10)
    matched = sum(1 for r in results if r[4])
    total = len(results)

    lines = [
        "# Parity results: 02_data_cleaning",
        "",
        f"Stage log: `{args.log}`",
        "",
        "Row base (from SAS): 5960 raw rows -> `home_equity_filtered` keeps LOAN, VALUE, BAD non-missing "
        f"({fmt(exp['rows.filtered'])} rows) -> `home_equity_final` additionally keeps 0 < LTV < 5, LOAN > 0, "
        f"VALUE > 0 ({fmt(exp['rows.final'])} rows). PROC MEANS #1 runs on the filtered set, PROC MEANS #2 and "
        "PROC PRINT on the final set. Percentiles follow SAS PCTLDEF=5.",
        "",
        f"**{matched} / {total} checks matched**",
        "",
        "| check | expected (SAS semantics, pandas) | actual (stage log) | tolerance | status |",
        "|---|---|---|---|---|",
    ]
    for key, e, a, tol, ok in results:
        lines.append(f"| {key} | {e} | {a} | {tol} | {'MATCH' if ok else 'MISMATCH'} |")
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    for key, e, a, tol, ok in results:
        if not ok:
            print(f"MISMATCH {key}: expected {e}, actual {a} [{tol}]")
    print(f"{matched} / {total} checks matched")
    print(f"Results written to {args.out}")
    return 0 if matched == total else 1


if __name__ == "__main__":
    sys.exit(main())
