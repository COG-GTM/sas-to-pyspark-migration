"""
Parity check for pyspark/04_risk_segmentation.py against sas/04_risk_segmentation.sas.

Independent recomputation in pandas / plain Python of every number the PySpark
stage prints, implemented from the SAS semantics:

  * Row base (sas/02_data_cleaning.sas -> work.home_equity_final):
        LOAN ne . and VALUE ne . and BAD ne .
        LTV > 0 and LTV < 5        (missing LTV is < 0 in SAS, so it is dropped)
        LOAN > 0 and VALUE > 0
    where LTV = MORTDUE / VALUE only when VALUE ne ., MORTDUE ne ., VALUE > 0.
  * PROC FREQ one-way tables drop missing levels from both the table and the
    percent base and report "Frequency Missing = n".
  * PROC MEANS n mean std / class RISK_SEGMENT: N is the per-variable
    non-missing count, STD is the sample standard deviation (n-1).
  * PROC FREQ LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent drops
    any observation with a missing value in any of the three variables and
    prints cell frequencies only.

Usage (from repo root):
    python validation/04_risk_segmentation/parity_check.py [--log PATH] [--out PATH]
"""

import argparse
import math
import os
import re
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "logs", "run.txt")
DEFAULT_OUT = os.path.join(HERE, "parity_results.md")
DATA_PATH = os.path.join(HERE, "..", "..", "data", "home_equity.csv")

FREQ_VARS = ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]
MEANS_VARS = ["BAD", "LOAN", "LTV", "DEBTINC"]
SEGMENT_ORDER = ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"]
LTV_ORDER = ["Low", "Medium", "High"]
DTI_ORDER = ["Low", "Medium", "High", "Very High"]
DELINQ_ORDER = ["None", "Low", "Medium", "High"]

SECTION_FREQ = "Distribution of Loans by Risk Segment"
SECTION_MEANS = "Average Default Rate by Risk Segment"
SECTION_CROSS = "Default Rates by LTV Risk and DTI Risk"


# ----------------------------------------------------------------------------
# SAS-semantics recomputation
# ----------------------------------------------------------------------------
def sas_row_base(path):
    df = pd.read_csv(path, encoding="utf-8-sig")

    ltv_ok = df["VALUE"].notna() & df["MORTDUE"].notna() & (df["VALUE"] > 0)
    df["LTV"] = float("nan")
    df.loc[ltv_ok, "LTV"] = df.loc[ltv_ok, "MORTDUE"] / df.loc[ltv_ok, "VALUE"]

    # 02 step 3: critical fields non-missing
    df = df[df["LOAN"].notna() & df["VALUE"].notna() & df["BAD"].notna()]
    # 02 step 5: outlier filters (missing LTV fails LTV > 0 in SAS)
    df = df[(df["LTV"] > 0) & (df["LTV"] < 5) & (df["LOAN"] > 0) & (df["VALUE"] > 0)]
    return df.reset_index(drop=True)


def ltv_cat(v):
    if pd.isna(v):
        return None
    if v < 0.60:
        return "Low"
    if v < 0.80:
        return "Medium"
    return "High"


def dti_cat(v):
    if pd.isna(v):
        return None
    if v < 30:
        return "Low"
    if v < 40:
        return "Medium"
    if v < 50:
        return "High"
    return "Very High"


def delinq_cat(v):
    if pd.isna(v):
        return None
    if v == 0:
        return "None"
    if v == 1:
        return "Low"
    if v <= 3:
        return "Medium"
    return "High"


def risk_score(row):
    score = 0.0
    ltv, dti, delinq, derog = row["LTV"], row["DEBTINC"], row["DELINQ"], row["DEROG"]
    if not pd.isna(ltv):
        if ltv >= 0.80:
            score += 3
        elif ltv >= 0.60:
            score += 1.5
    if not pd.isna(dti):
        if dti >= 50:
            score += 3
        elif dti >= 40:
            score += 2
        elif dti >= 30:
            score += 1
    if not pd.isna(delinq):
        if delinq >= 4:
            score += 2
        elif delinq >= 2:
            score += 1.5
        elif delinq == 1:
            score += 0.5
    if not pd.isna(derog):
        if derog >= 3:
            score += 2
        elif derog >= 1:
            score += 1
    return score


def risk_segment(score):
    if score < 3:
        return "Low Risk"
    if score < 5:
        return "Medium Risk"
    if score < 7:
        return "High Risk"
    return "Very High Risk"


def build_risk_frame(path):
    df = sas_row_base(path)
    df["LTV_RISK_CAT"] = df["LTV"].map(ltv_cat)
    df["DTI_RISK_CAT"] = df["DEBTINC"].map(dti_cat)
    df["DELINQ_RISK_CAT"] = df["DELINQ"].map(delinq_cat)
    df["RISK_SCORE"] = df.apply(risk_score, axis=1)
    df["RISK_SEGMENT"] = df["RISK_SCORE"].map(risk_segment)
    return df


def sample_std(series):
    s = series.dropna()
    if len(s) < 2:
        return float("nan")
    m = sum(s) / len(s)
    return math.sqrt(sum((x - m) ** 2 for x in s) / (len(s) - 1))


# ----------------------------------------------------------------------------
# Log parsing (Spark .show() tables)
# ----------------------------------------------------------------------------
def parse_log(text):
    """Return {section: {"tables": [(header, rows)], "missing": [ints]}}."""
    sections = {}
    section = None
    header = None
    rows = None
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.startswith("+-"):
            if header is not None and rows is not None and rows:
                sections[section]["tables"].append((header, rows))
                header, rows = None, None
            continue
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if header is None:
                header = cells
                rows = []
            else:
                rows.append(dict(zip(header, cells)))
            continue
        m = re.match(r"^--- (.+) ---$", line)
        if m:
            section = m.group(1)
            sections.setdefault(section, {"tables": [], "missing": []})
            continue
        m = re.match(r"^Frequency Missing = (\d+)$", line)
        if m and section is not None:
            sections[section]["missing"].append(int(m.group(1)))
            continue
        if not line or set(line) == {"="} or line.startswith("("):
            continue
        if section is not None and header is not None:
            continue
        section = line
        sections.setdefault(section, {"tables": [], "missing": []})
    return sections


def to_float(s):
    if s is None:
        return None
    s = s.strip()
    if s in ("", "NULL", "null", "NaN"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ----------------------------------------------------------------------------
# Comparison
# ----------------------------------------------------------------------------
class Checker:
    def __init__(self):
        self.results = []

    def add(self, section, name, expected, actual, tol=0.0, exact=False):
        if actual is None or (isinstance(actual, float) and math.isnan(actual)):
            ok = False
            detail = "not printed"
        elif isinstance(expected, str):
            ok = expected == actual
            detail = ""
        else:
            if expected is None or (isinstance(expected, float) and math.isnan(expected)):
                ok = actual is None
                detail = ""
            else:
                diff = abs(float(expected) - float(actual))
                ok = diff == 0 if exact else diff <= tol
                detail = f"diff={diff:.6g}"
        self.results.append((section, name, expected, actual, ok, detail))
        return ok

    @property
    def matched(self):
        return sum(1 for r in self.results if r[4])

    @property
    def total(self):
        return len(self.results)


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        return f"{v:.6g}" if abs(v) < 1e6 else f"{v:.2f}"
    return str(v)


def first_table(sections, section):
    tables = sections.get(section, {}).get("tables", [])
    return tables[0] if tables else (None, [])


def lookup(rows, key_col, key, value_col):
    for r in rows:
        if r.get(key_col) == key:
            return r.get(value_col)
    return None


def check_freq(chk, df, sections):
    orders = {
        "RISK_SEGMENT": SEGMENT_ORDER,
        "LTV_RISK_CAT": LTV_ORDER,
        "DTI_RISK_CAT": DTI_ORDER,
        "DELINQ_RISK_CAT": DELINQ_ORDER,
    }
    for var in FREQ_VARS:
        header, rows = first_table(sections, var)
        nonmissing = df[var].dropna()
        base = len(nonmissing)
        counts = nonmissing.value_counts()
        for level in orders[var]:
            freq = int(counts.get(level, 0))
            pct = freq / base * 100
            chk.add(var, f"Frequency[{level}]", freq, to_float(lookup(rows, var, level, "Frequency")), exact=True)
            chk.add(var, f"Percent[{level}] (base excludes missing)", round(pct, 2),
                    to_float(lookup(rows, var, level, "Percent")), tol=0.0101)
        n_missing = int(df[var].isna().sum())
        has_null_row = any(r.get(var) in ("NULL", "null", "") for r in rows)
        chk.add(var, "table excludes missing level (PROC FREQ default)",
                "excluded", "excluded" if rows and not has_null_row else ("NULL row present" if rows else None))
        reported = sections.get(var, {}).get("missing", [])
        chk.add(var, "Frequency Missing", n_missing, reported[0] if reported else None, exact=True)
    total_freq = sum(to_float(r.get("Frequency")) or 0
                     for r in first_table(sections, "RISK_SEGMENT")[1]
                     if r.get("RISK_SEGMENT") not in ("NULL", "null", ""))
    chk.add("ROW BASE", "sum of RISK_SEGMENT frequencies == SAS home_equity_final rows",
            len(df), total_freq if total_freq else None, exact=True)


def means_actual(rows, seg, var, stat):
    """Long format (fixed script): RISK_SEGMENT, N_Obs, Variable, N, Mean, Std.
    Falls back to the pre-fix wide format where possible."""
    for r in rows:
        if r.get("RISK_SEGMENT") == seg and r.get("Variable") == var:
            return to_float(r.get(stat))
    wide = {"N": None, "Mean": None, "Std": None}
    row = next((r for r in rows if r.get("RISK_SEGMENT") == seg), None)
    if row is None:
        return None
    if var == "BAD" and stat == "Mean":
        v = to_float(row.get("Default_Rate_Pct"))
        return None if v is None else v / 100
    if stat == "Mean":
        return to_float(row.get(f"Avg_{var}"))
    return wide[stat]


def check_means(chk, df, sections):
    header, rows = first_table(sections, SECTION_MEANS)
    tols = {"BAD": (0.00051, 0.00051), "LOAN": (0.0051, 0.0051), "LTV": (0.00051, 0.00051), "DEBTINC": (0.0051, 0.0051)}
    for seg in SEGMENT_ORDER:
        sub = df[df["RISK_SEGMENT"] == seg]
        n_obs = len(sub)
        actual_nobs = None
        row = next((r for r in rows if r.get("RISK_SEGMENT") == seg), None)
        if row is not None:
            actual_nobs = to_float(row.get("N_Obs", row.get("N")))
        chk.add(SECTION_MEANS, f"{seg}: N Obs", n_obs, actual_nobs, exact=True)
        for var in MEANS_VARS:
            s = sub[var].dropna()
            n = len(s)
            mean = float(s.mean()) if n else float("nan")
            std = sample_std(s)
            mean_tol, std_tol = tols[var]
            chk.add(SECTION_MEANS, f"{seg}: N({var}) (non-missing)", n, means_actual(rows, seg, var, "N"), exact=True)
            chk.add(SECTION_MEANS, f"{seg}: Mean({var})", mean, means_actual(rows, seg, var, "Mean"), tol=mean_tol)
            chk.add(SECTION_MEANS, f"{seg}: Std({var}) (sample, n-1)", std, means_actual(rows, seg, var, "Std"),
                    tol=std_tol)


def cross_actual(rows, ltv, dti, bad):
    for r in rows:
        if r.get("LTV_RISK_CAT") == ltv and r.get("DTI_RISK_CAT") == dti:
            v = to_float(r.get(f"BAD_{bad}"))
            if v is not None:
                return v
            n = to_float(r.get("N"))
            rate = to_float(r.get("Default_Rate_Pct"))
            if n is None or rate is None:
                return None
            bad1 = round(n * rate / 100)
            return bad1 if bad == 1 else n - bad1
    return None


def check_cross(chk, df, sections):
    header, rows = first_table(sections, SECTION_CROSS)
    complete = df[df["LTV_RISK_CAT"].notna() & df["DTI_RISK_CAT"].notna() & df["BAD"].notna()]
    n_missing = len(df) - len(complete)
    for ltv in LTV_ORDER:
        for dti in DTI_ORDER:
            cell = complete[(complete["LTV_RISK_CAT"] == ltv) & (complete["DTI_RISK_CAT"] == dti)]
            for bad in (0, 1):
                exp = int((cell["BAD"] == bad).sum())
                chk.add(SECTION_CROSS, f"{ltv} x {dti} x BAD={bad} frequency", exp,
                        cross_actual(rows, ltv, dti, bad), exact=True)
    has_null = any(r.get("LTV_RISK_CAT") in ("NULL", "") or r.get("DTI_RISK_CAT") in ("NULL", "") for r in rows)
    chk.add(SECTION_CROSS, "table excludes missing levels (PROC FREQ default)",
            "excluded", "excluded" if rows and not has_null else ("NULL rows present" if rows else None))
    reported = sections.get(SECTION_CROSS, {}).get("missing", [])
    chk.add(SECTION_CROSS, "Frequency Missing", n_missing, reported[0] if reported else None, exact=True)


# ----------------------------------------------------------------------------
def write_report(path, chk, log_path, df):
    lines = [
        "# Parity results: 04_risk_segmentation",
        "",
        f"Log checked: `{os.path.relpath(log_path)}`",
        "",
        f"SAS row base (work.home_equity_final): **{len(df)}** rows "
        "(LOAN/VALUE/BAD non-missing, 0 < LTV < 5, LOAN > 0, VALUE > 0).",
        "",
        f"## Summary: {chk.matched} / {chk.total} checks matched",
        "",
        "| Section | Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Detail |",
        "|---|---|---|---|---|---|",
    ]
    for section, name, exp, act, ok, detail in chk.results:
        lines.append(f"| {section} | {name} | {fmt(exp)} | {fmt(act)} | {'MATCH' if ok else 'MISMATCH'} | {detail} |")
    lines.append("")
    with open(path, "w") as fh:
        fh.write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--data", default=DATA_PATH)
    args = ap.parse_args()

    with open(args.log) as fh:
        sections = parse_log(fh.read())

    df = build_risk_frame(args.data)
    chk = Checker()
    check_freq(chk, df, sections)
    check_means(chk, df, sections)
    check_cross(chk, df, sections)
    write_report(args.out, chk, args.log, df)

    print(f"SAS row base: {len(df)} rows")
    for section, name, exp, act, ok, detail in chk.results:
        if not ok:
            print(f"MISMATCH [{section}] {name}: expected {fmt(exp)} got {fmt(act)} {detail}")
    print(f"{chk.matched} / {chk.total} checks matched")
    print(f"Results written to {os.path.relpath(args.out)}")
    return 0 if chk.matched == chk.total else 1


if __name__ == "__main__":
    sys.exit(main())
