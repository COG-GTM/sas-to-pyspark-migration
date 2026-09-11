"""
Parity check for pyspark/03_aggregation_reporting.py vs sas/03_aggregation_reporting.sas.

Independent recomputation in pandas / plain Python of every number the PySpark
stage prints, implemented from the SAS semantics:

* Row base (sas/02_data_cleaning.sas -> work.home_equity_final):
    LOAN ne . and VALUE ne . and BAD ne .
    LTV = MORTDUE / VALUE when VALUE ne . and MORTDUE ne . and VALUE > 0, else .
    if LTV > 0 and LTV < 5   (missing LTV compares low in SAS -> row dropped)
    if LOAN > 0; if VALUE > 0
* PROC FREQ drops missing levels from the table and from the percent base.
* PROC MEANS / PROC TABULATE with CLASS drop rows where any class variable is
  missing; N is the per-variable non-missing count; STD is the sample std.
* Median uses SAS default PCTLDEF=5 (average of the two middle values when n is
  even) which is the ordinary median.
* PROC SQL GROUP BY keeps a NULL group, HAVING count(*) >= 10, top 10 by avg_loan.

Usage (from the repo root):
    python validation/03_aggregation_reporting/parity_check.py [path/to/run_log.txt]
"""

import math
import os
import re
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
DATA_PATH = os.path.join(REPO_ROOT, "data", "home_equity.csv")
DEFAULT_LOG = os.path.join(HERE, "logs", "run.txt")
RESULTS_PATH = os.path.join(HERE, "parity_results.md")

FREQ_COLS = ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]
MEANS_VARS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]
MEANS_STATS = ["N", "Mean", "Median", "Std", "Min", "Max"]
STEP5_VARS = ["LOAN", "LTV", "DEBTINC"]
STEP5_STATS = ["N", "Mean", "Std", "Median"]

SECTION_FREQ = "Frequency Tables"
SECTION_MEANS = "Summary Statistics by Loan Outcome"
SECTION_TAB = "Cross-tabulation"
SECTION_SQL = "Top 10 States"
SECTION_STEP5 = "Loan Distribution by Reason and Outcome"


# ----------------------------------------------------------------------------
# SAS-semantics recomputation (pandas)
# ----------------------------------------------------------------------------
def load_sas_final():
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    keep = df["LOAN"].notna() & df["VALUE"].notna() & df["BAD"].notna()
    df = df[keep].copy()
    ltv_ok = df["VALUE"].notna() & df["MORTDUE"].notna() & (df["VALUE"] > 0)
    df["LTV"] = (df["MORTDUE"] / df["VALUE"]).where(ltv_ok)
    df["LOAN_OUTCOME"] = df["BAD"].map({0: "Paid", 1: "Default"})
    # SAS: missing LTV is less than any number, so `LTV > 0` is false -> dropped
    final = df[(df["LTV"] > 0) & (df["LTV"] < 5) & (df["LOAN"] > 0) & (df["VALUE"] > 0)]
    return final.reset_index(drop=True)


def sas_stats(series):
    s = series.dropna()
    return {
        "N": int(len(s)),
        "Mean": float(s.mean()) if len(s) else math.nan,
        "Median": float(s.median()) if len(s) else math.nan,
        "Std": float(s.std(ddof=1)) if len(s) > 1 else math.nan,
        "Min": float(s.min()) if len(s) else math.nan,
        "Max": float(s.max()) if len(s) else math.nan,
    }


def expected_values(final):
    exp = {}

    # Step 1: PROC FREQ (missing dropped from table and percent base)
    for c in FREQ_COLS:
        nonmiss = final[c].dropna()
        counts = nonmiss.value_counts()
        base = len(nonmiss)
        exp[("freq", c)] = {
            "levels": {str(k): (int(v), 100.0 * v / base) for k, v in counts.items()},
            "missing": int(final[c].isna().sum()),
        }

    # Step 2: PROC MEANS class LOAN_OUTCOME
    exp["means"] = {}
    for outcome, grp in final.groupby("LOAN_OUTCOME"):
        exp["means"][outcome] = {v: sas_stats(grp[v]) for v in MEANS_VARS}

    # Step 3: PROC TABULATE class JOB REGION var BAD (missing class rows dropped)
    tab = final[final["JOB"].notna() & final["REGION"].notna()]
    cells = {}
    for (job, region), grp in tab.groupby(["JOB", "REGION"]):
        cells[(job, region)] = (len(grp), 100.0 * grp["BAD"].mean())
    for job, grp in tab.groupby("JOB"):
        cells[(job, "Total")] = (len(grp), 100.0 * grp["BAD"].mean())
    for region, grp in tab.groupby("REGION"):
        cells[("Total", region)] = (len(grp), 100.0 * grp["BAD"].mean())
    cells[("Total", "Total")] = (len(tab), 100.0 * tab["BAD"].mean())
    exp["tabulate"] = cells

    # Step 4: PROC SQL top 10 states by avg loan
    g = final.groupby("STATE", dropna=False)
    sql = pd.DataFrame({
        "num_loans": g.size(),
        "avg_loan": g["LOAN"].mean(),
        "avg_property_value": g["VALUE"].mean(),
        "default_rate_pct": 100.0 * g["BAD"].mean(),
    })
    sql = sql[sql["num_loans"] >= 10].sort_values("avg_loan", ascending=False).head(10)
    exp["sql"] = sql

    # Step 5: PROC MEANS class REASON LOAN_OUTCOME (missing REASON dropped)
    exp["step5"] = {}
    s5 = final[final["REASON"].notna()]
    for (reason, outcome), grp in s5.groupby(["REASON", "LOAN_OUTCOME"]):
        exp["step5"][(reason, outcome)] = {v: sas_stats(grp[v]) for v in STEP5_VARS}

    return exp


# ----------------------------------------------------------------------------
# Log parsing (Spark DataFrame.show() tables, grouped by printed section)
# ----------------------------------------------------------------------------
class Table:
    def __init__(self, header, rows):
        self.header = header
        self.rows = rows

    def find_row(self, **keys):
        for row in self.rows:
            if all(k in row and row[k] == v for k, v in keys.items()):
                return row
        return None


def parse_log(text):
    sections = {}
    current = "preamble"
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^--- (\w+) ---$", line.strip())
        if line.startswith("====") and i + 1 < len(lines) and not lines[i + 1].startswith("+"):
            current = lines[i + 1].strip()
            sections.setdefault(current, {"tables": [], "text": []})
            i += 2
            continue
        if m:
            current = "freq:" + m.group(1)
            sections.setdefault(current, {"tables": [], "text": []})
            i += 1
            continue
        if line.startswith("+-") and i + 2 < len(lines) and lines[i + 1].startswith("|"):
            header = [h.strip() for h in lines[i + 1].strip().strip("|").split("|")]
            rows = []
            j = i + 3
            while j < len(lines) and lines[j].startswith("|"):
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                rows.append(dict(zip(header, cells)))
                j += 1
            sections.setdefault(current, {"tables": [], "text": []})
            sections[current]["tables"].append(Table(header, rows))
            i = j + 1
            continue
        sections.setdefault(current, {"tables": [], "text": []})["text"].append(line)
        i += 1
    return sections


def section_tables(sections, prefix):
    for name, sec in sections.items():
        if name.startswith(prefix):
            return sec["tables"], sec["text"]
    return [], []


def is_null_label(value):
    return value is None or value.strip().lower() in ("null", "none", "")


def get_stat(tables, keys, var, stat):
    """Locate a statistic either as a wide column `<stat>_<var>` or as a long row
    keyed by Variable == var with column `<stat>`. Returns string or None."""
    for t in tables:
        wide = f"{stat}_{var}"
        row = t.find_row(**keys)
        if row is not None and wide in row:
            return row[wide]
        if "Variable" in t.header and stat in t.header:
            row = t.find_row(**keys, Variable=var)
            if row is not None:
                return row[stat]
    return None


# ----------------------------------------------------------------------------
# Comparison helpers
# ----------------------------------------------------------------------------
class Checker:
    def __init__(self):
        self.results = []

    def add(self, name, expected, actual, ok, note=""):
        self.results.append((name, expected, actual, ok, note))

    def num(self, name, expected, actual_str, decimals=None, abs_tol=None, note=""):
        if actual_str is None:
            self.add(name, fmt(expected), "MISSING", False, note or "statistic not printed")
            return
        try:
            actual = float(actual_str.replace(",", ""))
        except ValueError:
            self.add(name, fmt(expected), actual_str, False, "unparseable")
            return
        if abs_tol is None:
            if decimals is None:
                decimals = len(actual_str.split(".")[1]) if "." in actual_str else 0
            abs_tol = 0.5 * 10 ** (-decimals) + 1e-9
        if isinstance(expected, float) and math.isnan(expected):
            ok = actual_str.lower() in ("nan", "null")
        else:
            ok = abs(actual - expected) <= abs_tol
        self.add(name, fmt(expected), actual_str, ok, note)

    def matched(self):
        return sum(1 for r in self.results if r[3])


def fmt(x):
    if isinstance(x, float):
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return str(x)


def run_checks(sections, exp):
    ck = Checker()

    # ---- Step 1: PROC FREQ ----
    for c in FREQ_COLS:
        tables, text = section_tables(sections, "freq:" + c)
        t = tables[0] if tables else Table([], [])
        e = exp[("freq", c)]
        for level, (n, pct) in e["levels"].items():
            row = t.find_row(**{c: level})
            ck.num(f"FREQ {c}={level} Frequency", n, row.get("Frequency") if row else None)
            ck.num(f"FREQ {c}={level} Percent", pct, row.get("Percent") if row else None)
        null_rows = [r for r in t.rows if is_null_label(r.get(c))]
        ck.add(f"FREQ {c} missing level excluded from table",
               "no NULL row", "NULL row present" if null_rows else "no NULL row", not null_rows)
        miss = [m.group(1) for line in text for m in [re.match(r"Frequency Missing = (\d+)", line.strip())] if m]
        ck.num(f"FREQ {c} Frequency Missing", e["missing"], miss[0] if miss else None)

    # ---- Step 2: PROC MEANS class LOAN_OUTCOME ----
    tables, _ = section_tables(sections, SECTION_MEANS)
    for outcome, per_var in exp["means"].items():
        for var in MEANS_VARS:
            for stat in MEANS_STATS:
                val = get_stat(tables, {"LOAN_OUTCOME": outcome}, var, stat)
                ck.num(f"MEANS {outcome} {var} {stat}", per_var[var][stat], val)

    # ---- Step 3: PROC TABULATE ----
    tables, _ = section_tables(sections, SECTION_TAB)
    detail = [t for t in tables if "N" in t.header and "Default_Rate_Pct" in t.header]
    dt = detail[0] if detail else Table([], [])
    for (job, region), (n, pct) in sorted(exp["tabulate"].items()):
        row = dt.find_row(JOB=job, REGION=region)
        ck.num(f"TABULATE JOB={job} REGION={region} N", n, row.get("N") if row else None)
        ck.num(f"TABULATE JOB={job} REGION={region} Default_Rate_Pct", pct,
               row.get("Default_Rate_Pct") if row else None)
    null_rows = [r for r in dt.rows if is_null_label(r.get("JOB"))]
    ck.add("TABULATE missing JOB class rows excluded", "no NULL JOB rows",
           "NULL JOB rows present" if null_rows else "no NULL JOB rows", not null_rows)

    # ---- Step 4: PROC SQL ----
    tables, _ = section_tables(sections, SECTION_SQL)
    st = tables[0] if tables else Table([], [])
    got_states = [r.get("STATE") for r in st.rows]
    exp_states = list(exp["sql"].index)
    ck.add("SQL top-10 state order", ", ".join(map(str, exp_states)), ", ".join(map(str, got_states)),
           got_states == exp_states)
    for state, r in exp["sql"].iterrows():
        row = st.find_row(STATE=str(state))
        ck.num(f"SQL {state} num_loans", int(r["num_loans"]), row.get("num_loans") if row else None)
        ck.num(f"SQL {state} avg_loan", r["avg_loan"], row.get("avg_loan") if row else None)
        ck.num(f"SQL {state} avg_property_value", r["avg_property_value"],
               row.get("avg_property_value") if row else None)
        ck.num(f"SQL {state} default_rate_pct", r["default_rate_pct"],
               row.get("default_rate_pct") if row else None)

    # ---- Step 5: PROC MEANS class REASON LOAN_OUTCOME ----
    tables, _ = section_tables(sections, SECTION_STEP5)
    for (reason, outcome), per_var in sorted(exp["step5"].items()):
        for var in STEP5_VARS:
            for stat in STEP5_STATS:
                val = get_stat(tables, {"REASON": reason, "LOAN_OUTCOME": outcome}, var, stat)
                ck.num(f"STEP5 {reason}/{outcome} {var} {stat}", per_var[var][stat], val)
    null_rows = [r for t in tables for r in t.rows if is_null_label(r.get("REASON"))]
    ck.add("STEP5 missing REASON class rows excluded", "no NULL REASON rows",
           "NULL REASON rows present" if null_rows else "no NULL REASON rows", not null_rows)

    return ck


def write_results(path, log_path, final, ck):
    total = len(ck.results)
    matched = ck.matched()
    lines = [
        "# Parity results: 03_aggregation_reporting",
        "",
        f"- Log checked: `{os.path.relpath(log_path, REPO_ROOT)}`",
        f"- SAS row base (work.home_equity_final): **{len(final)}** rows",
        f"- Result: **{matched} / {total} checks matched**",
        "",
        "| Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Note |",
        "|---|---|---|---|---|",
    ]
    for name, expected, actual, ok, note in ck.results:
        lines.append(f"| {name} | {expected} | {actual} | {'MATCH' if ok else 'MISMATCH'} | {note} |")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    log_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_LOG
    if not os.path.exists(log_path):
        print(f"log not found: {log_path}")
        sys.exit(2)
    with open(log_path) as fh:
        sections = parse_log(fh.read())
    final = load_sas_final()
    exp = expected_values(final)
    ck = run_checks(sections, exp)
    write_results(RESULTS_PATH, log_path, final, ck)

    print(f"SAS row base (work.home_equity_final): {len(final)} rows")
    for name, expected, actual, ok, note in ck.results:
        if not ok:
            print(f"MISMATCH  {name}: expected {expected}, got {actual}  {note}")
    print(f"{ck.matched()} / {len(ck.results)} checks matched")
    print(f"results written to {os.path.relpath(RESULTS_PATH, REPO_ROOT)}")
    sys.exit(0 if ck.matched() == len(ck.results) else 1)


if __name__ == "__main__":
    main()
