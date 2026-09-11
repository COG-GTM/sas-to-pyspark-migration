"""
Parity check for pyspark/01_data_loading.py against sas/01_data_loading.sas.

Independent recomputation (pandas / plain Python, no Spark) of every number the
PySpark stage prints, derived from the SAS program's semantics:

  * PROC IMPORT (dbms=csv, guessingrows=5960): every data row is loaded, every
    header field becomes a variable, and each variable is typed Num when all of
    its non-missing values parse as numbers over the full guessing window,
    otherwise Char.  Row base = all 5960 CSV records (no filters in stage 01).
  * PROC DATASETS ... LABEL: the 18 variable labels are taken verbatim from
    the SAS source and compared with the labels printed by the PySpark script.
  * PROC CONTENTS: number of observations, number of variables, variable names
    in file order, variable types (Num/Char).
  * PROC PRINT (obs=20): the first 20 records in file order, cell by cell.

Usage (from the repo root):
    python validation/01_data_loading/parity_check.py [--log PATH] [--out PATH]
"""

import argparse
import csv
import math
import os
import re
import sys

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_PATH = os.path.join(REPO_ROOT, "data", "home_equity.csv")
SAS_PATH = os.path.join(REPO_ROOT, "sas", "01_data_loading.sas")
DEFAULT_LOG = os.path.join(REPO_ROOT, "validation", "01_data_loading", "logs", "run.txt")
DEFAULT_OUT = os.path.join(REPO_ROOT, "validation", "01_data_loading", "parity_results.md")

PREVIEW_ROWS = 20
REL_TOL = 1e-9
ABS_TOL = 1e-9

SPARK_NUM_TYPES = {"integer", "long", "double", "float", "decimal", "short", "byte"}
SPARK_CHAR_TYPES = {"string"}


# ---------------------------------------------------------------------------
# Expected values: SAS semantics recomputed with pandas / plain Python
# ---------------------------------------------------------------------------
def load_csv_raw(path):
    """Read the CSV as raw strings (missing = '') so nothing is coerced yet."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    return df


def sas_type(series):
    """PROC IMPORT guessing rule: Num if every non-missing value is numeric, else Char."""
    non_missing = series[series.str.strip() != ""]
    if len(non_missing) == 0:
        return "Char"
    converted = pd.to_numeric(non_missing, errors="coerce")
    return "Num" if converted.notna().all() else "Char"


def sas_labels(sas_path):
    """Extract NAME="label" pairs from the LABEL statement of the SAS program."""
    with open(sas_path, encoding="utf-8") as fh:
        text = fh.read()
    match = re.search(r"\blabel\b(.*?);", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return {}
    return dict(re.findall(r'(\w+)\s*=\s*"([^"]*)"', match.group(1)))


def expected_from_sas():
    raw = load_csv_raw(DATA_PATH)
    with open(DATA_PATH, newline="", encoding="utf-8-sig") as fh:
        n_records = sum(1 for _ in csv.reader(fh)) - 1  # minus header
    columns = list(raw.columns)
    types = {c: sas_type(raw[c]) for c in columns}
    preview = raw.head(PREVIEW_ROWS)
    return {
        "n_rows": n_records,
        "n_cols": len(columns),
        "columns": columns,
        "types": types,
        "labels": sas_labels(SAS_PATH),
        "preview": preview,
    }


# ---------------------------------------------------------------------------
# Actual values: parse the PySpark stage log
# ---------------------------------------------------------------------------
def parse_log(log_path):
    with open(log_path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    actual = {"labels": {}, "schema": [], "n_rows": None, "n_cols": None, "preview": []}

    for line in lines:
        m = re.match(r"^\s{2}(\w+)\s+->\s+(.*)$", line)
        if m:
            actual["labels"][m.group(1)] = m.group(2).rstrip()
            continue
        m = re.match(r"^\s*\|--\s+(\w+):\s+(\w+)", line)
        if m:
            actual["schema"].append((m.group(1), m.group(2)))
            continue
        m = re.match(r"^Number of rows:\s+(\d+)", line)
        if m:
            actual["n_rows"] = int(m.group(1))
            continue
        m = re.match(r"^Number of columns:\s+(\d+)", line)
        if m:
            actual["n_cols"] = int(m.group(1))
            continue

    table_lines = [ln for ln in lines if ln.startswith("|")]
    if table_lines:
        header = [c.strip() for c in table_lines[0].strip().strip("|").split("|")]
        rows = []
        for ln in table_lines[1:]:
            cells = [c.strip() for c in ln.strip().strip("|").split("|")]
            if len(cells) == len(header):
                rows.append(dict(zip(header, cells)))
        actual["preview_header"] = header
        actual["preview"] = rows
    else:
        actual["preview_header"] = []
    return actual


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------
def is_missing_expected(value):
    return value is None or str(value).strip() == ""


def is_missing_actual(value):
    return value is None or str(value).strip().upper() in {"NULL", "NONE", "NAN", ""}


def cells_equal(expected, actual, col_type):
    if is_missing_expected(expected) or is_missing_actual(actual):
        return is_missing_expected(expected) and is_missing_actual(actual)
    if col_type == "Num":
        try:
            e = float(expected)
            a = float(actual)
        except ValueError:
            return False
        return math.isclose(e, a, rel_tol=REL_TOL, abs_tol=ABS_TOL)
    return str(expected) == str(actual)


class Results:
    def __init__(self):
        self.rows = []

    def add(self, section, name, expected, actual, ok, note=""):
        self.rows.append((section, name, str(expected), str(actual), bool(ok), note))

    @property
    def matched(self):
        return sum(1 for r in self.rows if r[4])

    @property
    def total(self):
        return len(self.rows)


def run_checks(expected, actual):
    res = Results()

    # PROC CONTENTS: observation / variable counts
    res.add("PROC CONTENTS", "Number of observations", expected["n_rows"], actual["n_rows"],
            actual["n_rows"] == expected["n_rows"])
    res.add("PROC CONTENTS", "Number of variables", expected["n_cols"], actual["n_cols"],
            actual["n_cols"] == expected["n_cols"])

    # PROC CONTENTS: variable names in file order
    schema_names = [n for n, _ in actual["schema"]]
    res.add("PROC CONTENTS", "Variable names (file order)", expected["columns"], schema_names,
            schema_names == expected["columns"])

    # PROC CONTENTS: variable types Num / Char
    schema_types = dict(actual["schema"])
    for col in expected["columns"]:
        spark_type = schema_types.get(col, "<missing>")
        if spark_type in SPARK_NUM_TYPES:
            mapped = "Num"
        elif spark_type in SPARK_CHAR_TYPES:
            mapped = "Char"
        else:
            mapped = "<%s>" % spark_type
        res.add("PROC CONTENTS type", col, expected["types"][col], "%s (%s)" % (mapped, spark_type),
                mapped == expected["types"][col])

    # PROC DATASETS LABEL: 18 labels verbatim from the SAS source
    for col, label in expected["labels"].items():
        got = actual["labels"].get(col, "<missing>")
        res.add("LABEL", col, label, got, got == label)

    # PROC PRINT obs=20: header + 20 records, cell by cell
    res.add("PROC PRINT", "Preview header", expected["columns"], actual.get("preview_header"),
            actual.get("preview_header") == expected["columns"])
    res.add("PROC PRINT", "Preview row count", PREVIEW_ROWS, len(actual["preview"]),
            len(actual["preview"]) == PREVIEW_ROWS)
    preview = expected["preview"]
    for i in range(PREVIEW_ROWS):
        exp_row = preview.iloc[i] if i < len(preview) else None
        act_row = actual["preview"][i] if i < len(actual["preview"]) else None
        if exp_row is None or act_row is None:
            res.add("PROC PRINT", "Obs %d" % (i + 1), "<row>", "<missing>", False)
            continue
        bad = []
        for col in expected["columns"]:
            e = exp_row[col]
            a = act_row.get(col)
            if not cells_equal(e, a, expected["types"][col]):
                bad.append("%s: expected %r got %r" % (col, e, a))
        summary_e = "BAD=%s LOAN=%s CITY=%s" % (exp_row["BAD"], exp_row["LOAN"], exp_row["CITY"])
        summary_a = "BAD=%s LOAN=%s CITY=%s" % (act_row.get("BAD"), act_row.get("LOAN"), act_row.get("CITY"))
        res.add("PROC PRINT", "Obs %d (18 cells)" % (i + 1), summary_e, summary_a, not bad, "; ".join(bad))
    return res


def write_report(res, out_path, log_path):
    lines = [
        "# Parity results: 01_data_loading",
        "",
        "SAS reference: `sas/01_data_loading.sas`  ",
        "PySpark stage: `pyspark/01_data_loading.py`  ",
        "Stage log parsed: `%s`  " % os.path.relpath(log_path, REPO_ROOT),
        "Expected values: recomputed with pandas / csv from `data/home_equity.csv` and the LABEL "
        "statement of the SAS source (no Spark).",
        "",
        "Row base: PROC IMPORT loads every CSV record (5960 rows, 18 variables). Stage 01 applies no "
        "filters; the `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` and non-missing LOAN/VALUE/BAD filters belong "
        "to `02_data_cleaning.sas` and are not expected here.",
        "",
        "Tolerances: numeric cells compared with rel_tol=%g / abs_tol=%g; SAS missing (`.` / blank) "
        "matched against Spark `NULL`; strings compared exactly." % (REL_TOL, ABS_TOL),
        "",
        "## Summary",
        "",
        "**%d / %d checks matched**" % (res.matched, res.total),
        "",
        "## Checks",
        "",
        "| # | Section | Check | Expected (SAS semantics) | Actual (PySpark log) | Match | Note |",
        "|---|---------|-------|--------------------------|----------------------|-------|------|",
    ]
    for i, (section, name, e, a, ok, note) in enumerate(res.rows, start=1):
        e_disp = e.replace("|", "\\|")
        a_disp = a.replace("|", "\\|")
        if len(e_disp) > 120:
            e_disp = e_disp[:117] + "..."
        if len(a_disp) > 120:
            a_disp = a_disp[:117] + "..."
        lines.append("| %d | %s | %s | %s | %s | %s | %s |" % (
            i, section, name, e_disp, a_disp, "OK" if ok else "MISMATCH", note.replace("|", "\\|")))
    lines.append("")
    lines.append("## Not applicable to this stage")
    lines.append("")
    lines.append("- No PROC FREQ / PROC MEANS / medians / model AUC in `01_data_loading.sas`; nothing to compare.")
    lines.append("- SAS display formats (dollar12., date9., 8.1, comma8.1) affect rendering only, not values; "
                 "PySpark prints raw values (e.g. `25860.0` for `$25,860`) - harmless.")
    lines.append("")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log", default=DEFAULT_LOG, help="PySpark stage stdout log to validate")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Markdown results file to write")
    args = parser.parse_args()

    if not os.path.exists(args.log):
        print("Stage log not found: %s" % args.log)
        return 2

    expected = expected_from_sas()
    actual = parse_log(args.log)
    res = run_checks(expected, actual)
    write_report(res, args.out, args.log)

    print("Parity check: pyspark/01_data_loading.py vs sas/01_data_loading.sas")
    print("Log: %s" % os.path.relpath(args.log, REPO_ROOT))
    for section, name, e, a, ok, note in res.rows:
        status = "OK      " if ok else "MISMATCH"
        detail = "" if ok else "  expected=%s actual=%s %s" % (e[:60], a[:60], note[:120])
        print("  [%s] %s / %s%s" % (status, section, name, detail))
    print()
    print("%d / %d checks matched" % (res.matched, res.total))
    print("Results written to %s" % os.path.relpath(args.out, REPO_ROOT))
    return 0 if res.matched == res.total else 1


if __name__ == "__main__":
    sys.exit(main())
