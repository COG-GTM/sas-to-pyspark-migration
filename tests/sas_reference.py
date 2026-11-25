"""
Reference implementation of sas/06_simple_ltv_bands.sas in plain Python (no Spark).

No SAS runtime is available, so the "SAS-expected" output is produced by hand-
translating the DATA step and PROC MEANS logic using SAS semantics:
  * empty CSV cells become SAS missing (.)
  * missing compares lower than every number (so "LTV < 0.8" is TRUE for missing)
  * N() and MEAN() ignore missing values
  * output is sorted by the CLASS variable (PROC SORT BY LTV_BAND)

Run directly to regenerate expected/06_ltv_band_summary_sas_expected.csv.
"""

import csv
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
EXPECTED_PATH = os.path.join(PROJECT_ROOT, "expected", "06_ltv_band_summary_sas_expected.csv")

MISSING = None


def sas_num(text):
    text = text.strip()
    return MISSING if text == "" else float(text)


def sas_lt(a, b):
    """SAS '<': missing is smaller than any non-missing number."""
    if a is MISSING:
        return b is not MISSING
    if b is MISSING:
        return False
    return a < b


def sas_gt(a, b):
    return sas_lt(b, a)


def ltv_band_rows(path=DATA_PATH):
    """DATA work.ltv_bands step, one dict per output observation."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for rec in csv.DictReader(f):
            bad, loan = sas_num(rec["BAD"]), sas_num(rec["LOAN"])
            mortdue, value = sas_num(rec["MORTDUE"]), sas_num(rec["VALUE"])

            if not (loan is not MISSING and value is not MISSING and sas_gt(value, 0)):
                continue

            ltv = mortdue / value if mortdue is not MISSING else MISSING

            if ltv is MISSING:
                band = "UNKNOWN"
            elif sas_lt(ltv, 0.8):
                band = "LOW"
            elif sas_lt(ltv, 1.0):
                band = "MEDIUM"
            else:
                band = "HIGH"

            rows.append({"BAD": bad, "LOAN": loan, "MORTDUE": mortdue, "VALUE": value,
                         "REASON": rec["REASON"], "LTV": ltv, "LTV_BAND": band})
    return rows


def ltv_band_summary(rows):
    """PROC MEANS NWAY CLASS LTV_BAND + PROC SORT BY LTV_BAND."""
    groups = {}
    for r in rows:
        groups.setdefault(r["LTV_BAND"], []).append(r)

    def sas_mean(values):
        present = [v for v in values if v is not MISSING]
        return sum(present) / len(present) if present else MISSING

    summary = []
    for band in sorted(groups):
        g = groups[band]
        summary.append({
            "LTV_BAND": band,
            "N_LOANS": sum(1 for r in g if r["LOAN"] is not MISSING),
            "AVG_LOAN": sas_mean([r["LOAN"] for r in g]),
            "DEFAULT_RATE": sas_mean([r["BAD"] for r in g]),
        })
    return summary


def write_expected(summary, path=EXPECTED_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["LTV_BAND", "N_LOANS", "AVG_LOAN", "DEFAULT_RATE"])
        writer.writeheader()
        for row in summary:
            writer.writerow({**row, "AVG_LOAN": repr(row["AVG_LOAN"]),
                             "DEFAULT_RATE": repr(row["DEFAULT_RATE"])})


if __name__ == "__main__":
    result = ltv_band_summary(ltv_band_rows())
    write_expected(result)
    for row in result:
        print(row)
    print(f"Wrote {EXPECTED_PATH}")
