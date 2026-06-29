"""
Generate the SAS baseline for module 04 (risk segmentation).

There is no SAS engine in CI, so this script encodes the logic of
sas/04_risk_segmentation.sas as a standalone pandas reference and writes the
default-rate-by-segment table that the SAS program would produce
(PROC MEANS: N and mean(BAD) by RISK_SEGMENT).

The output, tests/baselines/risk_segment_default_rate.csv, is the golden
"SAS baseline" that tests/test_pyspark_outputs.py asserts the PySpark
implementation matches. Re-run this only when the SAS logic or dataset changes.

Usage (from repo root):
    python tools/generate_sas_baseline.py
"""

import os

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(REPO_ROOT, "data", "home_equity.csv")
BASELINE_PATH = os.path.join(
    REPO_ROOT, "tests", "baselines", "risk_segment_default_rate.csv"
)


def ltv_component(ltv):
    if pd.isna(ltv):
        return 0.0
    if ltv >= 0.80:
        return 3.0
    if ltv >= 0.60:
        return 1.5
    return 0.0


def dti_component(debtinc):
    if pd.isna(debtinc):
        return 0.0
    if debtinc >= 50:
        return 3.0
    if debtinc >= 40:
        return 2.0
    if debtinc >= 30:
        return 1.0
    return 0.0


def delinq_component(delinq):
    if pd.isna(delinq):
        return 0.0
    if delinq >= 4:
        return 2.0
    if delinq >= 2:
        return 1.5
    if delinq == 1:
        return 0.5
    return 0.0


def derog_component(derog):
    if pd.isna(derog):
        return 0.0
    if derog >= 3:
        return 2.0
    if derog >= 1:
        return 1.0
    return 0.0


def risk_segment(score):
    if score < 3:
        return "Low Risk"
    if score < 5:
        return "Medium Risk"
    if score < 7:
        return "High Risk"
    return "Very High Risk"


def build_baseline(df):
    # Mirror the data preparation in pyspark/04_risk_segmentation.py
    df = df[
        df["LOAN"].notna()
        & df["VALUE"].notna()
        & df["BAD"].notna()
        & (df["LOAN"] > 0)
        & (df["VALUE"] > 0)
    ].copy()

    ltv = df["MORTDUE"] / df["VALUE"]
    ltv[~(df["MORTDUE"].notna() & df["VALUE"].notna() & (df["VALUE"] > 0))] = pd.NA

    df["RISK_SCORE"] = (
        ltv.map(ltv_component)
        + df["DEBTINC"].map(dti_component)
        + df["DELINQ"].map(delinq_component)
        + df["DEROG"].map(derog_component)
    )
    df["RISK_SEGMENT"] = df["RISK_SCORE"].map(risk_segment)

    grouped = (
        df.groupby("RISK_SEGMENT")
        .agg(N=("BAD", "size"), Default_Rate_Pct=("BAD", "mean"))
        .reset_index()
    )
    grouped["Default_Rate_Pct"] = (grouped["Default_Rate_Pct"] * 100).round(2)
    return grouped.sort_values("RISK_SEGMENT").reset_index(drop=True)


def main():
    df = pd.read_csv(DATA_PATH)
    baseline = build_baseline(df)
    os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
    baseline.to_csv(BASELINE_PATH, index=False)
    print(f"Wrote {BASELINE_PATH}")
    print(baseline.to_string(index=False))


if __name__ == "__main__":
    main()
