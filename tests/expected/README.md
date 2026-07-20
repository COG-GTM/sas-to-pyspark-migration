# SAS-parity expected outputs (golden oracle)

This directory holds the **golden/expected outputs** that later per-stage
migration PRs assert the PySpark scripts against. Each file captures what the SAS
programs in [`sas/`](../../sas) are expected to produce for the shipped dataset
[`data/home_equity.csv`](../../data/home_equity.csv) (5,960 rows × 18 columns).

## Provenance

**No live SAS environment is available in this repo/CI.** The expected values were
therefore **derived by faithfully replicating the logic of `sas/*.sas`** (the SAS
`DATA` steps and `PROC` logic) with PySpark, via
[`generate_expected.py`](./generate_expected.py). They are **not** copied from the
current `pyspark/*.py` scripts — those have known divergences from the SAS logic
(documented below) that the later per-stage PRs will fix; the oracle encodes the
**SAS** truth, so those PRs have something to converge on.

Regenerate (from the repo root) with:

```bash
python tests/expected/generate_expected.py
```

The generator is deterministic for the counts/aggregations. The stage-5 model
metrics depend on Spark's RNG/version — see tolerances below.

## Files

| File | Source SAS program | Contents |
|------|--------------------|----------|
| `stage1_data_loading.json` | `sas/01_data_loading.sas` (`PROC IMPORT`/`CONTENTS`) | Raw `row_count` (5960), `column_count` (18), and full schema (name/type/nullable). |
| `stage2_data_cleaning.json` | `sas/02_data_cleaning.sas` | Row counts after each filter step (raw → `home_equity_filtered` → `home_equity_final`), `LOAN_OUTCOME` counts, and 10 sample `LTV = MORTDUE/VALUE` values. |
| `stage3_aggregation_reporting.json` | `sas/03_aggregation_reporting.sas` | Top-10 states by avg `LOAN` (`HAVING COUNT(*) >= 10`), grouped summary stats (n/mean/std/min/median/max) by `LOAN_OUTCOME` and by `REASON`×`LOAN_OUTCOME`, and `PROC FREQ` tables. |
| `stage4_risk_segmentation.json` | `sas/04_risk_segmentation.sas` | `RISK_SCORE` range, `RISK_SEGMENT` counts, risk-category counts (LTV/DTI/DELINQ), and default rate + averages by segment. |
| `stage5_logistic_regression.json` | `sas/05_logistic_regression.sas` | Modeling-population counts and **reference** model AUC + confusion matrix with tolerances. |

## Derivation details (SAS logic replicated)

All of stages 2–5 operate on the SAS **`work.home_equity_final`** dataset:

1. Derive `LTV = MORTDUE / VALUE` (only when `VALUE`, `MORTDUE` non-missing and
   `VALUE > 0`) and `LOAN_OUTCOME` (`BAD=0`→`Paid`, `BAD=1`→`Default`).
2. `home_equity_filtered`: keep rows where `LOAN`, `VALUE`, `BAD` are non-missing → **5,848**.
3. `home_equity_final`: additionally keep `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` → **5,337**.
   (In SAS a missing `LTV` fails `LTV > 0` because missing sorts below 0, so those
   rows drop — the null-safe Spark comparison drops them identically.)

Categorical breakdowns represent a SAS missing/blank value as the string
`"(missing)"`.

### Stage 4 scoring (from the `PROC FORMAT` / scoring `DATA` step)

Composite `RISK_SCORE` = LTV component (0/1.5/3) + DTI component (0/1/2/3) +
delinquency component (0/0.5/1.5/2) + derogatory component (0/1/2); a missing
input contributes 0. `RISK_SEGMENT`: `<3` Low, `<5` Medium, `<7` High, else Very
High. Observed range on this dataset: **0.0 – 9.0**.

### Stage 5 tolerances (important)

SAS `PROC LOGISTIC ... SELECTION=STEPWISE` and PySpark's regularized
`LogisticRegression` (`elasticNetParam=0.8`) are **different algorithms**, so the
metrics are **not bit-identical** and must be compared with tolerance, not equality:

- `sas_model_data_count` (**3,881**) is the SAS modeling population: complete cases
  on `LOAN, MORTDUE, VALUE, DEBTINC, DELINQ, CLAGE`.
- `pyspark_reference_model_data_count` (**3,532**) additionally requires the
  categorical predictors (`DEROG, NINQ, JOB, REASON`) the PySpark pipeline
  one-hot encodes.
- `reference_auc` and `reference_confusion_matrix` come from the **PySpark
  reference pipeline** (`randomSplit([0.7, 0.3], seed=42)`), *not* from SAS.
- **Tolerance:** AUC within **±0.05**; confusion-matrix cell counts are
  **approximate** (they depend on the train/valid split and the Spark version).
  Treat AUC as the primary parity metric.
