# Validation: PySpark migration vs. SAS semantics

This document is the evidence that `pyspark/01..05` actually run, and that the
numbers they print are what the SAS programs under `sas/` would have produced.
Nothing here relies on the PySpark code checking itself: every figure in the
"Independent" column was recomputed a second way, with **pandas / plain Python /
scikit-learn straight off `data/home_equity.csv`**, following the semantics of
the SAS programs (not the PySpark code). The comparison itself is automated in
[`validation/parity_check.py`](validation/parity_check.py); the full 280-row
output is in [`validation/parity_results.md`](validation/parity_results.md).

Everything below was run on a single machine, in a visible terminal, with a
screen recording running (attached to the PR / delivered separately).

## Environment

| Item | Value |
|---|---|
| OS | Ubuntu Linux |
| Java | OpenJDK 17.0.20 (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`) |
| Python | 3.12.13 |
| PySpark | 4.2.0 |
| pandas / scikit-learn / pytest | 3.0.5 / 1.9.1 / 7.3.2 |
| Base commit validated | `32aca55` (origin/main) + the fixes in this PR |

![environment](validation/screenshots/00_environment.png)

## Headline result

* All five scripts run to completion with exit code 0.
* **280 / 280 independent parity checks match** after the fixes in this PR
  (105 / 281 matched on untouched `origin/main` – see "Real discrepancies").
* `tests/test_pyspark_outputs.py`: **13 passed, 1 failed**. The failure
  (`test_frequency_counts`) is a pre-existing bug in the *test*, reproduces
  identically on untouched `origin/main`, and is unrelated to the migration
  logic. It was **not** silently patched – see "Test-suite issue".

## Parity table (metric / SAS-expected or independent value / PySpark value / match?)

Independent values follow SAS semantics: `work.home_equity_final` is the raw
data with LOAN/VALUE/BAD non-missing, `LOAN > 0`, `VALUE > 0`, and
`0 < LTV < 5` (from `sas/02_data_cleaning.sas`). PROC FREQ excludes missing
values from the table and from the percent base by default; PROC MEANS
`class` statements drop rows with a missing class value.

### 01 – Data loading

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| Raw row count (HMEQ documented: 5,960) | 5,960 | 5,960 | yes |
| Column count | 18 | 18 | yes |
| BAD default rate (HMEQ documented: ~19.95%) | 19.95% (1,189 / 5,960) | 19.95% | yes |

### 02 – Cleaning / derived columns

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| Rows after LOAN/VALUE/BAD not-missing | 5,848 | 5,848 | yes |
| Final clean rows (`work.home_equity_final`) | 5,337 | 5,337 | yes |
| LTV = MORTDUE/VALUE spot check (25860 / 39025) | 0.6627 | 0.6627 | yes |
| LTV n / mean / std / min / max | 5,337 / 0.6890 / 0.2065 / 0.0205 / 4.7057 | same | yes |
| LOAN mean / std / min / max | 18,556.34 / 11,043.66 / 1,100 / 89,900 | same | yes |
| MORTDUE, VALUE, DEBTINC n/mean/std/min/max | see parity_results.md | same | yes (20/20) |

### 03 – Aggregation & reporting

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| JOB frequencies (6 levels) + percents, Frequency Missing = 176 | e.g. Other 2,070 (40.11%), ProfExe 1,242 (24.07%) | same | yes (14/14) |
| REASON frequencies, Frequency Missing = 159 | DebtCon 3,665 (70.78%), HomeImp 1,513 (29.22%) | same | yes |
| LOAN_OUTCOME frequencies | Paid 4,340 (81.32%), Default 997 (18.68%) | same | yes |
| REGION frequencies (4 levels) | South 2,039 (38.20%), West 1,266 (23.72%), ... | same | yes |
| Mean / median LOAN by outcome | Paid 19,000.71 / 16,900; Default 16,621.97 / 14,800 | same | yes |
| Mean / median DEBTINC by outcome | Paid 33.73 / 34.93; Default 40.52 / 38.36 | same | yes |
| **Mean / median LOAN by REASON** | DebtCon 19,665.73 / 17,700; HomeImp 16,027.16 / 12,900; missing 17,051.57 / 17,000 | same | yes |
| **Mean / median DEBTINC by REASON** | DebtCon 34.52 / 35.56; HomeImp 33.67 / 34.55; missing 34.77 / 34.41 | same | yes |
| **Mean / median LOAN by JOB** | Mgr 18,768.89 / 17,950; Office 18,163.26 / 16,200; Other 17,854.15 / 15,600; ProfExe 18,933.49 / 17,200; Sales 14,306.12 / 13,400; Self 27,469.32 / 23,650; missing 18,702.27 / 14,750 | same | yes |
| **Mean / median DEBTINC by JOB** | Mgr 35.34 / 35.74; Office 34.46 / 36.13; Other 34.70 / 35.79; ProfExe 32.65 / 33.46; Sales 39.12 / 37.76; Self 36.12 / 35.41; missing 32.46 / 32.56 | same | yes |
| REASON × outcome N / mean / median LOAN / mean LTV / mean+median DEBTINC | 36 values | same | yes (36/36) |
| Default % by JOB × REGION (PROC TABULATE) | 28 cells N + % | same | yes (56/56) |
| Top-10 states by avg LOAN (PROC SQL), order + values | Vermont 25,672.73 ... Pennsylvania 19,728.99 | same | yes (10/10) |

### 04 – Risk segmentation

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| Rows in risk dataset | 5,337 | 5,337 | yes |
| RISK_SEGMENT Low / Medium / High / Very High | 3,118 / 1,776 / 427 / 16 | 3,118 / 1,776 / 427 / 16 | yes |
| LTV_RISK_CAT Low / Medium / High | 1,131 / 2,967 / 1,239 | same | yes |
| DTI_RISK_CAT Low / Medium / High / Very High / missing | 1,122 / 2,293 / 796 / 42 / 1,084 | same | yes |
| DELINQ_RISK_CAT None / Low / Medium / High / missing | 3,872 / 598 / 332 / 145 / 390 | same | yes |
| Default % by segment | Low 15.23, Medium 21.68, High 28.34, Very High 100.0 | same | yes |
| Avg LOAN / LTV / DEBTINC by segment | 12 values | same | yes (12/12) |

### 05 – Logistic regression

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| Complete-case modelling rows (8 numeric + JOB + REASON non-missing) | 3,532 | 3,532 | yes |
| Train share | 0.70 (SAS `samprate=0.7` is exact) | 0.716 (2,528 / 3,532) | yes – approximate by design (`randomSplit` is Bernoulli) |
| Validation AUC – Spark ML vs scikit-learn LogisticRegression, same features, stratified 70/30 holdout | 0.797 (sklearn) | **0.8052** | plausible – same neighbourhood |
| Validation AUC – vs sklearn 5-fold CV mean (sd 0.015) | 0.784 ± 0.015 | 0.8052 | plausible |
| Confusion-matrix cells sum to validation rows | 1,004 | 907 + 2 + 80 + 15 = 1,004 | yes |
| Accuracy recomputed from confusion matrix | 0.9183 | 0.9183 | yes |

The AUC is not expected to be bit-identical: SAS `PROC SURVEYSELECT` draws an
exact 70% simple random sample, Spark `randomSplit` is a per-row Bernoulli
split, and Spark ML / sklearn / PROC LOGISTIC use different optimisers and
regularisation defaults. 0.80 vs 0.79–0.80 on ~1,000 validation rows is well
within the sampling noise (sd ≈ 0.015 across CV folds), so the Spark model is
credible.

## Real discrepancies found (and fixed in this PR)

Running the same harness against untouched `origin/main` gave
**105 / 281 matches** ([`validation/parity_results_before_fixes.md`](validation/parity_results_before_fixes.md)).
Every mismatch traced back to one of three genuine migration bugs:

1. **Missing SAS LTV sanity filter in 03, 04 and 05 (real bug).**
   `sas/02_data_cleaning.sas` builds `work.home_equity_final` with
   `0 < LTV < 5` in addition to the not-missing / `> 0` checks. The PySpark
   scripts 03–05 "replicate cleaning from script 02" but dropped that clause,
   so they operated on 5,848 rows instead of 5,337 (491 rows with missing
   MORTDUE/VALUE plus 20 with LTV ≥ 5 leaked in). Downstream effect on main:
   Default N 1,084 vs 997, risk dataset 5,848 vs 5,337, RISK_SEGMENT Low 3,603
   vs 3,118, modelling rows 3,548 vs 3,532, AUC 0.7642 vs 0.8052, and every
   percentage in 03/04 shifted. Fix: added `(LTV > 0) & (LTV < 5)` to the
   filter in `pyspark/03`, `04`, `05`.

2. **PROC FREQ missing-value semantics in 03 (real bug).**
   SAS PROC FREQ drops missing values from the table and from the percent
   denominator and prints `Frequency Missing = n`. The PySpark version kept a
   `NULL` row and divided by the full count (e.g. JOB=Other 40.15% vs 40.11%).
   Fix: filter nulls before the groupBy and print `Frequency Missing`.

3. **Medians missing from 03 (real gap).** `sas/03` runs `PROC MEANS n mean
   median ...`; the PySpark version only printed means. Fix: added
   `Median_LOAN` / `Median_DEBTINC` (Spark `median()`) to the by-outcome and
   REASON × outcome summaries.

4. **Duplicate dict key in 05 (real bug, output only).**
   `.agg({"pred_prob": "count", "pred_prob": "mean"})` – Python keeps only the
   last key, so the count column was silently dropped. Fix: explicit
   `count(...).alias("N"), mean(...).alias("Mean_pred_prob")`.

Nothing else was off. Missing-value handling on load, `inferSchema`, the LTV
formula, the risk-bucket thresholds, composite score and segment labels,
the JOB × REGION tabulate, the PROC SQL top-10 states and the SAS-style
half-up rounding all agree exactly.

## Harmless differences (not bugs)

* **Train/validation split**: 71.6% / 28.4% instead of exactly 70/30 –
  Bernoulli `randomSplit` vs SAS exact SRS. Expected.
* **AUC 0.8052 vs 0.797 (sklearn)** – different optimiser/regularisation and a
  different random split; within noise (see above).
* **Spark `NULL` vs SAS `.`** in printed output – display only.
* PySpark 4.2 prints a `FutureWarning` that pandas ≥ 3.0 is not fully supported
  by `pyspark.testing`; it only affects the test helper import, not the scripts.

## Test-suite issue (pre-existing, not changed here)

`tests/test_pyspark_outputs.py::test_frequency_counts` fails with
`AssertionError: 5960 != 5681`. It sums `df.groupBy("JOB").count()` (which
includes the `NULL` JOB group, 279 rows) and compares to
`df.filter(col("JOB").isNotNull()).count()`. Those can never be equal on data
with missing JOB, so the assertion is wrong, not the code under test. The same
failure occurs on untouched `origin/main` (screenshot below). Per the brief
("don't invent problems") the test was left as-is; the one-line fix is to
compare against `df.count()`.

## Terminal runs (screenshots)

`python pyspark/01_data_loading.py` – 5,960 rows × 18 columns

![01](validation/screenshots/01_data_loading.png)

`python pyspark/02_data_cleaning.py` – 5,960 → 5,848 → 5,337 rows

![02](validation/screenshots/02_data_cleaning.png)

`python pyspark/03_aggregation_reporting.py`

![03](validation/screenshots/03_aggregation_reporting.png)

`python pyspark/04_risk_segmentation.py`

![04](validation/screenshots/04_risk_segmentation.png)

`python pyspark/05_logistic_regression.py` – AUC 0.8052

![05](validation/screenshots/05_logistic_regression.png)

`python -m pytest tests/test_pyspark_outputs.py -v` – 13 passed, 1 failed

![pytest](validation/screenshots/06_pytest.png)

Same test on untouched `origin/main` (`32aca55`) – same failure

![pytest main](validation/screenshots/07_pytest_main_baseline.png)

`python validation/parity_check.py --logs validation/logs` – 280 / 280

![parity](validation/screenshots/08_parity_check.png)

## Reproduce

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # any Java 8/11/17
pip install pyspark pandas scikit-learn pytest
for s in pyspark/0*.py; do python "$s"; done
python -m pytest tests/test_pyspark_outputs.py -v
python validation/parity_check.py            # re-runs the scripts and compares
```

Raw terminal logs from the recorded run are in `validation/logs/`.
