# SAS -> PySpark parity validation

Independent validation of the five PySpark migration stages (`pyspark/01..05`) against their SAS
counterparts (`sas/01..05`). Each stage was validated in its own session (stage run recorded, real
bugs fixed in that stage's script only, parity evidence committed under `validation/<stage>/`), then
all five branches were merged into one tree and every stage, parity check and the pytest suite were
re-run from scratch on the merged tree. All numbers below come from that merged-tree run
(`validation/logs/`).

No SAS runtime was available. "SAS-expected" values are an independent pandas/csv recomputation of
the SAS semantics (PROC IMPORT guessing rules, PCTLDEF=5 percentiles, VARDEF=DF standard deviation,
CLASS/PROC FREQ missing-value handling, DATA-step filters) from `data/home_equity.csv`, not the
output of an actual SAS run.

## Environment

| Item | Value |
|---|---|
| OS | Ubuntu 22.04.5 LTS |
| Java | OpenJDK 17.0.20 (`/usr/lib/jvm/java-17-openjdk-amd64`) |
| Python | 3.12.13 |
| PySpark | 4.2.0 (local mode, headless) |
| pandas | 3.0.5 |
| scikit-learn | 1.9.1 |
| pytest | 7.3.2 |
| Base commit (`main`) | `32aca55aaf542c1159e9fda27297e8a30f413537` |
| Merged tree validated | `2bb9f975569de9a9cfb3b8a2bff04272b873e4f5` (merge of the five stage branches, see `validation/logs/environment.txt`) |

Reproduce with `bash validation/logs/run_all.sh` from the repo root.

## Headline result

| Item | Result |
|---|---|
| Stages exiting 0 (`python pyspark/<stage>.py`) | **5 / 5** (`validation/logs/exit_codes.txt`) |
| Parity checks matched, summed across stages | **593 / 593** (61 + 147 + 245 + 117 + 23) |
| Parity checks matched before the fixes | 223 / 593 (61 + 92 + 22 + 33 + 15; see each `validation/<stage>/parity_results_before_fixes.md`) |
| `pytest tests/test_pyspark_outputs.py -v` on merged tree | 13 passed, 1 failed (`validation/logs/pytest.txt`) |
| Same suite on untouched `main` (worktree of `32aca55`) | 13 passed, 1 failed — identical failure (`validation/logs/pytest_main.txt`) |
| Stages not validated | none |

Stage row bases confirmed on the merged tree: 5960 raw rows -> 5848 after LOAN/VALUE/BAD non-missing
-> 5337 after `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` (SAS `work.home_equity_final`) -> 3881 rows in
`work.model_data` -> 3532 complete cases used by PROC LOGISTIC.

| Stage | Exit code | Checks matched | Before fixes | Parity script | Log |
|---|---|---|---|---|---|
| 01_data_loading | 0 | 61 / 61 | 61 / 61 | `validation/01_data_loading/parity_check.py` | `validation/logs/01_data_loading.txt` |
| 02_data_cleaning | 0 | 147 / 147 | 92 / 147 | `validation/02_data_cleaning/parity_check.py` | `validation/logs/02_data_cleaning.txt` |
| 03_aggregation_reporting | 0 | 245 / 245 | 22 / 245 | `validation/03_aggregation_reporting/parity_check.py` | `validation/logs/03_aggregation_reporting.txt` |
| 04_risk_segmentation | 0 | 117 / 117 | 33 / 117 | `validation/04_risk_segmentation/parity_check.py` | `validation/logs/04_risk_segmentation.txt` |
| 05_logistic_regression | 0 | 23 / 23 | 15 / 23 | `validation/05_logistic_regression/parity_check.py` | `validation/logs/05_logistic_regression.txt` |

No stage's parity script regressed because of another stage's fix: every stage script is
self-contained (each re-reads `data/home_equity.csv` and re-applies the upstream filters), so the
merge was file-disjoint and the merged-tree results equal the per-stage results.

## Real discrepancies found (and fixed)

All fixes are in `pyspark/<stage>.py` only; `sas/`, `tests/` and `data/` are untouched.

### 01_data_loading — no discrepancies

61 / 61 matched before any change. Only edit: removed five unused `pyspark.sql.types` imports
(flake8 F401).

### 02_data_cleaning — 92 / 147 -> 147 / 147

| Symptom | Root cause | Fix |
|---|---|---|
| Percentiles wildly off (LTV p99 = 7.1625 vs SAS 0.9965, LOAN p1 = 1100 vs 3600, MORTDUE p99 = 399412 vs 232057) | `approxQuantile(..., relativeError=0.01)` is +/-58 ranks on 5848 rows, so p1/p99 collapse to min/max | `relativeError=0.0` (exact quantiles) |
| PROC MEANS percentiles printed for only 5 of the 8 `var` variables (CLAGE, DEROG, DELINQ missing) | hard-coded shorter column list | loop over the full `outlierCols` list |
| `nmiss` requested by both PROC MEANS calls never printed | `describe()` has no NMISS statistic | `describeWithNmiss()` helper unions a per-variable missing-count row |

### 03_aggregation_reporting — 22 / 245 -> 245 / 245

| Symptom | Root cause | Fix |
|---|---|---|
| Row base 5848 instead of SAS 5337; every FREQ/MEANS/TABULATE/SQL number off | upstream `if LTV > 0 and LTV < 5` from `02_data_cleaning.sas` dropped (in SAS it also implicitly drops MORTDUE-missing rows) | added `(col('LTV') > 0) & (col('LTV') < 5)` to the row filter |
| PROC FREQ tables showed a NULL level and percents used total rows as base (JOB Other 40.15% vs SAS 40.11%) | nulls not excluded from table / percent base | filter `isNotNull()`, base = non-missing count, print `Frequency Missing = n` like SAS |
| Step 2 PROC MEANS lacked Median for all vars and N/Std/Min/Max for MORTDUE/VALUE/DEBTINC; N was `count(LOAN)` | statistics missing, N not per-variable | long-format per-variable agg with N/Mean/Median (exact `percentile(x, 0.5)`)/Std/Min/Max |
| Step 3 PROC TABULATE printed NULL JOB rows and lacked the `all='Total'` margins | class-missing rows not dropped, totals not computed | filter JOB/REGION non-null, `cube('JOB','REGION')` with `coalesce(..., 'Total')` |
| Step 5 PROC MEANS printed NULL REASON rows, N was `count(*)` (DEBTINC has missing), no Median, no Std for LTV/DEBTINC | missing class handling and statistics | filter REASON non-null, long-format per-variable N/Mean/Std/Median |

### 04_risk_segmentation — 33 / 117 -> 117 / 117

| Symptom | Root cause | Fix |
|---|---|---|
| Row base 5848 instead of SAS 5337 | upstream `0 < LTV < 5` filter dropped | added `(LTV > 0) & (LTV < 5)` to the row filter |
| PROC FREQ tables listed a NULL level, percents used the full-row denominator | nullable category columns grouped directly, Percent divided by total count | per-variable `isNotNull` filter, non-missing count as percent base, print `Frequency Missing = n` |
| PROC MEANS output lacked Std and used `N_Obs` (`count('*')`) for every variable | stddev omitted, `count('*')` instead of per-variable non-missing count | per-variable union with `count(var)` as N, mean, sample stddev (SAS `n mean std`) |
| LTV x DTI cross-tab included missing-category groups and printed only aggregate rates | no `isNotNull` filter on class columns, no BAD cell counts | filter complete LTV_RISK_CAT/DTI_RISK_CAT/BAD rows, emit BAD_0/BAD_1/N/Default_Rate_Pct and Frequency Missing |

### 05_logistic_regression — 15 / 23 -> 23 / 23

| Symptom | Root cause | Fix |
|---|---|---|
| `model_data` had 3548 rows instead of SAS 3881 | upstream 02 filters (`0 < LTV < 5`, `LOAN > 0`, `VALUE > 0`) not inherited, and the 05 filter additionally dropped rows missing DEROG/NINQ/JOB/REASON, which the SAS DATA step does not | added LTV/LOAN/VALUE filters; `modelData` filter restricted to LOAN MORTDUE VALUE DEBTINC DELINQ CLAGE as in SAS |
| No counterpart to PROC LOGISTIC "Observations Used"; validation rows with missing predictors silently dropped from the confusion matrix | pipeline fit on all rows, missing predictors handled implicitly | fit on `train.dropna(modelVariables)`, print training rows used; score validation rows with missing predictors as PREDICTED_BAD = 0 / pred_prob missing (SAS DATA-step semantics) so the confusion matrix keeps every validation row |
| By-outcome pred_prob summary printed no N / p25 / median / p75 | `.agg({'pred_prob': 'count', 'pred_prob': 'mean'})` — duplicate dict keys keep only the last statistic | explicit aliased aggregations n/mean/std/min/p25/median/p75/max matching PROC MEANS |
| flake8 findings (unused `lit`, f-string without placeholders, late module-level `udf` import, duplicate dict key) | lint | fixed |

## Harmless differences

Differences between the PySpark output and the SAS output that are formatting / engine artefacts,
not logic errors. The parity scripts compare through them (tolerances, label-based matching).

- SAS display formats (`dollar12.`, `date9.`, `8.1`, `comma8.1`, `percent8.2`) are rendering-only; PySpark prints raw values (`25860.0` vs `$25,860`; APPDATE stays the SAS day count, e.g. `22040`; percentages are compared as `percent * 100`).
- Spark infers `integer` vs `double` per column where SAS has a single `Num` type; values identical. `describe()` labels `count`/`stddev` vs SAS `N`/`Std Dev`.
- Spark prints `NULL` where SAS prints `.` / blank for missing. Labels are dataset metadata in SAS; PySpark prints them from a dictionary.
- Percentiles: Spark exact `approxQuantile` returns the observation at rank `ceil(n*p)`; SAS PCTLDEF=5 averages neighbours when `n*p` is an integer (VALUE p25 66056 vs 66069, median 89231 vs 89235.5) — compared at 1% relative tolerance. Stage 05 uses Spark `percentile()` linear interpolation for p25/median/p75; checked for ordering/range, not exact equality.
- `show()` prints floats rounded to 2 (or 4) decimals; compared with half-ulp-of-printed-precision tolerance. Stage 03 Step 5 Mean/Std are rounded to 4 decimals instead of 2 so LTV is comparable.
- PROC FREQ orders levels alphabetically; PySpark orders by Frequency desc — compared by label.
- `initcap(CITY)` vs SAS `propcase(CITY)`: CITY is never printed by stage 02, so unverifiable; `propcase` also splits on `/ - ( . '` delimiters.
- The "Original rows / After filtering / Final clean rows" lines in stage 02 have no SAS printout; checked against the SAS row base 5960 / 5848 / 5337.
- Stage 05: PROC SURVEYSELECT `srs` draws exactly `round(0.7*3881) = 2717` training rows; Spark `randomSplit` is a Bernoulli split (2774 / 1107 after the fix, train share 0.7148 within +/-0.025). Split-dependent metrics are compared for plausibility only.
- Stage 05: Spark L1/elastic-net logistic regression vs SAS stepwise maximum likelihood — coefficient magnitudes differ; signs of non-zero numeric coefficients agree with an unpenalised scikit-learn fit. AUC 0.7533 vs sklearn baseline 0.7937 (tolerance 0.06), AUPR 0.4754 vs 0.4517. 16 one-hot features (StringIndexer + OneHotEncoder) vs 14 SAS `param=ref` design columns — equivalent model space.

## Pre-existing test-suite issues (not fixed)

`tests/test_pyspark_outputs.py` was deliberately left untouched.

- `test_frequency_counts` fails on untouched `main` and on the merged tree with the same assertion `5960 != 5681`: `groupBy('JOB').count()` includes the null group, so the summed counts equal the full row count, not the non-null JOB count. Unrelated to any stage script.
- The suite re-implements the transformations inline instead of importing/running `pyspark/*.py`, so it never exercises the migration scripts and could not detect any of the bugs above (approximate percentiles, dropped LTV filter, null-handling).
- `test_logistic_regression_model` builds model data by dropping rows missing any of the 10 model variables (incl. DEROG/NINQ/JOB/REASON) and omits the upstream `0 < LTV < 5` filter, i.e. it encodes the pre-fix 3548-row base rather than the SAS 3881-row contract. It still passes because it derives its own data.
- `pytest -k data` selects only `test_data_loads_successfully`; the other stage-01 tests need `-k "data or row_count or column"`.
- PySpark 4.2.0 emits `FutureWarning: PySpark does not yet fully support pandas >= 3.0.0` (pandas 3.0.5 installed); harmless.

## Stages not validated

None — all five stages produced usable output, exited 0 and reached full parity.

## Not verifiable without a SAS runtime

Exact SAS coefficient values, the stepwise variable-selection outcome, the exact SURVEYSELECT
sample, exact PROC MEANS percentile values (PCTLDEF=5 vs Spark rank/interpolation) and
`propcase(CITY)` vs `initcap(CITY)` were checked for plausibility / within tolerance only.

## Per-stage parity tables

Built from each `validation/<stage>/parity_results.md` as produced by the merged-tree run.

### 01_data_loading — 61 / 61 checks matched

Source: `validation/01_data_loading/parity_results.md` (log `validation/logs/01_data_loading.txt`)

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| PROC CONTENTS: Number of observations | 5960 | 5960 | Yes |
| PROC CONTENTS: Number of variables | 18 | 18 | Yes |
| PROC CONTENTS: Variable names (file order) | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | Yes |
| PROC CONTENTS type: BAD | Num | Num (integer) | Yes |
| PROC CONTENTS type: LOAN | Num | Num (integer) | Yes |
| PROC CONTENTS type: MORTDUE | Num | Num (double) | Yes |
| PROC CONTENTS type: VALUE | Num | Num (double) | Yes |
| PROC CONTENTS type: REASON | Char | Char (string) | Yes |
| PROC CONTENTS type: JOB | Char | Char (string) | Yes |
| PROC CONTENTS type: YOJ | Num | Num (double) | Yes |
| PROC CONTENTS type: DEROG | Num | Num (integer) | Yes |
| PROC CONTENTS type: DELINQ | Num | Num (integer) | Yes |
| PROC CONTENTS type: CLAGE | Num | Num (double) | Yes |
| PROC CONTENTS type: NINQ | Num | Num (integer) | Yes |
| PROC CONTENTS type: CLNO | Num | Num (integer) | Yes |
| PROC CONTENTS type: DEBTINC | Num | Num (double) | Yes |
| PROC CONTENTS type: APPDATE | Num | Num (integer) | Yes |
| PROC CONTENTS type: CITY | Char | Char (string) | Yes |
| PROC CONTENTS type: STATE | Char | Char (string) | Yes |
| PROC CONTENTS type: DIVISION | Char | Char (string) | Yes |
| PROC CONTENTS type: REGION | Char | Char (string) | Yes |
| LABEL: BAD | Loan Status (1=Default, 0=Paid) | Loan Status (1=Default, 0=Paid) | Yes |
| LABEL: LOAN | Amount of Loan Request | Amount of Loan Request | Yes |
| LABEL: MORTDUE | Amount Due on Existing Mortgage | Amount Due on Existing Mortgage | Yes |
| LABEL: VALUE | Value of Current Property | Value of Current Property | Yes |
| LABEL: REASON | Loan Purpose (HomeImp or DebtCon) | Loan Purpose (HomeImp or DebtCon) | Yes |
| LABEL: JOB | Job Category | Job Category | Yes |
| LABEL: YOJ | Years at Present Job | Years at Present Job | Yes |
| LABEL: DEROG | Number of Derogatory Reports | Number of Derogatory Reports | Yes |
| LABEL: DELINQ | Number of Delinquent Credit Lines | Number of Delinquent Credit Lines | Yes |
| LABEL: CLAGE | Age of Oldest Credit Line (months) | Age of Oldest Credit Line (months) | Yes |
| LABEL: NINQ | Number of Recent Credit Inquiries | Number of Recent Credit Inquiries | Yes |
| LABEL: CLNO | Number of Credit Lines | Number of Credit Lines | Yes |
| LABEL: DEBTINC | Debt to Income Ratio | Debt to Income Ratio | Yes |
| LABEL: APPDATE | Loan Application Date | Loan Application Date | Yes |
| LABEL: CITY | City | City | Yes |
| LABEL: STATE | State | State | Yes |
| LABEL: DIVISION | Census Division | Census Division | Yes |
| LABEL: REGION | Census Region | Census Region | Yes |
| PROC PRINT: Preview header | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | Yes |
| PROC PRINT: Preview row count | 20 | 20 | Yes |
| PROC PRINT: Obs 1 (18 cells) | BAD=1 LOAN=1100 CITY=oregon | BAD=1 LOAN=1100 CITY=oregon | Yes |
| PROC PRINT: Obs 2 (18 cells) | BAD=1 LOAN=1300 CITY=churchton | BAD=1 LOAN=1300 CITY=churchton | Yes |
| PROC PRINT: Obs 3 (18 cells) | BAD=1 LOAN=1500 CITY=orcas | BAD=1 LOAN=1500 CITY=orcas | Yes |
| PROC PRINT: Obs 4 (18 cells) | BAD=1 LOAN=1500 CITY=hastings | BAD=1 LOAN=1500 CITY=hastings | Yes |
| PROC PRINT: Obs 5 (18 cells) | BAD=0 LOAN=1700 CITY=wilmington | BAD=0 LOAN=1700 CITY=wilmington | Yes |
| PROC PRINT: Obs 6 (18 cells) | BAD=1 LOAN=1700 CITY=olympia | BAD=1 LOAN=1700 CITY=olympia | Yes |
| PROC PRINT: Obs 7 (18 cells) | BAD=1 LOAN=1800 CITY=williston | BAD=1 LOAN=1800 CITY=williston | Yes |
| PROC PRINT: Obs 8 (18 cells) | BAD=1 LOAN=1800 CITY=longview | BAD=1 LOAN=1800 CITY=longview | Yes |
| PROC PRINT: Obs 9 (18 cells) | BAD=1 LOAN=2000 CITY=elma | BAD=1 LOAN=2000 CITY=elma | Yes |
| PROC PRINT: Obs 10 (18 cells) | BAD=1 LOAN=2000 CITY=montclair | BAD=1 LOAN=2000 CITY=montclair | Yes |
| PROC PRINT: Obs 11 (18 cells) | BAD=1 LOAN=2000 CITY=whittier | BAD=1 LOAN=2000 CITY=whittier | Yes |
| PROC PRINT: Obs 12 (18 cells) | BAD=1 LOAN=2000 CITY=orlando | BAD=1 LOAN=2000 CITY=orlando | Yes |
| PROC PRINT: Obs 13 (18 cells) | BAD=1 LOAN=2000 CITY=elm grove | BAD=1 LOAN=2000 CITY=elm grove | Yes |
| PROC PRINT: Obs 14 (18 cells) | BAD=0 LOAN=2000 CITY=hatboro | BAD=0 LOAN=2000 CITY=hatboro | Yes |
| PROC PRINT: Obs 15 (18 cells) | BAD=1 LOAN=2100 CITY=linden | BAD=1 LOAN=2100 CITY=linden | Yes |
| PROC PRINT: Obs 16 (18 cells) | BAD=1 LOAN=2200 CITY=wildomar | BAD=1 LOAN=2200 CITY=wildomar | Yes |
| PROC PRINT: Obs 17 (18 cells) | BAD=1 LOAN=2200 CITY=minier | BAD=1 LOAN=2200 CITY=minier | Yes |
| PROC PRINT: Obs 18 (18 cells) | BAD=1 LOAN=2200 CITY=los banos | BAD=1 LOAN=2200 CITY=los banos | Yes |
| PROC PRINT: Obs 19 (18 cells) | BAD=1 LOAN=2300 CITY=anchorage | BAD=1 LOAN=2300 CITY=anchorage | Yes |
| PROC PRINT: Obs 20 (18 cells) | BAD=0 LOAN=2300 CITY=lufkin | BAD=0 LOAN=2300 CITY=lufkin | Yes |

### 02_data_cleaning — 147 / 147 checks matched

Source: `validation/02_data_cleaning/parity_results.md` (log `validation/logs/02_data_cleaning.txt`)

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| rows.original | 5960 | 5960 | Yes |
| rows.filtered | 5848 | 5848 | Yes |
| rows.final | 5337 | 5337 | Yes |
| outlier.LOAN.count | 5848 | 5848 | Yes |
| outlier.LOAN.nmiss | 0 | 0 | Yes |
| outlier.LOAN.mean | 18598.08482 | 18598.08482 | Yes |
| outlier.LOAN.stddev | 11195.20239 | 11195.20239 | Yes |
| outlier.LOAN.min | 1100 | 1100 | Yes |
| outlier.LOAN.max | 89900 | 89900 | Yes |
| pctl.LOAN.p1 | 3600 | 3600 | Yes |
| pctl.LOAN.p5 | 5900 | 5900 | Yes |
| pctl.LOAN.p25 | 11100 | 11100 | Yes |
| pctl.LOAN.median | 16400 | 16400 | Yes |
| pctl.LOAN.p75 | 23200 | 23200 | Yes |
| pctl.LOAN.p95 | 40000 | 40000 | Yes |
| pctl.LOAN.p99 | 62500 | 62500 | Yes |
| outlier.MORTDUE.count | 5357 | 5357 | Yes |
| outlier.MORTDUE.nmiss | 491 | 491 | Yes |
| outlier.MORTDUE.mean | 73762.97222 | 73762.97222 | Yes |
| outlier.MORTDUE.stddev | 44189.81431 | 44189.81431 | Yes |
| outlier.MORTDUE.min | 2063 | 2063 | Yes |
| outlier.MORTDUE.max | 399412 | 399412 | Yes |
| pctl.MORTDUE.p1 | 8117 | 8117 | Yes |
| pctl.MORTDUE.p5 | 18371 | 18371 | Yes |
| pctl.MORTDUE.p25 | 46466 | 46466 | Yes |
| pctl.MORTDUE.median | 65021 | 65021 | Yes |
| pctl.MORTDUE.p75 | 91350 | 91350 | Yes |
| pctl.MORTDUE.p95 | 152029 | 152029 | Yes |
| pctl.MORTDUE.p99 | 232057 | 232057 | Yes |
| outlier.VALUE.count | 5848 | 5848 | Yes |
| outlier.VALUE.nmiss | 0 | 0 | Yes |
| outlier.VALUE.mean | 101776.0487 | 101776.0487 | Yes |
| outlier.VALUE.stddev | 57385.77533 | 57385.77533 | Yes |
| outlier.VALUE.min | 8000 | 8000 | Yes |
| outlier.VALUE.max | 855909 | 855909 | Yes |
| pctl.VALUE.p1 | 26140 | 26140 | Yes |
| pctl.VALUE.p5 | 39050 | 39050 | Yes |
| pctl.VALUE.p25 | 66069 | 66056 | Yes |
| pctl.VALUE.median | 89235.5 | 89231 | Yes |
| pctl.VALUE.p75 | 119831.5 | 119817 | Yes |
| pctl.VALUE.p95 | 203720 | 203720 | Yes |
| pctl.VALUE.p99 | 289991 | 289991 | Yes |
| outlier.DEBTINC.count | 4662 | 4662 | Yes |
| outlier.DEBTINC.nmiss | 1186 | 1186 | Yes |
| outlier.DEBTINC.mean | 33.80941834 | 33.80941834 | Yes |
| outlier.DEBTINC.stddev | 8.564806248 | 8.564806248 | Yes |
| outlier.DEBTINC.min | 0.7202950067 | 0.7202950067 | Yes |
| outlier.DEBTINC.max | 203.3121487 | 203.3121487 | Yes |
| pctl.DEBTINC.p1 | 13.34721268 | 13.34721268 | Yes |
| pctl.DEBTINC.p5 | 20.58821436 | 20.58821436 | Yes |
| pctl.DEBTINC.p25 | 29.16004451 | 29.16004451 | Yes |
| pctl.DEBTINC.median | 34.83434725 | 34.83386481 | Yes |
| pctl.DEBTINC.p75 | 39.00850811 | 39.00850811 | Yes |
| pctl.DEBTINC.p95 | 42.73666557 | 42.73666557 | Yes |
| pctl.DEBTINC.p99 | 49.20639579 | 49.20639579 | Yes |
| outlier.LTV.count | 5357 | 5357 | Yes |
| outlier.LTV.nmiss | 491 | 491 | Yes |
| outlier.LTV.mean | 0.7084935888 | 0.7084935888 | Yes |
| outlier.LTV.stddev | 0.3811153433 | 0.3811153433 | Yes |
| outlier.LTV.min | 0.02053798981 | 0.02053798981 | Yes |
| outlier.LTV.max | 7.1625 | 7.1625 | Yes |
| pctl.LTV.p1 | 0.1216376197 | 0.1216376197 | Yes |
| pctl.LTV.p5 | 0.3161372759 | 0.3161372759 | Yes |
| pctl.LTV.p25 | 0.6230004794 | 0.6230004794 | Yes |
| pctl.LTV.median | 0.71886121 | 0.71886121 | Yes |
| pctl.LTV.p75 | 0.7956138912 | 0.7956138912 | Yes |
| pctl.LTV.p95 | 0.8911175499 | 0.8911175499 | Yes |
| pctl.LTV.p99 | 0.9965048544 | 0.9965048544 | Yes |
| outlier.CLAGE.count | 5559 | 5559 | Yes |
| outlier.CLAGE.nmiss | 289 | 289 | Yes |
| outlier.CLAGE.mean | 180.0719771 | 180.0719771 | Yes |
| outlier.CLAGE.stddev | 86.03260651 | 86.03260651 | Yes |
| outlier.CLAGE.min | 0 | 0 | Yes |
| outlier.CLAGE.max | 1168.233561 | 1168.233561 | Yes |
| pctl.CLAGE.p1 | 29.85636517 | 29.85636517 | Yes |
| pctl.CLAGE.p5 | 68.89009617 | 68.89009617 | Yes |
| pctl.CLAGE.p25 | 115.1302076 | 115.1302076 | Yes |
| pctl.CLAGE.median | 173.6252843 | 173.6252843 | Yes |
| pctl.CLAGE.p75 | 232.2921386 | 232.2921386 | Yes |
| pctl.CLAGE.p95 | 322.0045958 | 322.0045958 | Yes |
| pctl.CLAGE.p99 | 402.0868105 | 402.0868105 | Yes |
| outlier.DEROG.count | 5168 | 5168 | Yes |
| outlier.DEROG.nmiss | 680 | 680 | Yes |
| outlier.DEROG.mean | 0.2409055728 | 0.2409055728 | Yes |
| outlier.DEROG.stddev | 0.815596061 | 0.815596061 | Yes |
| outlier.DEROG.min | 0 | 0 | Yes |
| outlier.DEROG.max | 10 | 10 | Yes |
| pctl.DEROG.p1 | 0 | 0 | Yes |
| pctl.DEROG.p5 | 0 | 0 | Yes |
| pctl.DEROG.p25 | 0 | 0 | Yes |
| pctl.DEROG.median | 0 | 0 | Yes |
| pctl.DEROG.p75 | 0 | 0 | Yes |
| pctl.DEROG.p95 | 2 | 2 | Yes |
| pctl.DEROG.p99 | 4 | 4 | Yes |
| outlier.DELINQ.count | 5292 | 5292 | Yes |
| outlier.DELINQ.nmiss | 556 | 556 | Yes |
| outlier.DELINQ.mean | 0.4232804233 | 0.4232804233 | Yes |
| outlier.DELINQ.stddev | 1.078286216 | 1.078286216 | Yes |
| outlier.DELINQ.min | 0 | 0 | Yes |
| outlier.DELINQ.max | 15 | 15 | Yes |
| pctl.DELINQ.p1 | 0 | 0 | Yes |
| pctl.DELINQ.p5 | 0 | 0 | Yes |
| pctl.DELINQ.p25 | 0 | 0 | Yes |
| pctl.DELINQ.median | 0 | 0 | Yes |
| pctl.DELINQ.p75 | 0 | 0 | Yes |
| pctl.DELINQ.p95 | 3 | 3 | Yes |
| pctl.DELINQ.p99 | 5 | 5 | Yes |
| final.LOAN.count | 5337 | 5337 | Yes |
| final.LOAN.nmiss | 0 | 0 | Yes |
| final.LOAN.mean | 18556.34251 | 18556.34251 | Yes |
| final.LOAN.stddev | 11043.6573 | 11043.6573 | Yes |
| final.LOAN.min | 1100 | 1100 | Yes |
| final.LOAN.max | 89900 | 89900 | Yes |
| final.MORTDUE.count | 5337 | 5337 | Yes |
| final.MORTDUE.nmiss | 0 | 0 | Yes |
| final.MORTDUE.mean | 73245.88143 | 73245.88143 | Yes |
| final.MORTDUE.stddev | 43296.49949 | 43296.49949 | Yes |
| final.MORTDUE.min | 2063 | 2063 | Yes |
| final.MORTDUE.max | 399412 | 399412 | Yes |
| final.VALUE.count | 5337 | 5337 | Yes |
| final.VALUE.nmiss | 0 | 0 | Yes |
| final.VALUE.mean | 105255.4187 | 105255.4187 | Yes |
| final.VALUE.stddev | 54074.82651 | 54074.82651 | Yes |
| final.VALUE.min | 12737 | 12737 | Yes |
| final.VALUE.max | 512650 | 512650 | Yes |
| final.LTV.count | 5337 | 5337 | Yes |
| final.LTV.nmiss | 0 | 0 | Yes |
| final.LTV.mean | 0.6889890984 | 0.6889890984 | Yes |
| final.LTV.stddev | 0.2064954405 | 0.2064954405 | Yes |
| final.LTV.min | 0.02053798981 | 0.02053798981 | Yes |
| final.LTV.max | 4.705660674 | 4.705660674 | Yes |
| final.DEBTINC.count | 4253 | 4253 | Yes |
| final.DEBTINC.nmiss | 1084 | 1084 | Yes |
| final.DEBTINC.mean | 34.29305332 | 34.29305332 | Yes |
| final.DEBTINC.stddev | 8.297819138 | 8.297819138 | Yes |
| final.DEBTINC.min | 0.8381175254 | 0.8381175254 | Yes |
| final.DEBTINC.max | 203.3121487 | 203.3121487 | Yes |
| print.obs1 | 1 / 1100 / 25860 / 39025 / 0.6626521461 / Default / NULL / Other / HomeImp | 1 / 1100 / 25860.0 / 39025.0 / 0.6626521460602178 / Default / NULL / Other / HomeImp | Yes |
| print.obs2 | 1 / 1300 / 70053 / 68400 / 1.024166667 / Default / NULL / Other / HomeImp | 1 / 1300 / 70053.0 / 68400.0 / 1.0241666666666667 / Default / NULL / Other / HomeImp | Yes |
| print.obs3 | 1 / 1500 / 13500 / 16700 / 0.8083832335 / Default / NULL / Other / HomeImp | 1 / 1500 / 13500.0 / 16700.0 / 0.8083832335329342 / Default / NULL / Other / HomeImp | Yes |
| print.obs4 | 0 / 1700 / 97800 / 112000 / 0.8732142857 / Paid / NULL / Office / HomeImp | 0 / 1700 / 97800.0 / 112000.0 / 0.8732142857142857 / Paid / NULL / Office / HomeImp | Yes |
| print.obs5 | 1 / 1700 / 30548 / 40320 / 0.7576388889 / Default / 37.11361356 / Other / HomeImp | 1 / 1700 / 30548.0 / 40320.0 / 0.7576388888888889 / Default / 37.113613558 / Other / HomeImp | Yes |
| print.obs6 | 1 / 1800 / 48649 / 57037 / 0.8529375668 / Default / NULL / Other / HomeImp | 1 / 1800 / 48649.0 / 57037.0 / 0.8529375668425758 / Default / NULL / Other / HomeImp | Yes |
| print.obs7 | 1 / 1800 / 28502 / 43034 / 0.6623135195 / Default / 36.88489409 / Other / HomeImp | 1 / 1800 / 28502.0 / 43034.0 / 0.6623135195426871 / Default / 36.884894093 / Other / HomeImp | Yes |
| print.obs8 | 1 / 2000 / 32700 / 46740 / 0.6996148909 / Default / NULL / Other / HomeImp | 1 / 2000 / 32700.0 / 46740.0 / 0.699614890885751 / Default / NULL / Other / HomeImp | Yes |
| print.obs9 | 1 / 2000 / 20627 / 29800 / 0.6921812081 / Default / NULL / Office / HomeImp | 1 / 2000 / 20627.0 / 29800.0 / 0.6921812080536913 / Default / NULL / Office / HomeImp | Yes |
| print.obs10 | 1 / 2000 / 45000 / 55000 / 0.8181818182 / Default / NULL / Other / HomeImp | 1 / 2000 / 45000.0 / 55000.0 / 0.8181818181818182 / Default / NULL / Other / HomeImp | Yes |

### 03_aggregation_reporting — 245 / 245 checks matched

Source: `validation/03_aggregation_reporting/parity_results.md` (log `validation/logs/03_aggregation_reporting.txt`)

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| FREQ JOB=Other Frequency | 2070 | 2070 | Yes |
| FREQ JOB=Other Percent | 40.1085 | 40.11 | Yes |
| FREQ JOB=ProfExe Frequency | 1242 | 1242 | Yes |
| FREQ JOB=ProfExe Percent | 24.0651 | 24.07 | Yes |
| FREQ JOB=Office Frequency | 871 | 871 | Yes |
| FREQ JOB=Office Percent | 16.8766 | 16.88 | Yes |
| FREQ JOB=Mgr Frequency | 704 | 704 | Yes |
| FREQ JOB=Mgr Percent | 13.6408 | 13.64 | Yes |
| FREQ JOB=Self Frequency | 176 | 176 | Yes |
| FREQ JOB=Self Percent | 3.4102 | 3.41 | Yes |
| FREQ JOB=Sales Frequency | 98 | 98 | Yes |
| FREQ JOB=Sales Percent | 1.8989 | 1.9 | Yes |
| FREQ JOB missing level excluded from table | no NULL row | no NULL row | Yes |
| FREQ JOB Frequency Missing | 176 | 176 | Yes |
| FREQ REASON=DebtCon Frequency | 3665 | 3665 | Yes |
| FREQ REASON=DebtCon Percent | 70.7802 | 70.78 | Yes |
| FREQ REASON=HomeImp Frequency | 1513 | 1513 | Yes |
| FREQ REASON=HomeImp Percent | 29.2198 | 29.22 | Yes |
| FREQ REASON missing level excluded from table | no NULL row | no NULL row | Yes |
| FREQ REASON Frequency Missing | 159 | 159 | Yes |
| FREQ LOAN_OUTCOME=Paid Frequency | 4340 | 4340 | Yes |
| FREQ LOAN_OUTCOME=Paid Percent | 81.3191 | 81.32 | Yes |
| FREQ LOAN_OUTCOME=Default Frequency | 997 | 997 | Yes |
| FREQ LOAN_OUTCOME=Default Percent | 18.6809 | 18.68 | Yes |
| FREQ LOAN_OUTCOME missing level excluded from table | no NULL row | no NULL row | Yes |
| FREQ LOAN_OUTCOME Frequency Missing | 0 | 0 | Yes |
| FREQ REGION=South Frequency | 2039 | 2039 | Yes |
| FREQ REGION=South Percent | 38.205 | 38.2 | Yes |
| FREQ REGION=West Frequency | 1266 | 1266 | Yes |
| FREQ REGION=West Percent | 23.7212 | 23.72 | Yes |
| FREQ REGION=Midwest Frequency | 1112 | 1112 | Yes |
| FREQ REGION=Midwest Percent | 20.8357 | 20.84 | Yes |
| FREQ REGION=Northeast Frequency | 920 | 920 | Yes |
| FREQ REGION=Northeast Percent | 17.2381 | 17.24 | Yes |
| FREQ REGION missing level excluded from table | no NULL row | no NULL row | Yes |
| FREQ REGION Frequency Missing | 0 | 0 | Yes |
| MEANS Default LOAN N | 997 | 997 | Yes |
| MEANS Default LOAN Mean | 16621.9659 | 16621.97 | Yes |
| MEANS Default LOAN Median | 14800 | 14800.0 | Yes |
| MEANS Default LOAN Std | 10944.4714 | 10944.47 | Yes |
| MEANS Default LOAN Min | 1100 | 1100.0 | Yes |
| MEANS Default LOAN Max | 77400 | 77400.0 | Yes |
| MEANS Default MORTDUE N | 997 | 997 | Yes |
| MEANS Default MORTDUE Mean | 69092.824 | 69092.82 | Yes |
| MEANS Default MORTDUE Median | 60000 | 60000.0 | Yes |
| MEANS Default MORTDUE Std | 46498.0801 | 46498.08 | Yes |
| MEANS Default MORTDUE Min | 2063 | 2063.0 | Yes |
| MEANS Default MORTDUE Max | 399412 | 399412.0 | Yes |
| MEANS Default VALUE N | 997 | 997 | Yes |
| MEANS Default VALUE Mean | 98876.569 | 98876.57 | Yes |
| MEANS Default VALUE Median | 84624 | 84624.0 | Yes |
| MEANS Default VALUE Std | 58832.9772 | 58832.98 | Yes |
| MEANS Default VALUE Min | 16020 | 16020.0 | Yes |
| MEANS Default VALUE Max | 512650 | 512650.0 | Yes |
| MEANS Default DEBTINC N | 350 | 350 | Yes |
| MEANS Default DEBTINC Mean | 40.5208 | 40.52 | Yes |
| MEANS Default DEBTINC Median | 38.3589 | 38.36 | Yes |
| MEANS Default DEBTINC Std | 17.9789 | 17.98 | Yes |
| MEANS Default DEBTINC Min | 0.8381 | 0.84 | Yes |
| MEANS Default DEBTINC Max | 203.3121 | 203.31 | Yes |
| MEANS Paid LOAN N | 4340 | 4340 | Yes |
| MEANS Paid LOAN Mean | 19000.7143 | 19000.71 | Yes |
| MEANS Paid LOAN Median | 16900 | 16900.0 | Yes |
| MEANS Paid LOAN Std | 11019.7006 | 11019.7 | Yes |
| MEANS Paid LOAN Min | 1700 | 1700.0 | Yes |
| MEANS Paid LOAN Max | 89900 | 89900.0 | Yes |
| MEANS Paid MORTDUE N | 4340 | 4340 | Yes |
| MEANS Paid MORTDUE Mean | 74199.9363 | 74199.94 | Yes |
| MEANS Paid MORTDUE Median | 66724 | 66724.0 | Yes |
| MEANS Paid MORTDUE Std | 42475.3385 | 42475.34 | Yes |
| MEANS Paid MORTDUE Min | 2619 | 2619.0 | Yes |
| MEANS Paid MORTDUE Max | 371003 | 371003.0 | Yes |
| MEANS Paid VALUE N | 4340 | 4340 | Yes |
| MEANS Paid VALUE Mean | 106720.7904 | 106720.79 | Yes |
| MEANS Paid VALUE Median | 94273 | 94273.0 | Yes |
| MEANS Paid VALUE Std | 52819.9195 | 52819.92 | Yes |
| MEANS Paid VALUE Min | 12737 | 12737.0 | Yes |
| MEANS Paid VALUE Max | 471827 | 471827.0 | Yes |
| MEANS Paid DEBTINC N | 3903 | 3903 | Yes |
| MEANS Paid DEBTINC Mean | 33.7346 | 33.73 | Yes |
| MEANS Paid DEBTINC Median | 34.926 | 34.93 | Yes |
| MEANS Paid DEBTINC Std | 6.506 | 6.51 | Yes |
| MEANS Paid DEBTINC Min | 4.03 | 4.03 | Yes |
| MEANS Paid DEBTINC Max | 45.5698 | 45.57 | Yes |
| TABULATE JOB=Mgr REGION=Midwest N | 150 | 150 | Yes |
| TABULATE JOB=Mgr REGION=Midwest Default_Rate_Pct | 21.3333 | 21.33 | Yes |
| TABULATE JOB=Mgr REGION=Northeast N | 130 | 130 | Yes |
| TABULATE JOB=Mgr REGION=Northeast Default_Rate_Pct | 26.9231 | 26.92 | Yes |
| TABULATE JOB=Mgr REGION=South N | 265 | 265 | Yes |
| TABULATE JOB=Mgr REGION=South Default_Rate_Pct | 19.6226 | 19.62 | Yes |
| TABULATE JOB=Mgr REGION=Total N | 704 | 704 | Yes |
| TABULATE JOB=Mgr REGION=Total Default_Rate_Pct | 21.875 | 21.88 | Yes |
| TABULATE JOB=Mgr REGION=West N | 159 | 159 | Yes |
| TABULATE JOB=Mgr REGION=West Default_Rate_Pct | 22.0126 | 22.01 | Yes |
| TABULATE JOB=Office REGION=Midwest N | 195 | 195 | Yes |
| TABULATE JOB=Office REGION=Midwest Default_Rate_Pct | 14.8718 | 14.87 | Yes |
| TABULATE JOB=Office REGION=Northeast N | 145 | 145 | Yes |
| TABULATE JOB=Office REGION=Northeast Default_Rate_Pct | 8.9655 | 8.97 | Yes |
| TABULATE JOB=Office REGION=South N | 324 | 324 | Yes |
| TABULATE JOB=Office REGION=South Default_Rate_Pct | 12.3457 | 12.35 | Yes |
| TABULATE JOB=Office REGION=Total N | 871 | 871 | Yes |
| TABULATE JOB=Office REGION=Total Default_Rate_Pct | 12.1699 | 12.17 | Yes |
| TABULATE JOB=Office REGION=West N | 207 | 207 | Yes |
| TABULATE JOB=Office REGION=West Default_Rate_Pct | 11.5942 | 11.59 | Yes |
| TABULATE JOB=Other REGION=Midwest N | 399 | 399 | Yes |
| TABULATE JOB=Other REGION=Midwest Default_Rate_Pct | 20.5514 | 20.55 | Yes |
| TABULATE JOB=Other REGION=Northeast N | 365 | 365 | Yes |
| TABULATE JOB=Other REGION=Northeast Default_Rate_Pct | 21.9178 | 21.92 | Yes |
| TABULATE JOB=Other REGION=South N | 801 | 801 | Yes |
| TABULATE JOB=Other REGION=South Default_Rate_Pct | 20.5993 | 20.6 | Yes |
| TABULATE JOB=Other REGION=Total N | 2070 | 2070 | Yes |
| TABULATE JOB=Other REGION=Total Default_Rate_Pct | 21.9324 | 21.93 | Yes |
| TABULATE JOB=Other REGION=West N | 505 | 505 | Yes |
| TABULATE JOB=Other REGION=West Default_Rate_Pct | 25.1485 | 25.15 | Yes |
| TABULATE JOB=ProfExe REGION=Midwest N | 272 | 272 | Yes |
| TABULATE JOB=ProfExe REGION=Midwest Default_Rate_Pct | 13.9706 | 13.97 | Yes |
| TABULATE JOB=ProfExe REGION=Northeast N | 212 | 212 | Yes |
| TABULATE JOB=ProfExe REGION=Northeast Default_Rate_Pct | 13.2075 | 13.21 | Yes |
| TABULATE JOB=ProfExe REGION=South N | 470 | 470 | Yes |
| TABULATE JOB=ProfExe REGION=South Default_Rate_Pct | 16.383 | 16.38 | Yes |
| TABULATE JOB=ProfExe REGION=Total N | 1242 | 1242 | Yes |
| TABULATE JOB=ProfExe REGION=Total Default_Rate_Pct | 15.1369 | 15.14 | Yes |
| TABULATE JOB=ProfExe REGION=West N | 288 | 288 | Yes |
| TABULATE JOB=ProfExe REGION=West Default_Rate_Pct | 15.625 | 15.63 | Yes |
| TABULATE JOB=Sales REGION=Midwest N | 20 | 20 | Yes |
| TABULATE JOB=Sales REGION=Midwest Default_Rate_Pct | 25 | 25.0 | Yes |
| TABULATE JOB=Sales REGION=Northeast N | 17 | 17 | Yes |
| TABULATE JOB=Sales REGION=Northeast Default_Rate_Pct | 29.4118 | 29.41 | Yes |
| TABULATE JOB=Sales REGION=South N | 38 | 38 | Yes |
| TABULATE JOB=Sales REGION=South Default_Rate_Pct | 39.4737 | 39.47 | Yes |
| TABULATE JOB=Sales REGION=Total N | 98 | 98 | Yes |
| TABULATE JOB=Sales REGION=Total Default_Rate_Pct | 36.7347 | 36.73 | Yes |
| TABULATE JOB=Sales REGION=West N | 23 | 23 | Yes |
| TABULATE JOB=Sales REGION=West Default_Rate_Pct | 47.8261 | 47.83 | Yes |
| TABULATE JOB=Self REGION=Midwest N | 37 | 37 | Yes |
| TABULATE JOB=Self REGION=Midwest Default_Rate_Pct | 21.6216 | 21.62 | Yes |
| TABULATE JOB=Self REGION=Northeast N | 27 | 27 | Yes |
| TABULATE JOB=Self REGION=Northeast Default_Rate_Pct | 33.3333 | 33.33 | Yes |
| TABULATE JOB=Self REGION=South N | 70 | 70 | Yes |
| TABULATE JOB=Self REGION=South Default_Rate_Pct | 41.4286 | 41.43 | Yes |
| TABULATE JOB=Self REGION=Total N | 176 | 176 | Yes |
| TABULATE JOB=Self REGION=Total Default_Rate_Pct | 28.9773 | 28.98 | Yes |
| TABULATE JOB=Self REGION=West N | 42 | 42 | Yes |
| TABULATE JOB=Self REGION=West Default_Rate_Pct | 11.9048 | 11.9 | Yes |
| TABULATE JOB=Total REGION=Midwest N | 1073 | 1073 | Yes |
| TABULATE JOB=Total REGION=Midwest Default_Rate_Pct | 18.0801 | 18.08 | Yes |
| TABULATE JOB=Total REGION=Northeast N | 896 | 896 | Yes |
| TABULATE JOB=Total REGION=Northeast Default_Rate_Pct | 18.9732 | 18.97 | Yes |
| TABULATE JOB=Total REGION=South N | 1968 | 1968 | Yes |
| TABULATE JOB=Total REGION=South Default_Rate_Pct | 19.2073 | 19.21 | Yes |
| TABULATE JOB=Total REGION=Total N | 5161 | 5161 | Yes |
| TABULATE JOB=Total REGION=Total Default_Rate_Pct | 19.163 | 19.16 | Yes |
| TABULATE JOB=Total REGION=West N | 1224 | 1224 | Yes |
| TABULATE JOB=Total REGION=West Default_Rate_Pct | 20.1797 | 20.18 | Yes |
| TABULATE missing JOB class rows excluded | no NULL JOB rows | no NULL JOB rows | Yes |
| SQL top-10 state order | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, Delaware, District of Columbia, South Dakota, Connecticut, Pennsylvania | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, Delaware, District of Columbia, South Dakota, Connecticut, Pennsylvania | Yes |
| SQL Vermont num_loans | 11 | 11 | Yes |
| SQL Vermont avg_loan | 25672.7273 | 25672.73 | Yes |
| SQL Vermont avg_property_value | 116467.2727 | 116467.27 | Yes |
| SQL Vermont default_rate_pct | 9.0909 | 9.09 | Yes |
| SQL Rhode Island num_loans | 19 | 19 | Yes |
| SQL Rhode Island avg_loan | 23557.8947 | 23557.89 | Yes |
| SQL Rhode Island avg_property_value | 116219.6316 | 116219.63 | Yes |
| SQL Rhode Island default_rate_pct | 26.3158 | 26.32 | Yes |
| SQL Nebraska num_loans | 33 | 33 | Yes |
| SQL Nebraska avg_loan | 22106.0606 | 22106.06 | Yes |
| SQL Nebraska avg_property_value | 118060.8485 | 118060.85 | Yes |
| SQL Nebraska default_rate_pct | 12.1212 | 12.12 | Yes |
| SQL Arkansas num_loans | 47 | 47 | Yes |
| SQL Arkansas avg_loan | 21444.6809 | 21444.68 | Yes |
| SQL Arkansas avg_property_value | 102673.7234 | 102673.72 | Yes |
| SQL Arkansas default_rate_pct | 14.8936 | 14.89 | Yes |
| SQL Alaska num_loans | 13 | 13 | Yes |
| SQL Alaska avg_loan | 21215.3846 | 21215.38 | Yes |
| SQL Alaska avg_property_value | 106543.2308 | 106543.23 | Yes |
| SQL Alaska default_rate_pct | 7.6923 | 7.69 | Yes |
| SQL Delaware num_loans | 18 | 18 | Yes |
| SQL Delaware avg_loan | 20638.8889 | 20638.89 | Yes |
| SQL Delaware avg_property_value | 97622.0556 | 97622.06 | Yes |
| SQL Delaware default_rate_pct | 27.7778 | 27.78 | Yes |
| SQL District of Columbia num_loans | 12 | 12 | Yes |
| SQL District of Columbia avg_loan | 20575 | 20575.0 | Yes |
| SQL District of Columbia avg_property_value | 95766.75 | 95766.75 | Yes |
| SQL District of Columbia default_rate_pct | 16.6667 | 16.67 | Yes |
| SQL South Dakota num_loans | 15 | 15 | Yes |
| SQL South Dakota avg_loan | 20406.6667 | 20406.67 | Yes |
| SQL South Dakota avg_property_value | 115230.5333 | 115230.53 | Yes |
| SQL South Dakota default_rate_pct | 13.3333 | 13.33 | Yes |
| SQL Connecticut num_loans | 56 | 56 | Yes |
| SQL Connecticut avg_loan | 19894.6429 | 19894.64 | Yes |
| SQL Connecticut avg_property_value | 98450.1071 | 98450.11 | Yes |
| SQL Connecticut default_rate_pct | 32.1429 | 32.14 | Yes |
| SQL Pennsylvania num_loans | 207 | 207 | Yes |
| SQL Pennsylvania avg_loan | 19728.9855 | 19728.99 | Yes |
| SQL Pennsylvania avg_property_value | 110177.4302 | 110177.43 | Yes |
| SQL Pennsylvania default_rate_pct | 18.8406 | 18.84 | Yes |
| STEP5 DebtCon/Default LOAN N | 655 | 655 | Yes |
| STEP5 DebtCon/Default LOAN Mean | 18293.1298 | 18293.1298 | Yes |
| STEP5 DebtCon/Default LOAN Std | 10172.6006 | 10172.6006 | Yes |
| STEP5 DebtCon/Default LOAN Median | 16000 | 16000.0 | Yes |
| STEP5 DebtCon/Default LTV N | 655 | 655 | Yes |
| STEP5 DebtCon/Default LTV Mean | 0.7009 | 0.7009 | Yes |
| STEP5 DebtCon/Default LTV Std | 0.1698 | 0.1698 | Yes |
| STEP5 DebtCon/Default LTV Median | 0.7241 | 0.7241 | Yes |
| STEP5 DebtCon/Default DEBTINC N | 241 | 241 | Yes |
| STEP5 DebtCon/Default DEBTINC Mean | 40.8511 | 40.8511 | Yes |
| STEP5 DebtCon/Default DEBTINC Std | 17.2004 | 17.2004 | Yes |
| STEP5 DebtCon/Default DEBTINC Median | 39.1401 | 39.1401 | Yes |
| STEP5 DebtCon/Paid LOAN N | 3010 | 3010 | Yes |
| STEP5 DebtCon/Paid LOAN Mean | 19964.4186 | 19964.4186 | Yes |
| STEP5 DebtCon/Paid LOAN Std | 10456.5568 | 10456.5568 | Yes |
| STEP5 DebtCon/Paid LOAN Median | 18050 | 18050.0 | Yes |
| STEP5 DebtCon/Paid LTV N | 3010 | 3010 | Yes |
| STEP5 DebtCon/Paid LTV Mean | 0.698 | 0.698 | Yes |
| STEP5 DebtCon/Paid LTV Std | 0.2139 | 0.2139 | Yes |
| STEP5 DebtCon/Paid LTV Median | 0.7181 | 0.7181 | Yes |
| STEP5 DebtCon/Paid DEBTINC N | 2705 | 2705 | Yes |
| STEP5 DebtCon/Paid DEBTINC Mean | 33.9609 | 33.9609 | Yes |
| STEP5 DebtCon/Paid DEBTINC Std | 6.3842 | 6.3842 | Yes |
| STEP5 DebtCon/Paid DEBTINC Median | 35.1882 | 35.1882 | Yes |
| STEP5 HomeImp/Default LOAN N | 310 | 310 | Yes |
| STEP5 HomeImp/Default LOAN Mean | 12839.3548 | 12839.3548 | Yes |
| STEP5 HomeImp/Default LOAN Std | 11635.9642 | 11635.9642 | Yes |
| STEP5 HomeImp/Default LOAN Median | 10000 | 10000.0 | Yes |
| STEP5 HomeImp/Default LTV N | 310 | 310 | Yes |
| STEP5 HomeImp/Default LTV Mean | 0.6688 | 0.6688 | Yes |
| STEP5 HomeImp/Default LTV Std | 0.2059 | 0.2059 | Yes |
| STEP5 HomeImp/Default LTV Median | 0.7107 | 0.7107 | Yes |
| STEP5 HomeImp/Default DEBTINC N | 100 | 100 | Yes |
| STEP5 HomeImp/Default DEBTINC Mean | 38.4888 | 38.4888 | Yes |
| STEP5 HomeImp/Default DEBTINC Std | 17.3739 | 17.3739 | Yes |
| STEP5 HomeImp/Default DEBTINC Median | 36.3683 | 36.3683 | Yes |
| STEP5 HomeImp/Paid LOAN N | 1203 | 1203 | Yes |
| STEP5 HomeImp/Paid LOAN Mean | 16848.6284 | 16848.6284 | Yes |
| STEP5 HomeImp/Paid LOAN Std | 12409.8401 | 12409.8401 | Yes |
| STEP5 HomeImp/Paid LOAN Median | 13600 | 13600.0 | Yes |
| STEP5 HomeImp/Paid LTV N | 1203 | 1203 | Yes |
| STEP5 HomeImp/Paid LTV Mean | 0.6717 | 0.6717 | Yes |
| STEP5 HomeImp/Paid LTV Std | 0.2058 | 0.2058 | Yes |
| STEP5 HomeImp/Paid LTV Median | 0.7256 | 0.7256 | Yes |
| STEP5 HomeImp/Paid DEBTINC N | 1085 | 1085 | Yes |
| STEP5 HomeImp/Paid DEBTINC Mean | 33.2241 | 33.2241 | Yes |
| STEP5 HomeImp/Paid DEBTINC Std | 6.8738 | 6.8738 | Yes |
| STEP5 HomeImp/Paid DEBTINC Median | 34.3862 | 34.3862 | Yes |
| STEP5 missing REASON class rows excluded | no NULL REASON rows | no NULL REASON rows | Yes |

### 04_risk_segmentation — 117 / 117 checks matched

Source: `validation/04_risk_segmentation/parity_results.md` (log `validation/logs/04_risk_segmentation.txt`)

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| RISK_SEGMENT: Frequency[Low Risk] | 3118 | 3118 | Yes |
| RISK_SEGMENT: Percent[Low Risk] (base excludes missing) | 58.42 | 58.42 | Yes |
| RISK_SEGMENT: Frequency[Medium Risk] | 1776 | 1776 | Yes |
| RISK_SEGMENT: Percent[Medium Risk] (base excludes missing) | 33.28 | 33.28 | Yes |
| RISK_SEGMENT: Frequency[High Risk] | 427 | 427 | Yes |
| RISK_SEGMENT: Percent[High Risk] (base excludes missing) | 8 | 8 | Yes |
| RISK_SEGMENT: Frequency[Very High Risk] | 16 | 16 | Yes |
| RISK_SEGMENT: Percent[Very High Risk] (base excludes missing) | 0.3 | 0.3 | Yes |
| RISK_SEGMENT: table excludes missing level (PROC FREQ default) | excluded | excluded | Yes |
| RISK_SEGMENT: Frequency Missing | 0 | 0 | Yes |
| LTV_RISK_CAT: Frequency[Low] | 1131 | 1131 | Yes |
| LTV_RISK_CAT: Percent[Low] (base excludes missing) | 21.19 | 21.19 | Yes |
| LTV_RISK_CAT: Frequency[Medium] | 2967 | 2967 | Yes |
| LTV_RISK_CAT: Percent[Medium] (base excludes missing) | 55.59 | 55.59 | Yes |
| LTV_RISK_CAT: Frequency[High] | 1239 | 1239 | Yes |
| LTV_RISK_CAT: Percent[High] (base excludes missing) | 23.22 | 23.22 | Yes |
| LTV_RISK_CAT: table excludes missing level (PROC FREQ default) | excluded | excluded | Yes |
| LTV_RISK_CAT: Frequency Missing | 0 | 0 | Yes |
| DTI_RISK_CAT: Frequency[Low] | 1122 | 1122 | Yes |
| DTI_RISK_CAT: Percent[Low] (base excludes missing) | 26.38 | 26.38 | Yes |
| DTI_RISK_CAT: Frequency[Medium] | 2293 | 2293 | Yes |
| DTI_RISK_CAT: Percent[Medium] (base excludes missing) | 53.91 | 53.91 | Yes |
| DTI_RISK_CAT: Frequency[High] | 796 | 796 | Yes |
| DTI_RISK_CAT: Percent[High] (base excludes missing) | 18.72 | 18.72 | Yes |
| DTI_RISK_CAT: Frequency[Very High] | 42 | 42 | Yes |
| DTI_RISK_CAT: Percent[Very High] (base excludes missing) | 0.99 | 0.99 | Yes |
| DTI_RISK_CAT: table excludes missing level (PROC FREQ default) | excluded | excluded | Yes |
| DTI_RISK_CAT: Frequency Missing | 1084 | 1084 | Yes |
| DELINQ_RISK_CAT: Frequency[None] | 3872 | 3872 | Yes |
| DELINQ_RISK_CAT: Percent[None] (base excludes missing) | 78.27 | 78.27 | Yes |
| DELINQ_RISK_CAT: Frequency[Low] | 598 | 598 | Yes |
| DELINQ_RISK_CAT: Percent[Low] (base excludes missing) | 12.09 | 12.09 | Yes |
| DELINQ_RISK_CAT: Frequency[Medium] | 332 | 332 | Yes |
| DELINQ_RISK_CAT: Percent[Medium] (base excludes missing) | 6.71 | 6.71 | Yes |
| DELINQ_RISK_CAT: Frequency[High] | 145 | 145 | Yes |
| DELINQ_RISK_CAT: Percent[High] (base excludes missing) | 2.93 | 2.93 | Yes |
| DELINQ_RISK_CAT: table excludes missing level (PROC FREQ default) | excluded | excluded | Yes |
| DELINQ_RISK_CAT: Frequency Missing | 390 | 390 | Yes |
| ROW BASE: sum of RISK_SEGMENT frequencies == SAS home_equity_final rows | 5337 | 5337 | Yes |
| Average Default Rate by Risk Segment: Low Risk: N Obs | 3118 | 3118 | Yes |
| Average Default Rate by Risk Segment: Low Risk: N(BAD) (non-missing) | 3118 | 3118 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Mean(BAD) | 0.152341 | 0.1523 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Std(BAD) (sample, n-1) | 0.359409 | 0.3594 | Yes |
| Average Default Rate by Risk Segment: Low Risk: N(LOAN) (non-missing) | 3118 | 3118 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Mean(LOAN) | 20244.3 | 20244.3 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Std(LOAN) (sample, n-1) | 12018.1 | 12018.1 | Yes |
| Average Default Rate by Risk Segment: Low Risk: N(LTV) (non-missing) | 3118 | 3118 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Mean(LTV) | 0.612748 | 0.6127 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Std(LTV) (sample, n-1) | 0.166188 | 0.1662 | Yes |
| Average Default Rate by Risk Segment: Low Risk: N(DEBTINC) (non-missing) | 2409 | 2409 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Mean(DEBTINC) | 32.0719 | 32.0719 | Yes |
| Average Default Rate by Risk Segment: Low Risk: Std(DEBTINC) (sample, n-1) | 6.51328 | 6.5133 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: N Obs | 1776 | 1776 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: N(BAD) (non-missing) | 1776 | 1776 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Mean(BAD) | 0.216779 | 0.2168 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Std(BAD) (sample, n-1) | 0.412167 | 0.4122 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: N(LOAN) (non-missing) | 1776 | 1776 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Mean(LOAN) | 16580.5 | 16580.5 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Std(LOAN) (sample, n-1) | 9335.18 | 9335.18 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: N(LTV) (non-missing) | 1776 | 1776 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Mean(LTV) | 0.774845 | 0.7748 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Std(LTV) (sample, n-1) | 0.11686 | 0.1169 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: N(DEBTINC) (non-missing) | 1448 | 1448 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Mean(DEBTINC) | 36.0048 | 36.0048 | Yes |
| Average Default Rate by Risk Segment: Medium Risk: Std(DEBTINC) (sample, n-1) | 8.56746 | 8.5675 | Yes |
| Average Default Rate by Risk Segment: High Risk: N Obs | 427 | 427 | Yes |
| Average Default Rate by Risk Segment: High Risk: N(BAD) (non-missing) | 427 | 427 | Yes |
| Average Default Rate by Risk Segment: High Risk: Mean(BAD) | 0.283372 | 0.2834 | Yes |
| Average Default Rate by Risk Segment: High Risk: Std(BAD) (sample, n-1) | 0.451164 | 0.4512 | Yes |
| Average Default Rate by Risk Segment: High Risk: N(LOAN) (non-missing) | 427 | 427 | Yes |
| Average Default Rate by Risk Segment: High Risk: Mean(LOAN) | 14481.3 | 14481.3 | Yes |
| Average Default Rate by Risk Segment: High Risk: Std(LOAN) (sample, n-1) | 7228.4 | 7228.4 | Yes |
| Average Default Rate by Risk Segment: High Risk: N(LTV) (non-missing) | 427 | 427 | Yes |
| Average Default Rate by Risk Segment: High Risk: Mean(LTV) | 0.876325 | 0.8763 | Yes |
| Average Default Rate by Risk Segment: High Risk: Std(LTV) (sample, n-1) | 0.398217 | 0.3982 | Yes |
| Average Default Rate by Risk Segment: High Risk: N(DEBTINC) (non-missing) | 381 | 381 | Yes |
| Average Default Rate by Risk Segment: High Risk: Mean(DEBTINC) | 40.5917 | 40.5917 | Yes |
| Average Default Rate by Risk Segment: High Risk: Std(DEBTINC) (sample, n-1) | 7.74862 | 7.7486 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: N Obs | 16 | 16 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: N(BAD) (non-missing) | 16 | 16 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Mean(BAD) | 1 | 1 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Std(BAD) (sample, n-1) | 0 | 0 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: N(LOAN) (non-missing) | 16 | 16 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Mean(LOAN) | 17693.8 | 17693.8 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Std(LOAN) (sample, n-1) | 6972.85 | 6972.85 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: N(LTV) (non-missing) | 16 | 16 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Mean(LTV) | 1.01699 | 1.017 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Std(LTV) (sample, n-1) | 0.339205 | 0.3392 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: N(DEBTINC) (non-missing) | 15 | 15 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Mean(DEBTINC) | 65.7773 | 65.7773 | Yes |
| Average Default Rate by Risk Segment: Very High Risk: Std(DEBTINC) (sample, n-1) | 33.2453 | 33.2453 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Low x BAD=0 frequency | 325 | 325 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Low x BAD=1 frequency | 14 | 14 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Medium x BAD=0 frequency | 354 | 354 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Medium x BAD=1 frequency | 29 | 29 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x High x BAD=0 frequency | 131 | 131 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x High x BAD=1 frequency | 21 | 21 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Very High x BAD=0 frequency | 0 | 0 | Yes |
| Default Rates by LTV Risk and DTI Risk: Low x Very High x BAD=1 frequency | 9 | 9 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Low x BAD=0 frequency | 507 | 507 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Low x BAD=1 frequency | 26 | 26 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Medium x BAD=0 frequency | 1259 | 1259 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Medium x BAD=1 frequency | 69 | 69 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x High x BAD=0 frequency | 394 | 394 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x High x BAD=1 frequency | 59 | 59 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Very High x BAD=0 frequency | 0 | 0 | Yes |
| Default Rates by LTV Risk and DTI Risk: Medium x Very High x BAD=1 frequency | 20 | 20 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Low x BAD=0 frequency | 235 | 235 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Low x BAD=1 frequency | 15 | 15 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Medium x BAD=0 frequency | 538 | 538 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Medium x BAD=1 frequency | 44 | 44 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x High x BAD=0 frequency | 160 | 160 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x High x BAD=1 frequency | 31 | 31 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Very High x BAD=0 frequency | 0 | 0 | Yes |
| Default Rates by LTV Risk and DTI Risk: High x Very High x BAD=1 frequency | 13 | 13 | Yes |
| Default Rates by LTV Risk and DTI Risk: table excludes missing levels (PROC FREQ default) | excluded | excluded | Yes |
| Default Rates by LTV Risk and DTI Risk: Frequency Missing | 1084 | 1084 | Yes |

### 05_logistic_regression — 23 / 23 checks matched

Source: `validation/05_logistic_regression/parity_results.md` (log `validation/logs/05_logistic_regression.txt`)

| Metric | SAS-expected / independent | PySpark | Match? |
|---|---|---|---|
| Step 1 modeling dataset rows (work.model_data) | 3881 | 3881 | Yes |
| Step 2 train + valid == model rows | 3881 | 3881 | Yes |
| Step 2 training share (SURVEYSELECT samprate=0.7) | 0.7000 +/- 0.025 | 0.7148 | Yes |
| Step 2 validation share | 0.3000 +/- 0.025 | 0.2852 | Yes |
| Step 3 number of assembled features | 16 | 16 | Yes |
| Step 3 complete-case share of train used by fit (PROC LOGISTIC 'Number of Observations Used') | 0.9101 +/- 0.03 | 0.9092 | Yes |
| Step 3 non-zero numeric coefficient signs agree with unpenalised sklearn fit | same signs | DEBTINC: spark +0.0843 / sklearn +0.7537; DELINQ: spark +0.5379 / sklearn +0.5726; DEROG: spark +0.5371 / sklearn +0.3894; CLAGE: spark -0.0020 / sklearn -0.3367; NINQ: spark +0.0972 / sklearn +0.1333 | Yes |
| Step 6 confusion matrix total == validation rows (PROC FREQ keeps every valid row) | 1107 | 1107 | Yes |
| Step 6 validation BAD=1 share vs modeling base rate | 0.0858 +/- 0.03 | 0.0894 | Yes |
| Step 7 accuracy recomputed from confusion matrix | 0.9205 +/- 0.0005 | 0.9205 | Yes |
| Step 7 weightedPrecision recomputed from confusion matrix | 0.9164 +/- 0.0005 | 0.9164 | Yes |
| Step 7 weightedRecall recomputed from confusion matrix | 0.9205 +/- 0.0005 | 0.9205 | Yes |
| Step 7 f1 (weighted) recomputed from confusion matrix | 0.8928 +/- 0.0005 | 0.8928 | Yes |
| Step 7 AUC vs sklearn complete-case baseline (SAS c-statistic proxy) | 0.7937 +/- 0.06 | 0.7533 | Yes |
| Step 7 AUPR vs sklearn baseline | 0.4517 +/- 0.1 | 0.4754 | Yes |
| Step 8 N statistic present in by-outcome summary table (agg dict key collision) | printed | yes | Yes |
| Step 8 N(BAD=0)+N(BAD=1)+unscored == validation rows (PROC MEANS drops missing pred_prob) | 1107 | 1107 | Yes |
| Step 8 mean pred_prob: BAD=1 > BAD=0 | mean1 > mean0 | 0.0693 < 0.2388 | Yes |
| Step 8 pooled mean pred_prob ~ validation BAD rate (calibration) | 0.0894 +/- 0.03 | 0.0849 | Yes |
| Step 8 BAD=0: min <= p25 <= median <= p75 <= max within [0,1] | all printed & ordered | min=0.00532949, p25=0.0376735, median=0.0584201, p75=0.087595, max=0.565262 | Yes |
| Step 8 BAD=0: std printed and in (0, 0.5) | printed | 0.0536143 | Yes |
| Step 8 BAD=1: min <= p25 <= median <= p75 <= max within [0,1] | all printed & ordered | min=0.0144056, p25=0.0596143, median=0.129132, p75=0.33754, max=0.999084 | Yes |
| Step 8 BAD=1: std printed and in (0, 0.5) | printed | 0.248135 | Yes |
## Screenshots

Captured during each stage's recorded validation run (`validation/<stage>/screenshots/`).

### 01_data_loading

![Java version](validation/01_data_loading/screenshots/01_java_version.png)
![PySpark version](validation/01_data_loading/screenshots/02_pyspark_version.png)
![Stage run](validation/01_data_loading/screenshots/03_stage_run.png)
![pytest](validation/01_data_loading/screenshots/04_pytest.png)
![pytest stage 01](validation/01_data_loading/screenshots/05_pytest_stage01.png)
![Parity check](validation/01_data_loading/screenshots/06_parity_check.png)
![parity_results.md](validation/01_data_loading/screenshots/07_parity_results_md.png)

### 02_data_cleaning

![Versions](validation/02_data_cleaning/screenshots/01_versions.png)
![Stage run](validation/02_data_cleaning/screenshots/02_stage_run.png)
![pytest](validation/02_data_cleaning/screenshots/03_pytest.png)
![Parity check](validation/02_data_cleaning/screenshots/04_parity_check.png)

### 03_aggregation_reporting

![Java / PySpark versions](validation/03_aggregation_reporting/screenshots/01_java_pyspark_versions.png)
![Stage run started](validation/03_aggregation_reporting/screenshots/02_stage_run_started.png)
![Stage run complete, exit 0](validation/03_aggregation_reporting/screenshots/03_stage_run_complete_exit0.png)
![pytest aggregation tests](validation/03_aggregation_reporting/screenshots/04_pytest_aggregation_tests.png)
![Parity check](validation/03_aggregation_reporting/screenshots/05_parity_check.png)
![parity_results.md](validation/03_aggregation_reporting/screenshots/06_parity_results_md.png)

### 04_risk_segmentation

![Java / PySpark versions](validation/04_risk_segmentation/screenshots/01_java_pyspark_versions.png)
![Stage run tail](validation/04_risk_segmentation/screenshots/02_stage_run_tail.png)
![pytest risk tests](validation/04_risk_segmentation/screenshots/03_pytest_risk.png)
![Parity check](validation/04_risk_segmentation/screenshots/04_parity_check.png)

### 05_logistic_regression

![Java / PySpark versions](validation/05_logistic_regression/screenshots/01_java_pyspark_versions.png)
![Stage run](validation/05_logistic_regression/screenshots/02_stage_run.png)
![pytest logistic test](validation/05_logistic_regression/screenshots/03_pytest_logistic.png)
![Parity check](validation/05_logistic_regression/screenshots/04_parity_check.png)

## Recordings

Screen recordings of each stage's validation run (environment bring-up, stage run, pytest, parity check).

| Stage | Recording |
|---|---|
| 01_data_loading | https://app.devin.ai/attachments/0484c4a4-3dca-47f3-ad8f-32f1245214d5/rec-6b51f08d-ff32-41e4-af9a-e88abdbb0946-edited.mp4 |
| 02_data_cleaning | https://app.devin.ai/attachments/a1dcffb6-99c6-405d-9210-cb0cac944d99/rec-30805c87-e924-46d5-985f-a07a6684e2d9-edited.mp4 |
| 03_aggregation_reporting | https://app.devin.ai/attachments/f99e1eb9-0bf5-4568-b176-59c1cf3726fa/rec-66c73f6e-5fdd-42b3-a6cf-e1847298124f-edited.mp4 |
| 04_risk_segmentation | https://app.devin.ai/attachments/e5f032f9-08b6-43c7-bcba-694dafb6b4ac/rec-46a7942a-1189-4cbc-9e97-a1b332e71d34-edited.mp4 |
| 05_logistic_regression | https://app.devin.ai/attachments/7cc4e48e-2b35-4c5b-aa4c-a28cdb3ea814/parity_05_logistic_regression-edited.mp4 |
