# SAS → PySpark Migration Plan: Lending Analytics (HOME_EQUITY)

**Scope:** the five programs under `sas/` and their counterparts under `pyspark/`.
**Audience:** the lending analytics team (SAS-fluent) and the risk team that signs off on parity.
**Status of this document:** plan only. No code was changed while producing it.

Every number in this plan was checked against `data/home_equity.csv` (5,960 rows, 18 columns), the current `pyspark/` scripts, and one run of the existing test suite. Where something is an expectation rather than a measurement, it is marked *expected*.

---

## 0. How to read this if you know SAS but not Spark

| SAS idea | Closest Spark idea | What is different |
|---|---|---|
| WORK dataset | DataFrame | A DataFrame is a *recipe*, not a file. Nothing runs until you ask for output (`show`, `count`, `write`). |
| DATA step (row by row) | `withColumn` / `filter` chain | You describe the whole column at once; there is no implicit loop, no PDV, no `RETAIN`. |
| `.` (missing) | `null` | Both mean "no value", but **comparisons behave differently** — see §2.1. This is the single biggest source of silent wrong answers. |
| `CLASS` / `BY` | `groupBy` | `groupBy` keeps a group for `null`; `CLASS` drops missing by default. |
| PROC FREQ / MEANS / TABULATE | `groupBy().agg()`, `crosstab`, `rollup` | Same statistics, but you build the table shape yourself. |
| PROC SQL | `spark.sql(...)` | Nearly identical SQL. `outobs=` becomes `LIMIT`. |
| PROC FORMAT | `when(...).otherwise(...)` | No format catalog; bucketing is code. |
| LABEL / FORMAT | Column metadata / presentation layer | Spark stores data, not display rules. |
| PROC LOGISTIC | `pyspark.ml` `LogisticRegression` | Same model, different fitting engine, **no stepwise**, different categorical encoding defaults. |
| Macro variables / `%MACRO` | Python variables / functions | Straightforward. None are used in this codebase. |

---

## 1. Program inventory

### Pipeline shape

The SAS programs form a single chain through WORK datasets. The PySpark scripts do **not** — each one re-reads the CSV and re-implements cleaning (inconsistently; see §3).

```
01: home_equity.csv ──► work.home_equity
02: ──► home_equity_clean ──► home_equity_imputed ──► home_equity_filtered ──► home_equity_final
03: home_equity_final ──► reports only
04: home_equity_final ──► home_equity_risk
05: home_equity_risk ──► model_data ──► model_split ──► train / valid ──► logit_model, scored sets
```

Row counts at each stage (computed from the CSV, applying the SAS logic literally):

| Dataset | Rows | Rule |
|---|---|---|
| `home_equity` | 5,960 | raw |
| `home_equity_filtered` | 5,848 | `LOAN`, `VALUE`, `BAD` non-missing (drops 112 rows with missing `VALUE`) |
| `home_equity_final` | **5,337** | plus `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` (drops 491 with missing `LTV` and 20 with `LTV ≥ 5`) |
| `model_data` | 3,881 | plus `MORTDUE`, `DEBTINC`, `DELINQ`, `CLAGE` non-missing |
| rows PROC LOGISTIC actually fits on | 3,532 | PROC LOGISTIC silently drops any row missing *any* model variable (`DEROG`, `NINQ`, `JOB`, `REASON` too) |

Missing values in the raw file: `DEBTINC` 1,267 · `DEROG` 708 · `DELINQ` 580 · `MORTDUE` 518 · `YOJ` 515 · `NINQ` 510 · `CLAGE` 308 · `JOB` 279 · `REASON` 252 · `CLNO` 222 · `VALUE` 112 · `CITY` 12 · `BAD`/`LOAN`/`APPDATE`/`STATE`/`DIVISION`/`REGION` 0.

### 1.1 `01_data_loading.sas` — load and describe

| Category | Used |
|---|---|
| DATA steps | none |
| PROCs | `PROC IMPORT` (dbms=csv, `guessingrows=5960`), `PROC DATASETS … MODIFY` ×2, `PROC CONTENTS`, `PROC PRINT (obs=20)` |
| Macros / macro vars | none |
| Labels | 18 variable labels (one per column) |
| Formats | `dollar12.` (LOAN, MORTDUE, VALUE), `date9.` (APPDATE), `8.1` (DEBTINC), `comma8.1` (CLAGE) |
| SAS-specific functions | none |
| Outputs | `work.home_equity`; two listing reports |

Note: `APPDATE` in the CSV is a **SAS date serial** (days since 1960-01-01; values 20820–22645 = 2017-01-01 … 2022-01-01). The `date9.` format is what makes it readable in SAS. Any consumer outside SAS needs an explicit conversion.

### 1.2 `02_data_cleaning.sas` — derive, flag, filter

| Category | Used |
|---|---|
| DATA steps | 4 (`home_equity_clean`, `home_equity_imputed`, `home_equity_filtered`, `home_equity_final`) |
| PROCs | `PROC MEANS` ×2 (with `p1 p5 p25 median p75 p95 p99`, `nmiss`), `PROC PRINT (obs=10)` |
| Macros / macro vars | none |
| Labels | `LTV`, `LOAN_OUTCOME` |
| Formats | `percent8.2` on `LTV` |
| SAS-specific constructs | `LENGTH $7`, `ARRAY` ×2 with `DO i = 1 to 8`, subsetting `IF`, `propcase()`, `ne .` missing tests |
| Outputs | `work.home_equity_final` (5,337 rows) is the input for 03/04/05 |

Despite its name, `home_equity_imputed` does **not** impute — it only adds eight `*_MISS` 0/1 flags. Nothing downstream uses the flags.

### 1.3 `03_aggregation_reporting.sas` — reports only

| Category | Used |
|---|---|
| DATA steps | none |
| PROCs | `PROC FREQ` (`/ nocum`), `PROC MEANS` ×2 with `CLASS` (statistics incl. `median`), `PROC TABULATE` (nested `TABLE` with `all='Total'` margins, `mean*f=percent8.2`), `PROC SQL` (`outobs=10`, `having`, column formats) |
| Macros / macro vars | none |
| Formats | `dollar12.`, `percent8.2` on SQL result columns; `percent8.2` inside TABULATE |
| SAS-specific functions | `mean()` inside PROC SQL (ignores missing, same as SQL `AVG`) |
| Outputs | five listing reports; no datasets |

### 1.4 `04_risk_segmentation.sas` — bucketing and composite score

| Category | Used |
|---|---|
| DATA steps | 1 (`home_equity_risk`) |
| PROCs | `PROC FORMAT` (4 `VALUE` formats with `low -< x`, `x - high` ranges), `PROC FREQ` ×2 (one 3-way table `LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent`), `PROC MEANS` with `CLASS` |
| Macros / macro vars | none |
| Labels | 5 |
| Formats | `ltv_risk`, `dti_risk`, `delinq_risk`, `risk_score` — **defined but never applied**. The DATA step re-implements the same cut-points with `IF/ELSE IF`. The two definitions agree today; they are a maintenance hazard in SAS and simply collapse into one definition in PySpark. |
| SAS-specific constructs | `LENGTH $6 $9 $14`, `ne .` guards around every bucket, accumulator pattern `RISK_SCORE = RISK_SCORE + n` |
| Outputs | `work.home_equity_risk` (adds `LTV_RISK_CAT`, `DTI_RISK_CAT`, `DELINQ_RISK_CAT`, `RISK_SCORE` 0–10, `RISK_SEGMENT`) |

### 1.5 `05_logistic_regression.sas` — model, score, evaluate

| Category | Used |
|---|---|
| DATA steps | 3 (`model_data`, `train`/`valid` two-output step, `valid_scored` threshold step) |
| PROCs | `PROC SURVEYSELECT` (`method=srs samprate=0.7 seed=42`), `PROC LOGISTIC` ×2, `PROC PLM` (`restore=`, `score … / ilink`), `PROC FREQ` (2×2 confusion matrix), `PROC PRINT`, `PROC MEANS` with `CLASS` and percentiles |
| PROC LOGISTIC options | `descending`; `CLASS JOB(ref='Other') REASON(ref='HomeImp') / param=ref`; `selection=stepwise slentry=0.05 slstay=0.05`; `details`; `lackfit` (Hosmer–Lemeshow); `output out= predicted=`; `store` (item store); `roc`; `ods output Association=` |
| Macros / macro vars | none |
| Formats / labels | none |
| Outputs | `logit_model` item store, `train_scored`, `valid_scored`, `association_stats` |

Two things worth knowing before anyone compares numbers:

1. **Step 6 refits a brand-new model on the validation set** (`proc logistic data=work.valid … roc;`) and reports *that* model's c-statistic. It is not the out-of-sample AUC of the model trained in Step 3. The PySpark script evaluates the trained model on `valid`, which is the statistically correct thing but a *different number*. The team should decide which metric is the contract (recommendation: the out-of-sample one, with the SAS program corrected to match).
2. **Row-drop behaviour differs between the two engines by default.** `model_data` keeps 3,881 rows, SURVEYSELECT samples 70% of those, then PROC LOGISTIC quietly discards rows with missing `DEROG`/`NINQ`/`JOB`/`REASON`. Whatever we build in PySpark has to make the same rows disappear at the same point, or the training sets will not match.

### Constructs the customer asked about that are *not* present

- No `%LET`, `%MACRO`, `&var`, `%INCLUDE`, `CALL SYMPUT`.
- No `BY`-group processing, `FIRST.`/`LAST.`, `RETAIN`, `MERGE`, `PROC SORT`, `PROC TRANSPOSE`.
- No user-written informats, `INPUT`/`PUT` conversions, `SUBSTR`, `SCAN`, date functions.

This is good news for effort, but §2 still covers how we would handle them so the plan transfers to the rest of the lending codebase.

---

## 2. Hard-to-port constructs and how we handle each

### 2.1 Missing-value semantics (highest risk)

**SAS:** `.` is a *value* that sorts below every number. `if DEBTINC < 30` is **true** for a missing `DEBTINC`. `mean()` ignores missing. `PROC FREQ` and `CLASS` drop missing rows. `N` counts non-missing.

**Spark:** `null` is *unknown*. `col("DEBTINC") < 30` is **null** (neither true nor false); a `filter` drops it, and a `when(...)` chain falls through to `otherwise(...)`. `groupBy` keeps a `null` group. `count("*")` counts every row; `count(col)` counts non-null.

Concrete consequence in this repo: `04_risk_segmentation.sas` guards each bucket with `if DEBTINC ne . then do; … end;`. Drop that guard in Spark and a missing `DEBTINC` becomes `"Very High"` (via `otherwise`) whereas SAS would have produced `"Low"` (missing < 30). The existing `pyspark/04` correctly keeps an explicit `isNull()` branch first — that pattern must be mandatory.

**Handling rule set**

| Situation | Rule |
|---|---|
| Any comparison on a nullable column | Write the `isNull()` branch first, explicitly, and decide what SAS would have done (usually: it fell into the lowest bucket, or the row was excluded). Never rely on `otherwise`. |
| PROC FREQ / CLASS | `filter(col.isNotNull())` before `groupBy`, unless the SAS code uses `/ missing`. Compute percentages from the *non-missing* total. |
| `N` statistic | `count(col)`, not `count("*")`. |
| Sorting | SAS sorts `.` first; Spark sorts `null` first ascending by default (`asc_nulls_first`) — matches, but make it explicit. |
| Subsetting `IF a > 0` | `filter(col("a") > 0)` drops nulls — matches SAS. `IF a < 5` does **not** match (SAS keeps missing). |

A short linter check (grep for `when(` without a preceding `isNull`) is cheap and worth adding.

### 2.2 PROC LOGISTIC options

| SAS option | PySpark handling | Notes |
|---|---|---|
| `descending` | Nothing — MLlib models `P(label = 1)` by default. | Cast `BAD` to double as `label`. |
| `CLASS JOB(ref='Other') REASON(ref='HomeImp') / param=ref` | **Build the dummy columns by hand**: `when(JOB == "Mgr", 1).otherwise(0)`, … omitting `Other`; same for `REASON` omitting `HomeImp`. | `StringIndexer` + `OneHotEncoder` orders levels by *frequency* and drops the *last* one, so the reference level is whatever is least common, and with `handleInvalid="keep"` (as in the current script) an extra "unseen" slot becomes the reference and every real level gets a dummy — the intercept is then confounded. Hand-built dummies are also far easier for a SAS reader to audit. |
| `selection=stepwise slentry=.05 slstay=.05` | No equivalent in MLlib. Two options: **(a)** freeze the effect list SAS selected (read it from the SAS log once) and fit that fixed model with `regParam=0`; **(b)** implement stepwise as a Python loop of `LogisticRegression.fit` calls using Wald or likelihood-ratio tests. | (a) is what we recommend for the parity contract — the model *specification* becomes a versioned artifact instead of an algorithm output. (b) is a reusable utility (M effort) if the team re-runs selection regularly. L1/ElasticNet (the current script) is **not** equivalent: it shrinks every coefficient and picks variables by a different criterion. |
| No regularization in SAS | `regParam=0.0`, `elasticNetParam=0.0`, `standardization=False`, tight `tol` (e.g. `1e-8`), enough `maxIter`. | *Expected*: with identical rows and the same design matrix, MLE coefficients agree to ~1e-4 (SAS: Fisher scoring; MLlib: L-BFGS — both find the same maximum). |
| `details` | Log each step of the stepwise loop (option b) or record in the frozen-spec file (option a). | |
| `lackfit` (Hosmer–Lemeshow) | Compute by hand: `ntile(10)` over predicted probability, sum observed vs expected per decile, χ² with 8 df. | S effort. `scipy.stats.chi2` for the p-value. |
| `output out= predicted=` | `model.transform(df)`; extract `P(1)` with `pyspark.ml.functions.vector_to_array(col("probability"))[1]`. | Not a Python UDF (slow, and the current script's UDF is where the duplicate-key `agg` bug lives). |
| `store` / `PROC PLM restore= score … / ilink` | `pipelineModel.write().overwrite().save(path)` / `PipelineModel.load(path).transform(df)`. `ilink` = probability scale, which `transform` already gives. | MLflow is the conventional registry on Databricks. |
| `roc` + `ods output Association=` | `c` = `BinaryClassificationEvaluator(metricName="areaUnderROC")`; `Somers' D = 2c − 1`; `Gamma`, `Tau-a`, and percent concordant/discordant/tied need a pair count — feasible with a self-join on this data size, or a rank-based formula for larger data. | The c-statistic equals AUC when tied pairs are counted as one half, which is how SAS defines `c`. |
| `PROC SURVEYSELECT method=srs samprate=0.7 seed=42` | Cannot reproduce SAS's random stream. **For parity: export the `selected` flag from SAS once and join it in.** For greenfield: `orderBy(rand(seed)).limit(n)` gives an exact 70% without replacement; `randomSplit` (current script) is approximate (1,008 validation rows in the run we did, not exactly 30%). | Requires a stable row key — the dataset has none. Add a `LOAN_ID` to the extract, or hash all 18 columns. |

### 2.3 Formats and labels

- **Labels** (18 + 2 + 5 in this codebase): Spark has no display label, but columns carry metadata. Use `df.withMetadata("BAD", {"comment": "Loan Status (1=Default, 0=Paid)"})` (Spark ≥ 3.3) and, on Delta/Databricks, `COMMENT` on the table column so labels show up in the catalog and BI tools. Keep the dictionary in one module, not repeated per script.
- **Display formats** (`dollar12.`, `percent8.2`, `comma8.1`, `8.1`): do **not** bake into stored data. Keep raw numeric columns; apply `format_number` / `format_string` only in the final report cell, or leave to the BI layer. Storing formatted strings breaks downstream arithmetic and makes parity checks harder.
- **`date9.` on a SAS date serial**: `date_add(lit("1960-01-01"), col("APPDATE"))` → a real `DateType`. This is a *data* change, not a display change, and must be done once at load.
- **`PROC FORMAT` value ranges** (`low -< 0.60`, `0.80 - high`): `when` chains, in ascending order, with the `isNull()` branch first (§2.1). Where a format is applied in many places, wrap it in a Python function (`def ltv_risk(c): return when(...)`) — that *is* the format catalog.
- **`LENGTH $n`**: Spark strings are variable-length; nothing is truncated. In this codebase every literal fits its declared length, so no behaviour change. Elsewhere, look for SAS code that *relies* on truncation (e.g. `$1` flags) and add `substring(col, 1, n)` explicitly.

### 2.4 Macro variables and macros

None here. Pattern for the wider codebase: `%LET` → module-level Python constant; `&var` inside PROC SQL → f-string into `spark.sql`; `%MACRO … %MEND` → Python function taking and returning DataFrames; `%DO` over a list → `for`; `CALL SYMPUTX` (data-driven macro var) → `df.agg(...).first()[0]`. The risk is not translation but *scope*: SAS macro variables are global and mutable; make the Python equivalents function arguments.

### 2.5 BY-group / `FIRST.` / `LAST.` / `RETAIN`

None here. Pattern: `Window.partitionBy(by_cols).orderBy(order_cols)`; `FIRST.x` → `row_number() == 1`; `LAST.x` → `row_number()` descending `== 1` (or `count` over the window); `RETAIN` running totals → `sum(...).over(window.rowsBetween(Window.unboundedPreceding, 0))`; `LAG` → `lag()`. Non-obvious trap: SAS `BY` requires sorted input and processes in that order; Spark windows need an *explicit* `orderBy` with a **total** ordering — ties in the order key give non-deterministic `FIRST.` in Spark where SAS gives the file order. Always add a stable tiebreaker.

### 2.6 Other constructs found in these programs

| Construct | Handling | Fidelity |
|---|---|---|
| `PROC IMPORT guessingrows=5960` / `inferSchema=True` | Replace with an **explicit `StructType`** so `BAD` is not sometimes int and sometimes double. SAS stores every numeric as 8-byte float; use `DoubleType` for all numerics except identifiers. | Exact |
| CSV has a UTF-8 BOM on the first header (`\ufeff` before `BAD`) | Spark's CSV reader strips it (observed: the `BAD` column test passes). Keep an assertion on the column list anyway. | Exact |
| `propcase(CITY)` | `initcap(CITY)`. SAS `PROPCASE` also capitalises after `-`, `/`, `(`, `)`, `.` by default; `initcap` only after whitespace. One city in this data is affected: `winston-salem` → SAS `Winston-Salem`, Spark `Winston-salem`. Fix with `regexp_replace` on the delimiter set or accept and document. | Near-exact |
| `ARRAY` + `DO i = 1 to 8` | Python `for` over a column list calling `withColumn`. Or `select("*", *[...])` for one pass. | Exact |
| Subsetting `IF` (no `THEN`) | `filter(...)`. | Exact, given §2.1 |
| Two-output DATA step (`output work.train; else output work.valid`) | Two `filter` calls on the same DataFrame (Spark is lazy; the source is not read twice unless you ask). | Exact |
| `PROC MEANS` exact percentiles (`p1 … p99`) | `approxQuantile` (current script, relative error 0.01) is **not** exact. Use Spark SQL `percentile(col, p)` (exact) for parity checks. SAS default `PCTLDEF=5` and Spark's linear interpolation differ slightly for small groups — I believe they agree only when the sample size makes the interpolation gap negligible; agree a tolerance (e.g. 0.5% of the IQR) or implement PCTLDEF=5 with window ranks (S). | Tolerance |
| `PROC MEANS … median` | `percentile(col, 0.5)` / `median()` (Spark ≥ 3.4). The current `pyspark/03` skips medians. | Tolerance |
| `PROC TABULATE … all='Total'` | `groupBy(...).pivot(...)` for the shape plus `rollup()` / `cube()` for margins — `rollup("JOB", "REGION")` yields exactly the "each level plus Total" rows SAS prints. | Exact |
| `PROC FREQ` 3-way `A*B*C` | One `groupBy(A, B, C).count()` and pivot `C` for display. | Exact |
| `PROC SQL outobs=10 … order by` | `LIMIT 10` — but ties in `avg_loan` are broken arbitrarily by both engines; add a deterministic secondary key (`STATE`). | Exact with tiebreaker |
| `PROC PRINT (obs=20)` | `show(20)`. Spark makes **no order guarantee** without `orderBy`; for a single local CSV it happens to be file order. On a cluster it will not be. Add a row-sequence column at load if "first 20" matters. | Cosmetic |
| `PROC CONTENTS` | `printSchema()` + metadata comments. | Cosmetic |
| `title` / listing output | Print statements now; long-term, write report DataFrames to tables and let BI render them. | Cosmetic |

---

## 3. Program-by-program status of the existing PySpark versions

Baseline from the current tree: `python -m pytest tests/test_pyspark_outputs.py` → **13 passed, 1 failed**. The failure (`test_frequency_counts`, `5960 != 5681`) is precisely the §2.1 `groupBy`-keeps-nulls issue: 279 rows with missing `JOB` land in a `null` group that SAS would have excluded.

Cross-cutting gap that affects 03, 04 and 05: **they skip the `0 < LTV < 5` filter**, so they operate on 5,848 rows where SAS uses 5,337 (511 rows, 8.7%, of divergence). `02` applies it correctly but nothing downstream reuses `02`. Fixing this means one shared cleaning module (or persisted intermediate tables) instead of four copies of the cleaning code.

| # | SAS → PySpark | Exists | Complete? | Gaps |
|---|---|---|---|---|
| 01 | `01_data_loading` | yes | **Mostly** | `inferSchema` instead of explicit schema (`APPDATE`, `BAD` typed as int; SAS treats all as numeric double). `APPDATE` never converted from SAS date serial — shows as `22040`, SAS shows `05MAY2020`. Labels live in a dict that is printed and thrown away; not attached as column metadata. Display formats not represented anywhere. `show(20)` order not guaranteed. |
| 02 | `02_data_cleaning` | yes | **Mostly** | Percentiles use `approxQuantile(…, 0.01)` — not comparable to SAS `p1/p99` for a parity report. `describe()` has no `nmiss`; SAS asks for it. Labels/`percent8.2` on `LTV` not carried. Otherwise the derivations (`LTV`, `LOAN_OUTCOME`, `*_MISS` flags, both filters) match SAS logic line for line, and the final row count would be 5,337. |
| 03 | `03_aggregation_reporting` | yes | **Partial** | Wrong population (5,848 vs 5,337). Frequency tables include a `null` row and compute `Percent` against a total that includes missing — SAS excludes both. Step 2 (`PROC MEANS CLASS LOAN_OUTCOME`) omits **median** entirely and only computes std/min/max for `LOAN`, not `MORTDUE`/`VALUE`/`DEBTINC`; `N` uses `count("LOAN")` for every column. Step 3 (`TABULATE`) gives a JOB×REGION count crosstab plus a separate default-rate list; no `all='Total'` margins, `null` JOB/REGION groups present. Step 5 omits median. Ordering and rounding differ (cosmetic). |
| 04 | `04_risk_segmentation` | yes | **Mostly** | Wrong population (5,848 vs 5,337). Frequency tables include `null` groups. Bucketing and the 0–10 score are otherwise a faithful translation — `isNull()` guards are present, cut-points and point values match, `RISK_SEGMENT` thresholds match. Step 5 reports `N` + default rate per LTV×DTI cell instead of SAS's `BAD` count cells; same information, different shape. Labels not carried. |
| 05 | `05_logistic_regression` | yes | **Not parity-grade** | (1) Wrong population, and the complete-case filter is applied *before* the split (SAS: after, inside PROC LOGISTIC) — 3,548 candidate rows vs SAS's 3,881 sampled / 3,532 fitted. (2) `randomSplit` ≠ SURVEYSELECT: different rows, not exactly 70%. (3) `StringIndexer`+`OneHotEncoder(handleInvalid="keep")` yields 16 features with **no reference level** for `JOB`/`REASON`; SAS's `ref='Other'`/`ref='HomeImp'` is not honoured and the intercept is not comparable. (4) `regParam=0.01, elasticNetParam=0.8` is L1-regularised — coefficients are shrunk and variable selection is not stepwise. (5) Coefficients printed by index (`Feature 3: 0.0667`) not name. (6) `agg({"pred_prob": "count", "pred_prob": "mean"})` — duplicate dict key, `count` silently dropped (observed in output). (7) Python UDF for probability extraction instead of `vector_to_array`. (8) No Hosmer–Lemeshow (`lackfit`), no Somers' D / Gamma / Tau-a / concordant-pair counts, no `details`. (9) Model not persisted (`store`/PLM). (10) Does not reproduce (or correct) SAS Step 6's refit-on-validation quirk — the AUC it prints (0.7642 in our run) is a different quantity from what SAS prints. (11) `train_scored` not produced. |
| tests | `tests/test_pyspark_outputs.py` | yes | **Self-consistency only** | 14 tests; they re-implement the transformations inline rather than importing the scripts (the scripts are top-level code, not functions), so a bug in `pyspark/*.py` cannot fail a test. No SAS-derived expected values anywhere in the repo — "parity" is currently asserted, not measured. |
| docs | `migration_guide/*.md` | yes | Good reference | Mapping tables are accurate and useful for onboarding. The `FIRST./LAST.` row should mention the ordering/tiebreaker trap (§2.5); the `PROC FORMAT` row should mention the null-first rule (§2.1). |

---

## 4. Recommended migration order, effort, risks

Effort key (Devin sessions, each roughly a focused working day): **S** = part of one session · **M** = one session · **L** = two to three sessions, usually because of external waits (SAS baseline runs, sign-offs) rather than code volume.

| Order | Work item | Effort | Depends on | Why this position | Key risks |
|---|---|---|---|---|---|
| 0 | **Foundation**: `pyspark/common.py` with explicit schema, `load()`, `clean()` (→ 5,337 rows), label metadata, format helpers; refactor scripts into importable functions; parity harness skeleton (§5) | M | — | Every later item reuses it; removes the four inconsistent copies of cleaning logic. | Refactor touches all scripts at once — keep behaviour identical and prove it with the row-count checks before moving on. |
| 1 | **01 data loading** | S | 0 | Cheapest; establishes schema and `APPDATE` conversion for everything downstream. | `APPDATE` interpretation must be confirmed with the data owner (SAS date serial assumed; consistent with values 2017–2022). |
| 2 | **02 data cleaning** | S–M | 0, 1 | Produces `home_equity_final`, the contract dataset for 03/04/05. Exact percentiles needed for the outlier report. | Percentile definition mismatch (§2.6) — agree tolerance up front. |
| 3 | **04 risk segmentation** | S | 2 | Deterministic business rules with regulatory relevance; highest value per hour; already 90% correct. Fits before 03 because 03 is output-only and has no dependants. | Missing-value guards must survive any refactor (§2.1). Confirm with the team that the unused `PROC FORMAT` catalog is not applied elsewhere (other reports) before treating it as dead. |
| 4 | **03 aggregation & reporting** | M | 2 | Many small tables; each needs null handling, medians, margins, and a decision on presentation formats. No downstream dependency, so it can run in parallel with 5. | Volume of small cosmetic differences that reviewers may read as errors — deliver with a side-by-side parity report, not raw `show()` output. |
| 5 | **05 logistic regression** | L | 2 (needs `home_equity_final`), SAS access | Longest, most scrutinised, and needs artifacts only SAS can produce (the `selected` flag, the stepwise-selected effect list, the reference coefficient table). Start the SAS exports during phase 0 so they are ready. | Stepwise not reproducible algorithmically (freeze spec instead); split not reproducible (join flag); reference-level encoding; agreement on which AUC is the contract (§1.5 point 1); tolerance for coefficients. |
| 6 | **Tests & docs**: replace inline re-implementations with imports from `common.py` and the scripts; add SAS baselines under `baselines/`; fix `test_frequency_counts` via the real fix, not the assertion | M | 0–5 | Turns the suite into an actual parity gate for CI. | Baselines require a SAS session and a one-off export program (`PROC EXPORT` / `ODS OUTPUT` to CSV) — an external dependency on SAS licence access. |

Rough total: 6–8 sessions, with the SAS baseline export and risk-team review being the schedule drivers rather than the PySpark code.

**What we will not attempt to match** (and need explicit agreement on):
- The SAS random stream (`seed=42`) — we join the SAS `selected` flag instead.
- Stepwise selection as an *algorithm* — we freeze the selected effect list as versioned configuration.
- Listing-output layout (titles, column widths, `dollar12.` spacing). We match the numbers.

---

## 5. How we validate parity

The principle for the risk team: **every SAS dataset that feeds a decision gets a PySpark twin, and we prove the twins are identical row-for-row; every SAS report number gets a PySpark number with a stated tolerance.** Today's repo has no SAS outputs checked in, so the first step is generating them.

### 5.1 Baselines (one-off, in SAS)

Export from a single SAS run, checked into `baselines/` as CSV (or Parquet):

| Artifact | How |
|---|---|
| `home_equity_final`, `home_equity_risk`, `model_data` datasets | `PROC EXPORT` |
| `model_split` with the `selected` flag and a row key | `PROC EXPORT` — this is what lets PySpark train on identical rows |
| PROC MEANS / FREQ / TABULATE / SQL result tables | `ODS OUTPUT` (`Summary=`, `OneWayFreqs=`, `CrossTabFreqs=`) or `out=` options |
| PROC LOGISTIC: `ParameterEstimates`, `Association`, `LackFitChiSq`, `ModelBuildingSummary` (stepwise steps), `valid_scored` | `ODS OUTPUT` + `PROC EXPORT` |

Requires a stable row key. The dataset has none; the SAS export program should add `ROW_ID = _N_` on the raw import and PySpark should add the same at load (single-file, `monotonically_increasing_id()` is *not* safe for this — use `row_number()` over a zipped-with-index read).

### 5.2 Checks, in increasing strength

| Level | Check | Tolerance | Applies to |
|---|---|---|---|
| 1 Structure | Column names, types, row counts at every stage: 5,960 → 5,848 → 5,337 → 3,881 → 3,532 fitted | Exact | all |
| 2 Column profiles | Per numeric column: `N`, `NMISS`, `sum`, `mean`, `min`, `max`; per categorical: full level distribution | Counts exact; floats `abs ≤ 1e-6` or `rel ≤ 1e-9` | 01, 02, 04 |
| 3 Row-level | `spark_df.exceptAll(sas_df)` and the reverse, joined on `ROW_ID`, on every derived column (`LTV`, `LOAN_OUTCOME`, `*_MISS`, `*_RISK_CAT`, `RISK_SCORE`, `RISK_SEGMENT`, `PREDICTED_BAD`) | Zero differing rows (floats compared with `abs ≤ 1e-9`) | 02, 04, 05 |
| 4 Report aggregates | Every cell of every SAS report table vs the PySpark table, keyed by class levels | Counts/percentages exact; means/std `rel ≤ 1e-6`; percentiles per agreed rule (§2.6) | 02, 03, 04 |
| 5 Model | Same training rows (via `selected` flag); same effect list; then: coefficients and intercept; predicted probability per validation row; confusion matrix; c / Somers' D; Hosmer–Lemeshow χ² | Coefficients `abs ≤ 1e-4` (*expected* given both are unpenalised MLE); probabilities `abs ≤ 1e-6`; confusion matrix exact; c `abs ≤ 1e-4`; HL χ² `abs ≤ 1e-3` | 05 |

Anything outside tolerance is a defect until explained; explanations that are *accepted* (e.g. `winston-salem` casing, refit-on-validation AUC) go into a `PARITY_EXCEPTIONS.md` with the risk team's sign-off.

### 5.3 Where the checks live

- `tests/` — one pytest module per SAS program, importing the real PySpark functions, reading `baselines/`, asserting the levels above. Runs in CI on every change.
- A `parity_report.py` that emits a single markdown/HTML side-by-side (SAS value, Spark value, diff, tolerance, pass/fail) — this is the artifact to hand to the risk team.
- The existing 14 tests remain as smoke tests but stop re-implementing logic.

### 5.4 What "we know it's right" means, in one paragraph

For every dataset SAS produces, PySpark produces the same rows with the same values (level 3, zero differences). For every number on a SAS report, PySpark produces the same number within a stated, reviewed tolerance (level 4). For the model, trained on provably the same rows with the same specification, the coefficients, the per-loan probabilities and the summary statistics agree within tolerances that are tighter than any business decision depends on (level 5). Everything we chose *not* to match is written down with a reason and a signature.

---

## 6. Decisions needed from the team

1. Which AUC is the contract for 05 — the trained model scored on `valid` (recommended) or SAS Step 6's refit?
2. Freeze the stepwise-selected effects as configuration (recommended) or build a stepwise routine in PySpark?
3. Confirm `APPDATE` is a SAS date serial and whether downstream consumers expect a `DateType`.
4. Percentile tolerance rule for the outlier report (§2.6).
5. Is the unused `PROC FORMAT` catalog in 04 referenced by any other program outside this repo?
6. Who runs the SAS baseline export (needs a SAS licence; nothing in this repo can produce it).
