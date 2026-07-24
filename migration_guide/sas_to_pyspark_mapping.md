# SAS to PySpark Migration Mapping Reference

A reference table mapping the SAS constructs used across this repository's five
programs (`sas/01`–`sas/05`) to their PySpark equivalents as implemented in the
matching `pyspark/` scripts. Every entry corresponds to a construct that appears
in the SAS and/or PySpark code in this repo.

Mappings are organized by category:

- [Data Access & I/O](#data-access--io)
- [DATA Step Operations](#data-step-operations)
- [Statistical Procedures](#statistical-procedures)
- [Reporting & Display](#reporting--display)
- [Machine Learning](#machine-learning)
- [Macro Language](#macro-language)
- [Data Management](#data-management)

## Data Access & I/O

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC IMPORT` (CSV) | `spark.read.csv()` | Use `header=True, inferSchema=True`; scripts read `data/home_equity.csv` |
| `guessingrows=` | `inferSchema=True` | SAS scans rows to guess types; Spark infers from the full file (or use an explicit `StructType`) |
| `PROC EXPORT` (CSV) | `df.write.csv()` | Use `header=True, mode="overwrite"`; PySpark writes a partitioned directory |
| `PROC EXPORT` (Parquet) | `df.write.parquet()` | Native Spark columnar format, preferred for performance |
| Output to database | `df.write.jdbc()` | Provide JDBC URL and connection properties |

> The five PySpark scripts only read the CSV and print/`show()` results; the
> `write.*` rows above document the standard equivalents for the output-writing
> pattern (see `common_patterns.md`, section 9).

## DATA Step Operations

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `DATA` step (new dataset) | `df.withColumn()` / `df.select()` / `df.filter()` | Chain DataFrame transformations; DataFrames are immutable so each step returns a new DataFrame |
| `SET` (read dataset) | Source DataFrame reference | Reuse or reassign the existing DataFrame |
| `IF / THEN / ELSE` | `when().otherwise()` | From `pyspark.sql.functions`; chained `when()` are evaluated top-to-bottom, first match wins |
| Subsetting `IF` (implicit filter) | `.filter()` / `.where()` | `if LOAN ne . and VALUE ne . and BAD ne .;` → `.filter(col("LOAN").isNotNull() & ...)` |
| `LENGTH` (character, e.g. `$ 7`) | No direct equivalent | Spark strings are unbounded; no fixed width to declare |
| `LENGTH` (numeric) | `.cast()` | Cast to `DoubleType` / `IntegerType` as needed |
| `DROP` | `.drop()` | `drop i;` → drop helper columns from the DataFrame |
| `KEEP` | `.select()` | Select the columns to retain |
| `LABEL` | No native equivalent | Documented in a Python `columnLabels` dictionary (see `pyspark/01_data_loading.py`) |
| `FORMAT` / display formats (`dollar12.`, `percent8.2`, `comma8.1`, `date9.`) | `round()`, `format_number()`, `format_string()` | No persistent format attribute; format at display/write time. Percentages: multiply by 100 then `round()` |
| `ARRAY` + `DO i = 1 to n` | Python `for` loop over a column-name list + `withColumn()` | The missing-flag loop in `pyspark/02_data_cleaning.py` iterates `numCols` and adds a `<col>_MISS` column each pass |
| Sum / accumulation (`RISK_SCORE = RISK_SCORE + n`) | Additive `when()` column expressions | `pyspark/04` builds `RISK_SCORE = ltvScore + dtiScore + delinqScore + derogScore`, each a `when()` chain |
| `propcase()` | `initcap()` | From `pyspark.sql.functions`; `CITY = propcase(CITY)` → `initcap(col("CITY"))` |
| `OUTPUT` to multiple datasets | `randomSplit()` / `.filter()` per subset | Conditional `output work.train work.valid;` maps to `modelData.randomSplit([0.7, 0.3], seed=42)` |
| Missing value (`.` / `''`) | `None` / `null` | Use `.isNull()` / `.isNotNull()`; Spark uses `null` for both numeric and character missings |

## Statistical Procedures

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC MEANS` (`n mean std min max median`) | `.groupBy().agg()` / `.describe()` | Use `count`, `mean`, `stddev`, `min`, `max` from `pyspark.sql.functions` |
| `PROC MEANS` with `CLASS` | `.groupBy(<class var>).agg(...)` | `class LOAN_OUTCOME;` → `.groupBy("LOAN_OUTCOME").agg(...)` |
| `PROC MEANS` percentiles (`p1 p5 p25 ... p99`) | `.approxQuantile()` | `pyspark/02` calls `approxQuantile(col, [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99], 0.01)` |
| `nmiss` | `count(when(col(c).isNull(), 1))` | Spark's `describe()`/`count()` count non-null rows; count nulls explicitly |
| `PROC FREQ` (one-way, `nocum`) | `.groupBy(col).count()` | Add a percentage column with `count("*") / total * 100` |
| `PROC FREQ` (cross-tab, `tables A*B`) | `.stat.crosstab()` or `.groupBy(a, b).count()` | `df.stat.crosstab("JOB", "REGION")` in `pyspark/03` |
| `PROC TABULATE` | `.groupBy().pivot().agg()` / `.groupBy().agg()` | Pivot for the cross-tabulation layout; `pyspark/03` uses `crosstab` plus grouped default rates |
| `PROC SQL` | `spark.sql()` on a temp view | Register with `createOrReplaceTempView()`; full ANSI SQL support |
| `SELECT ... GROUP BY ... HAVING` | `spark.sql(...)` or `.groupBy().agg().filter()` | Matches the top-states query in `pyspark/03` |
| `ORDER BY` / `outobs=` / `LIMIT` | `.orderBy()` + `LIMIT n` | `order by avg_loan desc` + `outobs=10` → `ORDER BY avg_loan DESC ... LIMIT 10` |
| `PROC FORMAT` `value` ranges | `when().otherwise()` chains (or a UDF) | `pyspark/04` replaces `value ltv_risk low -< 0.60 = 'Low' ...` with `when(col("LTV") < 0.60, "Low")...` |
| `PROC SURVEYSELECT` (`method=srs samprate=`) | `df.randomSplit()` / `df.sample()` | `samprate=0.7 seed=42` → `randomSplit([0.7, 0.3], seed=42)` |

## Reporting & Display

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `TITLE` / `TITLE;` | `print()` banner strings | No native report titling; scripts print header lines around each section |
| `PROC PRINT` (`obs=n`) | `.show(n)` | `proc print data=... (obs=20);` → `df.show(20, truncate=False)` |
| `PROC PRINT` with `VAR` | `df.select(cols).show()` | Select the columns to display first |
| `PROC CONTENTS` | `.printSchema()` / `.dtypes` / `len(df.columns)` / `df.count()` | Inspect schema, column count, and row count |

## Machine Learning

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC LOGISTIC` | `pyspark.ml.classification.LogisticRegression` | Wrapped in a `Pipeline` with feature-preparation stages |
| `PROC LOGISTIC descending` | Default `LogisticRegression` label handling | `descending` models the event `BAD=1`; Spark models the `1` label by default |
| ML `Pipeline` | `pyspark.ml.Pipeline` | Chain stages: indexers → encoders → assembler → estimator (`pyspark/05`) |
| `CLASS` statement | `StringIndexer` + `OneHotEncoder` | Encode categorical `JOB`, `REASON` |
| `param=ref` (reference coding) | `OneHotEncoder` (`dropLast=True`, default) | Drops the last category as the reference level |
| `MODEL` statement | `VectorAssembler` + estimator | Assemble numeric + encoded vectors into a single `features` column, then fit |
| `SELECTION=STEPWISE` (`slentry`/`slstay`) | `elasticNetParam` (L1) / `CrossValidator` | `pyspark/05` uses `elasticNetParam=0.8` for sparsity-driven feature selection |
| `OUTPUT ... PREDICTED=` | `model.transform()` | Produces `prediction`, `probability`, and `rawPrediction` columns |
| `STORE` (save fitted model) | `model.save()` | Persist the fitted `PipelineModel` |
| `PROC PLM RESTORE= ... SCORE` | `PipelineModel.load()` + `.transform()` | Restore a stored model and score new data (validation set) |
| Confusion matrix (`PROC FREQ BAD*PREDICTED`) | `.groupBy(label, prediction).count()` | Threshold at `0.5` and cross-tabulate actual vs predicted |
| `roc` / concordance / c-statistic (via `ODS OUTPUT Association=`) | `BinaryClassificationEvaluator(metricName="areaUnderROC")` | Also `"areaUnderPR"`; the c-statistic equals area under the ROC curve |
| Classification metrics (accuracy/precision/recall/F1) | `MulticlassClassificationEvaluator` | Set `metricName` to `accuracy`, `weightedPrecision`, `weightedRecall`, `f1` |

## Macro Language

General reference for parameterizing and reusing logic. The five programs in this
repo do not use SAS macros, but these are the standard equivalents when migrating
macro-driven SAS code (see `common_patterns.md`, section 7).

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `%LET` macro variable | Python variable | `%let n = 10;` → `n = 10` |
| `&macrovar` resolution | f-string / `.format()` | `f"SELECT * FROM {tableName}"` for dynamic Spark SQL |
| `%MACRO` / `%MEND` | Python function | Define a reusable function taking DataFrame/column arguments |
| `%IF / %THEN / %ELSE` | Python `if / elif / else` | Standard control flow at runtime |
| `%DO` loop | Python `for` loop | `for i in range(n):` |

## Data Management

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC DATASETS` (`modify` label/format) | Metadata dictionary + display/write logic | No catalog-level label/format attributes in Spark; document labels in a dict |
| Temporary dataset (`work.*`) | `.createOrReplaceTempView()` / in-memory DataFrame | Session-scoped; `pyspark/03` registers `home_equity` for SQL |
| Permanent dataset (`lib.*`) | `.write.saveAsTable()` / `.write.parquet()` | Persist to a metastore table or storage path |
