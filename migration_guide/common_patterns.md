# Common SAS to PySpark Migration Patterns

Detailed side-by-side examples of the migration patterns that appear most often
across this repository's five programs. Each PySpark example mirrors how the
matching `pyspark/` script actually implements the pattern.

---

## 1. Reading Data and Applying Schemas

*From `sas/01_data_loading.sas` → `pyspark/01_data_loading.py`.*

### SAS
```sas
proc import datafile="/data/home_equity.csv"
    dbms=csv
    out=work.home_equity
    replace;
    guessingrows=5960;
run;

proc datasets lib=work;
    modify home_equity;
    label LOAN="Amount of Loan Request"
          BAD="Loan Status (1=Default, 0=Paid)";
    format LOAN dollar12.
           APPDATE date9.;
quit;
```

### PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("HomeEquity_DataLoading") \
    .master("local[*]") \
    .getOrCreate()

# Option 1: Infer schema automatically (used by the scripts in this repo)
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# Option 2: Define schema explicitly (recommended for production)
schema = StructType([
    StructField("BAD", IntegerType(), True),
    StructField("LOAN", DoubleType(), True),
    StructField("MORTDUE", DoubleType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("REASON", StringType(), True),
    # ... additional fields
])
df = spark.read.csv("data/home_equity.csv", header=True, schema=schema)

# Labels: documented as a metadata dictionary (PySpark has no native labels)
columnLabels = {
    "LOAN": "Amount of Loan Request",
    "BAD": "Loan Status (1=Default, 0=Paid)",
}
```

**Key differences:**
- SAS `guessingrows` controls type inference; PySpark uses `inferSchema=True` or an explicit `StructType`.
- SAS labels and formats have no direct PySpark equivalent; use metadata dictionaries and format at display/write time.
- PySpark reads are lazy; data is not loaded until an action (e.g. `count()`, `show()`) is called.

---

## 2. Conditional & Derived Columns (IF/THEN/ELSE to when/otherwise)

*From `sas/02_data_cleaning.sas` and `sas/04_risk_segmentation.sas`.*

### SAS
```sas
data work.home_equity_clean;
    length LOAN_OUTCOME $ 7;
    set work.home_equity;

    /* Calculated column with condition (LTV) */
    if VALUE ne . and MORTDUE ne . and VALUE > 0 then
        LTV = MORTDUE / VALUE;
    else
        LTV = .;

    /* Simple IF/THEN/ELSE */
    if BAD = 0 then LOAN_OUTCOME = 'Paid';
    else if BAD = 1 then LOAN_OUTCOME = 'Default';
    else LOAN_OUTCOME = '';

    /* Proper-case a string */
    CITY = propcase(CITY);

    /* Nested conditions (risk bucket) */
    if DEBTINC ne . then do;
        if DEBTINC < 30 then DTI_RISK_CAT = 'Low';
        else if DEBTINC < 40 then DTI_RISK_CAT = 'Medium';
        else if DEBTINC < 50 then DTI_RISK_CAT = 'High';
        else DTI_RISK_CAT = 'Very High';
    end;
run;
```

### PySpark
```python
from pyspark.sql.functions import col, when, lit, initcap

# Calculated column with condition (matches pyspark/02_data_cleaning.py)
df = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
)

# Simple when/otherwise (no otherwise() => null where no branch matches,
# equivalent to SAS assigning '' when BAD is missing)
df = df.withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
)

# propcase -> initcap
df = df.withColumn("CITY", initcap(col("CITY")))

# Nested conditions (matches pyspark/04_risk_segmentation.py)
df = df.withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
)
```

**Key differences:**
- SAS uses `ne .` for null checks; PySpark uses `.isNotNull()` / `.isNull()`.
- SAS `IF` modifies the row in place; PySpark `.withColumn()` returns a new DataFrame.
- A `when()` chain with no `.otherwise()` yields `null` for unmatched rows — the same effect as the SAS `else LOAN_OUTCOME = ''` / `else LTV = .` branches here.
- `when()` chains are evaluated top-to-bottom; the first matching branch wins.

---

## 3. Building a Composite Score (additive when expressions)

*From `sas/04_risk_segmentation.sas` → `pyspark/04_risk_segmentation.py`.*

### SAS
```sas
RISK_SCORE = 0;

/* LTV component (0-3 points) */
if LTV ne . then do;
    if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
    else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
end;

/* DTI component (0-3 points) */
if DEBTINC ne . then do;
    if DEBTINC >= 50 then RISK_SCORE = RISK_SCORE + 3;
    else if DEBTINC >= 40 then RISK_SCORE = RISK_SCORE + 2;
    else if DEBTINC >= 30 then RISK_SCORE = RISK_SCORE + 1;
end;

length RISK_SEGMENT $14;
if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
else RISK_SEGMENT = 'Very High Risk';
```

### PySpark
```python
from pyspark.sql.functions import col, when, lit

# Each SAS "component" becomes a when() column expression
ltvScore = (
    when(col("LTV").isNull(), lit(0))
    .when(col("LTV") >= 0.80, lit(3))
    .when(col("LTV") >= 0.60, lit(1.5))
    .otherwise(lit(0))
)
dtiScore = (
    when(col("DEBTINC").isNull(), lit(0))
    .when(col("DEBTINC") >= 50, lit(3))
    .when(col("DEBTINC") >= 40, lit(2))
    .when(col("DEBTINC") >= 30, lit(1))
    .otherwise(lit(0))
)

# Sum the components instead of accumulating with "RISK_SCORE = RISK_SCORE + ..."
df = df.withColumn("RISK_SCORE", ltvScore + dtiScore)  # + delinqScore + derogScore

df = df.withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)
```

**Key differences:**
- SAS accumulates into a single variable across successive `IF` blocks; PySpark defines each component as an independent `when()` expression and adds them.
- SAS `if X ne . then do; ... end;` (skip missing) maps to a leading `when(col("X").isNull(), lit(0))` branch so missing values contribute 0.

---

## 4. Missing Value Handling (SAS `.` vs PySpark `null`)

*From `sas/02_data_cleaning.sas` → `pyspark/02_data_cleaning.py`.*

### SAS
```sas
/* Array-based missing-value flagging */
array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
                   DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
do i = 1 to 8;
    if num_vars{i} = . then num_flags{i} = 1;
    else num_flags{i} = 0;
end;
drop i;

/* Drop rows with any missing in critical columns */
if LOAN ne . and VALUE ne . and BAD ne .;
```

### PySpark
```python
from pyspark.sql.functions import col, when, lit

# Array-based flagging becomes a Python loop over a column-name list
numCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for colName in numCols:
    df = df.withColumn(
        f"{colName}_MISS",
        when(col(colName).isNull(), lit(1)).otherwise(lit(0))
    )

# Drop rows with any missing in critical columns
df = df.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# Optional alternatives when you need to impute rather than flag:
df = df.na.fill({"DEROG": 0})                 # replace missing numeric with 0
df = df.na.fill({"JOB": "Unknown"})           # replace missing string
# from pyspark.sql.functions import coalesce
# df = df.withColumn("DEROG", coalesce(col("DEROG"), lit(0)))
```

**Key differences:**
- SAS uses `.` for missing numeric and `''` for missing character; PySpark uses `null` for both.
- The SAS `array` + `do` loop that flags missing values becomes a Python `for` loop calling `withColumn()` once per column (this is exactly how `pyspark/02` does it).
- `na.fill()` / `coalesce()` are the imputation equivalents when you need to replace missings rather than flag them.

---

## 5. Aggregation, Frequencies, and Cross-Tabs

*From `sas/03_aggregation_reporting.sas` → `pyspark/03_aggregation_reporting.py`.*

### SAS
```sas
/* PROC FREQ one-way frequencies */
proc freq data=work.home_equity_final;
    tables JOB REASON LOAN_OUTCOME REGION / nocum;
run;

/* PROC MEANS with CLASS variable */
proc means data=work.home_equity_final n mean median std min max;
    class LOAN_OUTCOME;
    var LOAN MORTDUE VALUE DEBTINC;
run;

/* PROC TABULATE cross-tab of default rates */
proc tabulate data=work.home_equity_final;
    class JOB REGION;
    var BAD;
    table JOB, REGION * BAD * mean;
run;
```

### PySpark
```python
from pyspark.sql.functions import col, count, mean, stddev, lit, \
    min as spark_min, max as spark_max, round as spark_round

# PROC FREQ one-way (frequency + percent), matches pyspark/03
total = df.count()
for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    df.groupBy(catCol).agg(
        count("*").alias("Frequency"),
        spark_round(count("*") / lit(total) * 100, 2).alias("Percent")
    ).orderBy(col("Frequency").desc()).show(truncate=False)

# PROC MEANS with CLASS
df.groupBy("LOAN_OUTCOME").agg(
    count("LOAN").alias("N"),
    spark_round(mean("LOAN"), 2).alias("Mean_LOAN"),
    spark_round(stddev("LOAN"), 2).alias("Std_LOAN"),
    spark_round(spark_min("LOAN"), 2).alias("Min_LOAN"),
    spark_round(spark_max("LOAN"), 2).alias("Max_LOAN"),
    spark_round(mean("MORTDUE"), 2).alias("Mean_MORTDUE"),
).show(truncate=False)

# PROC TABULATE cross-tab: crosstab for counts, groupBy for default rates
df.stat.crosstab("JOB", "REGION").show(truncate=False)
df.groupBy("JOB", "REGION").agg(
    count("*").alias("N"),
    spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
).orderBy("JOB", "REGION").show(50, truncate=False)
```

**Key differences:**
- SAS procedures are self-contained; PySpark chains `groupBy().agg()` transformations.
- `PROC MEANS` computes several statistics automatically; PySpark requires an explicit `.agg()` entry per statistic.
- `df.stat.crosstab()` gives the PROC TABULATE-style contingency layout; a grouped mean gives the rate cells.

---

## 6. Ad-hoc Queries with Spark SQL (PROC SQL)

*From `sas/03_aggregation_reporting.sas` → `pyspark/03_aggregation_reporting.py`.*

### SAS
```sas
proc sql outobs=10;
    select STATE,
           count(*) as num_loans,
           mean(LOAN) as avg_loan format=dollar12.,
           mean(BAD) as default_rate format=percent8.2
    from work.home_equity_final
    group by STATE
    having count(*) >= 10
    order by avg_loan desc;
quit;
```

### PySpark
```python
# Register a temp view, then run Spark SQL (matches pyspark/03)
df.createOrReplaceTempView("home_equity")

spark.sql("""
    SELECT
        STATE,
        COUNT(*) AS num_loans,
        ROUND(AVG(LOAN), 2) AS avg_loan,
        ROUND(AVG(BAD) * 100, 2) AS default_rate_pct
    FROM home_equity
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)
```

**Key differences:**
- SAS `PROC SQL` runs directly against work datasets; Spark SQL runs against a registered temp view (`createOrReplaceTempView`).
- SAS `outobs=` becomes `LIMIT`.
- SAS display `format=` (e.g. `dollar12.`, `percent8.2`) has no persistent equivalent — apply `ROUND()` and multiply percentages by 100 when producing the output.

---

## 7. Macro Variables and Macros → Python Variables & Functions

The five programs in this repo do not use SAS macros, but macro-driven SAS code
is common. Python variables replace `%LET` macro variables and Python functions
replace `%MACRO` definitions.

### SAS
```sas
%let targetVar = BAD;

%macro runAnalysis(dataset, classVar, analysisVar);
    proc means data=&dataset n mean std;
        class &classVar;
        var &analysisVar;
    run;
%mend;

%runAnalysis(work.home_equity, JOB, LOAN);
%runAnalysis(work.home_equity, REGION, VALUE);
```

### PySpark
```python
from pyspark.sql.functions import count, mean, stddev

# %LET becomes a Python variable
targetVar = "BAD"
df.select(targetVar).describe().show()

# %MACRO becomes a Python function (runtime execution, not code generation)
def runAnalysis(dataset, classVar, analysisVar):
    dataset.groupBy(classVar).agg(
        count(analysisVar).alias("N"),
        mean(analysisVar).alias("Mean"),
        stddev(analysisVar).alias("Std"),
    ).show()

runAnalysis(df, "JOB", "LOAN")
runAnalysis(df, "REGION", "VALUE")

# &macrovar resolution in dynamic SQL becomes an f-string
tableName, filterCol, filterVal = "home_equity", "REGION", "South"
spark.sql(f"SELECT * FROM {tableName} WHERE {filterCol} = '{filterVal}'")
```

**Key differences:**
- SAS macros generate code at compile time; Python functions execute at runtime.
- `%LET` becomes a simple variable assignment; `&macrovar` becomes an f-string / `.format()`.

---

## 8. Logistic Regression Pipeline

*From `sas/05_logistic_regression.sas` → `pyspark/05_logistic_regression.py`.*

### SAS
```sas
/* Split 70/30 */
proc surveyselect data=work.model_data out=work.model_split
    method=srs samprate=0.7 seed=42;
run;
data work.train work.valid;
    set work.model_split;
    if selected = 1 then output work.train;
    else output work.valid;
run;

/* Fit with stepwise selection and reference coding */
proc logistic data=work.train descending;
    class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON
              / selection=stepwise;
    output out=work.train_scored predicted=pred_prob;
    store work.logit_model;
run;

/* Score validation and evaluate */
proc plm restore=work.logit_model;
    score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
run;
```

### PySpark
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from pyspark.sql.functions import col

modelData = df.withColumn("label", col("BAD").cast("double"))

# PROC SURVEYSELECT srs 70/30 -> randomSplit
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)

# CLASS statement -> StringIndexer + OneHotEncoder (dropLast=True = reference coding)
jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

# MODEL statement -> VectorAssembler
assembler = VectorAssembler(
    inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG",
               "CLAGE", "NINQ", "JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# SELECTION=STEPWISE -> L1 (elasticNet) regularization for feature sparsity
lr = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=100, regParam=0.01, elasticNetParam=0.8
)

pipeline = Pipeline(stages=[jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr])
model = pipeline.fit(train)              # OUTPUT / STORE
predictions = model.transform(valid)     # PROC PLM SCORE

# roc / concordance (c-statistic) -> areaUnderROC
auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(predictions)

# accuracy / precision / recall / F1
metrics = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
for name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    metrics.setMetricName(name)
    print(name, metrics.evaluate(predictions))
```

**Key differences:**
- SAS `PROC SURVEYSELECT method=srs samprate=0.7` becomes `randomSplit([0.7, 0.3], seed=...)`.
- The `CLASS` statement (categorical handling) becomes `StringIndexer` + `OneHotEncoder`; `param=ref` corresponds to `OneHotEncoder`'s default `dropLast=True`.
- `MODEL` variables are consolidated into one `features` vector via `VectorAssembler`; the whole flow is chained in an `ml.Pipeline`.
- `SELECTION=STEPWISE` is approximated by L1/elastic-net regularization (`elasticNetParam`), which drives feature sparsity.
- `STORE` / `PROC PLM RESTORE` map to `model.save()` / `PipelineModel.load()`; scoring is `model.transform()`.
- The `ODS OUTPUT Association=` concordance/AUC statistics are produced by `BinaryClassificationEvaluator`; class-level metrics come from `MulticlassClassificationEvaluator`.

---

## 9. Writing Results

The five scripts in this repo only `print()` / `.show()` their output rather than
persisting it, but writing results is a standard final migration step. These are
the equivalents for the SAS output/export constructs.

### SAS
```sas
/* Write to a permanent SAS dataset */
data mylib.clean_loans;
    set work.home_equity_clean;
run;

/* Export to CSV */
proc export data=work.home_equity_clean
    outfile="/output/clean_loans.csv"
    dbms=csv replace;
run;
```

### PySpark
```python
# Write to CSV (partitioned directory)
df.write.csv("output/clean_loans.csv", header=True, mode="overwrite")

# Single CSV file (limits parallelism; use for small outputs)
df.coalesce(1).write.csv("output/clean_loans_single.csv", header=True, mode="overwrite")

# Parquet (preferred in Spark ecosystems: columnar, compressed)
df.write.parquet("output/clean_loans.parquet", mode="overwrite")

# Persist as a metastore table
df.write.saveAsTable("default.clean_loans", mode="overwrite")

# Write to a database via JDBC
df.write.jdbc(
    url="jdbc:postgresql://host:5432/mydb",
    table="loan_results",
    mode="overwrite",
    properties={"user": "username", "password": "password"}
)
```

**Key differences:**
- SAS writes a single file; PySpark writes a partitioned directory by default. Use `.coalesce(1)` for a single file.
- Parquet is the preferred format in Spark ecosystems; CSV and JDBC remain available.
- PySpark write modes are `overwrite`, `append`, `ignore`, and `error`.
