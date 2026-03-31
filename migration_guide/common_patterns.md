# Common SAS to PySpark Migration Patterns

Detailed side-by-side examples of the most frequently encountered migration patterns.

---

## 1. Reading Data and Applying Schemas

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
          BAD="Loan Status";
    format LOAN dollar12.
           APPDATE date9.;
quit;
```

### PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Option 1: Infer schema automatically
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
    "BAD": "Loan Status",
}
```

**Key differences:**
- SAS `guessingrows` controls type inference; PySpark uses `inferSchema=True` or explicit schemas
- SAS labels and formats have no direct PySpark equivalent; use metadata dictionaries
- PySpark reads are lazy; data is not loaded until an action is called

---

## 2. Conditional Column Creation (IF/THEN/ELSE to when/otherwise)

### SAS
```sas
data work.loans;
    set work.home_equity;

    /* Simple IF/THEN/ELSE */
    if BAD = 0 then LOAN_OUTCOME = 'Paid';
    else if BAD = 1 then LOAN_OUTCOME = 'Default';
    else LOAN_OUTCOME = 'Unknown';

    /* Nested conditions */
    if DEBTINC < 30 then RISK = 'Low';
    else if DEBTINC < 40 then RISK = 'Medium';
    else if DEBTINC < 50 then RISK = 'High';
    else if DEBTINC ne . then RISK = 'Very High';
    else RISK = 'Unknown';

    /* Calculated column with condition */
    if VALUE > 0 and MORTDUE ne . then LTV = MORTDUE / VALUE;
    else LTV = .;
run;
```

### PySpark
```python
from pyspark.sql.functions import col, when, lit

# Simple when/otherwise (equivalent to IF/THEN/ELSE)
df = df.withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit("Unknown"))
)

# Nested conditions
df = df.withColumn(
    "RISK",
    when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .when(col("DEBTINC").isNotNull(), lit("Very High"))
    .otherwise(lit("Unknown"))
)

# Calculated column with condition
df = df.withColumn(
    "LTV",
    when(
        (col("VALUE") > 0) & col("MORTDUE").isNotNull(),
        col("MORTDUE") / col("VALUE")
    )
)
```

**Key differences:**
- SAS uses `ne .` for null checks; PySpark uses `.isNotNull()` or `.isNull()`
- SAS `IF` modifies in place; PySpark `.withColumn()` creates a new DataFrame
- PySpark `when()` chains are evaluated top-to-bottom, first match wins

---

## 3. Missing Value Handling (SAS `.` vs PySpark `null`)

### SAS
```sas
/* Check for missing */
if LOAN = . then LOAN_MISSING = 1;
else LOAN_MISSING = 0;

/* Replace missing numeric with 0 */
if DEROG = . then DEROG = 0;

/* Replace missing character with 'Unknown' */
if JOB = '' then JOB = 'Unknown';

/* Drop rows with any missing in key columns */
if LOAN ne . and VALUE ne . and BAD ne .;

/* Array-based imputation */
array nums{*} LOAN MORTDUE VALUE;
do i = 1 to dim(nums);
    if nums{i} = . then nums{i} = 0;
end;
```

### PySpark
```python
from pyspark.sql.functions import col, when, lit, coalesce

# Check for missing
df = df.withColumn(
    "LOAN_MISSING",
    when(col("LOAN").isNull(), lit(1)).otherwise(lit(0))
)

# Replace missing numeric with 0
df = df.na.fill({"DEROG": 0})

# Replace missing character with 'Unknown'
df = df.na.fill({"JOB": "Unknown"})

# Drop rows with any missing in key columns
df = df.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# Bulk fill for multiple columns (like SAS array processing)
numCols = ["LOAN", "MORTDUE", "VALUE"]
df = df.na.fill({c: 0 for c in numCols})

# Alternative: use coalesce for default values
df = df.withColumn("DEROG", coalesce(col("DEROG"), lit(0)))
```

**Key differences:**
- SAS uses `.` for missing numeric and `''` for missing character; PySpark uses `null` for both
- SAS array processing becomes a Python loop over column names
- `na.fill()` can handle multiple columns at once with a dictionary

---

## 4. Aggregation with Multiple Statistics

### SAS
```sas
/* PROC MEANS with CLASS variable */
proc means data=work.home_equity n mean std min max median;
    class LOAN_OUTCOME;
    var LOAN MORTDUE VALUE DEBTINC;
run;

/* PROC FREQ for frequency counts */
proc freq data=work.home_equity;
    tables JOB * REGION / norow nocol;
run;

/* PROC TABULATE for cross-tabs */
proc tabulate data=work.home_equity;
    class JOB REGION;
    var BAD;
    table JOB, REGION * BAD * mean;
run;
```

### PySpark
```python
from pyspark.sql.functions import count, mean, stddev, min, max, round

# PROC MEANS equivalent
df.groupBy("LOAN_OUTCOME").agg(
    count("LOAN").alias("N"),
    round(mean("LOAN"), 2).alias("Mean_LOAN"),
    round(stddev("LOAN"), 2).alias("Std_LOAN"),
    round(min("LOAN"), 2).alias("Min_LOAN"),
    round(max("LOAN"), 2).alias("Max_LOAN"),
    round(mean("MORTDUE"), 2).alias("Mean_MORTDUE"),
    round(mean("VALUE"), 2).alias("Mean_VALUE"),
).show()

# PROC FREQ equivalent (cross-tab)
df.stat.crosstab("JOB", "REGION").show()

# or with counts
df.groupBy("JOB", "REGION").count().orderBy("JOB", "REGION").show()

# PROC TABULATE equivalent (pivot)
df.groupBy("JOB").pivot("REGION").agg(
    round(mean("BAD"), 4).alias("mean_default")
).show()
```

**Key differences:**
- SAS procedures are self-contained; PySpark chains transformations
- SAS `PROC MEANS` automatically computes multiple stats; PySpark requires explicit `.agg()` calls
- PySpark `.pivot()` provides PROC TABULATE-like cross-tabulation

---

## 5. Joining / Merging Datasets

### SAS
```sas
/* Sort both datasets first (required for MERGE) */
proc sort data=work.loans; by customer_id; run;
proc sort data=work.customers; by customer_id; run;

/* Inner merge */
data work.combined;
    merge work.loans(in=a) work.customers(in=b);
    by customer_id;
    if a and b;  /* inner join */
run;

/* Left join using PROC SQL */
proc sql;
    create table work.combined as
    select a.*, b.customer_name, b.credit_score
    from work.loans as a
    left join work.customers as b
    on a.customer_id = b.customer_id;
quit;
```

### PySpark
```python
# Inner join (no pre-sorting required)
combined = loans.join(customers, on="customer_id", how="inner")

# Left join
combined = loans.join(
    customers.select("customer_id", "customer_name", "credit_score"),
    on="customer_id",
    how="left"
)

# Multiple join keys
combined = loans.join(
    customers,
    on=["customer_id", "account_type"],
    how="inner"
)

# Join with different column names
combined = loans.join(
    customers,
    loans.cust_id == customers.customer_id,
    how="left"
)

# Equivalent using Spark SQL
loans.createOrReplaceTempView("loans")
customers.createOrReplaceTempView("customers")

combined = spark.sql("""
    SELECT a.*, b.customer_name, b.credit_score
    FROM loans a
    LEFT JOIN customers b ON a.customer_id = b.customer_id
""")
```

**Key differences:**
- SAS `MERGE` requires pre-sorted data; PySpark `.join()` does not
- SAS `in=` dataset flags become join type specification in PySpark
- PySpark supports all standard join types: `inner`, `left`, `right`, `outer`, `cross`, `semi`, `anti`

---

## 6. Macro Variable Replacement with Python Variables

### SAS
```sas
%let startDate = 01JAN2020;
%let endDate = 31DEC2020;
%let targetVar = BAD;

data work.filtered;
    set work.home_equity;
    where APPDATE between "&startDate"d and "&endDate"d;
run;

proc means data=work.filtered;
    var &targetVar;
run;

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
# Python variables replace SAS macro variables
startDate = "2020-01-01"
endDate = "2020-12-31"
targetVar = "BAD"

# Use variables in filter
filtered = df.filter(
    (col("APPDATE") >= startDate) & (col("APPDATE") <= endDate)
)

# Use variables in aggregation
filtered.select(targetVar).describe().show()

# Python functions replace SAS macros
def runAnalysis(dataset, classVar, analysisVar):
    """Equivalent to %macro runAnalysis"""
    dataset.groupBy(classVar).agg(
        count(analysisVar).alias("N"),
        mean(analysisVar).alias("Mean"),
        stddev(analysisVar).alias("Std"),
    ).show()

runAnalysis(df, "JOB", "LOAN")
runAnalysis(df, "REGION", "VALUE")

# Dynamic SQL with f-strings (replaces &macrovar in PROC SQL)
tableName = "home_equity"
filterCol = "REGION"
filterVal = "South"

result = spark.sql(f"""
    SELECT * FROM {tableName}
    WHERE {filterCol} = '{filterVal}'
""")
```

**Key differences:**
- SAS macros generate code at compile time; Python functions execute at runtime
- `%LET` becomes simple Python variable assignment
- `&macrovar` resolution becomes f-strings or `.format()`
- SAS `%INCLUDE` becomes Python `import`

---

## 7. Output Dataset Creation (Writing Results)

### SAS
```sas
/* Write to SAS dataset */
data mylib.clean_loans;
    set work.home_equity_clean;
run;

/* Export to CSV */
proc export data=work.home_equity_clean
    outfile="/output/clean_loans.csv"
    dbms=csv replace;
run;

/* Export to database */
libname mydb odbc dsn="MyDatabase";
data mydb.loan_results;
    set work.scored_loans;
run;
```

### PySpark
```python
# Write to Parquet (recommended for Spark ecosystems)
df.write.parquet("output/clean_loans.parquet", mode="overwrite")

# Write to CSV
df.write.csv(
    "output/clean_loans.csv",
    header=True,
    mode="overwrite"
)

# Write single CSV file (coalesce to 1 partition)
df.coalesce(1).write.csv(
    "output/clean_loans_single.csv",
    header=True,
    mode="overwrite"
)

# Write to database via JDBC
df.write.jdbc(
    url="jdbc:postgresql://host:5432/mydb",
    table="loan_results",
    mode="overwrite",
    properties={"user": "username", "password": "password"}
)

# Save as Hive table
df.write.saveAsTable("default.clean_loans", mode="overwrite")

# Write to Delta Lake (Databricks)
df.write.format("delta").save("output/clean_loans_delta")
```

**Key differences:**
- SAS writes single files; PySpark writes partitioned directories by default
- Use `.coalesce(1)` for single-file output (but this limits parallelism)
- Parquet is the preferred format in Spark ecosystems (columnar, compressed)
- PySpark supports `overwrite`, `append`, `ignore`, `error` write modes
