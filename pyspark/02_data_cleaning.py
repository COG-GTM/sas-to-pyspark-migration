"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
         - Create derived columns (LTV, LOAN_OUTCOME)
         - Flag missing values
         - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load source data (equivalent to work.home_equity from 01_data_loading)
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

print("=" * 60)
print("Source Data (work.home_equity)")
print("=" * 60)
print(f"Number of rows: {df.count()}")

# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else
#           LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#       label LTV = "Loan to Value Ratio"
#             LOAN_OUTCOME = "Loan Outcome";
#       format LTV percent8.2;
#   run;
#
# Note: PySpark has no native column labels or display formats.
# They are documented as metadata in dictionaries below.
# initcap() is the closest equivalent to SAS propcase().
# ------------------------------------------------------------------
df_clean = df \
    .withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull()
            & F.col("MORTDUE").isNotNull()
            & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE")
        ).otherwise(F.lit(None).cast("double"))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, "Paid")
         .when(F.col("BAD") == 1, "Default")
         .otherwise("")
    ) \
    .withColumn("CITY", F.initcap(F.col("CITY")))

columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",
}

print("\n" + "=" * 60)
print("Step 1: Derived Columns (work.home_equity_clean)")
print("=" * 60)
for col, label in columnLabels.items():
    fmt = columnFormats.get(col, "")
    print(f"  {col:12s} -> {label}" + (f"  [SAS format: {fmt}]" if fmt else ""))
print(f"Number of rows: {df_clean.count()}")

# ------------------------------------------------------------------
# Step 2: Flag missing values (array processing)
# SAS equivalent:
#   data work.home_equity_imputed;
#       set work.home_equity_clean;
#       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#       array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
#                          DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
#       do i = 1 to 8;
#           if num_vars{i} = . then num_flags{i} = 1;
#           else num_flags{i} = 0;
#       end;
#       drop i;
#   run;
#
# Note: SAS arrays are replaced by a Python list of column names;
# the flag columns are built in a single select (no row-level loop).
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

df_imputed = df_clean.select(
    "*",
    *[
        F.when(F.col(c).isNull(), 1).otherwise(0).alias(f"{c}_MISS")
        for c in numVars
    ]
)

print("\n" + "=" * 60)
print("Step 2: Missing-Value Flags (work.home_equity_imputed)")
print("=" * 60)
df_imputed.select(
    [F.sum(f"{c}_MISS").alias(f"{c}_MISS") for c in numVars]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
df_filtered = df_imputed.filter(
    F.col("LOAN").isNotNull()
    & F.col("VALUE").isNotNull()
    & F.col("BAD").isNotNull()
)

print("\n" + "=" * 60)
print("Step 3: Filter Missing Critical Fields (work.home_equity_filtered)")
print("=" * 60)
print(f"Number of rows: {df_filtered.count()}")

# ------------------------------------------------------------------
# Step 4: Check for outliers using summary statistics
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
#
# Note: percentile_approx is used for p1..p99; SAS uses exact
# percentiles, so values may differ slightly on large datasets.
# ------------------------------------------------------------------
def proc_means(data, columns, percentiles=None):
    """Build a PROC MEANS-style table: one row per variable."""
    percentiles = percentiles or {}
    aggs = []
    for c in columns:
        aggs += [
            F.count(c).alias(f"{c}__N"),
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__NMISS"),
            F.mean(c).alias(f"{c}__MEAN"),
            F.stddev(c).alias(f"{c}__STD"),
            F.min(c).alias(f"{c}__MIN"),
            F.max(c).alias(f"{c}__MAX"),
        ]
        for name, p in percentiles.items():
            aggs.append(F.percentile_approx(c, p, 100000).alias(f"{c}__{name}"))
    row = data.agg(*aggs).collect()[0].asDict()

    stats = ["N", "NMISS", "MEAN", "STD", "MIN"] + list(percentiles) + ["MAX"]
    rows = [
        tuple(
            [c, int(row[f"{c}__N"]), int(row[f"{c}__NMISS"])]
            + [
                float(row[f"{c}__{s}"]) if row[f"{c}__{s}"] is not None else None
                for s in stats[2:]
            ]
        )
        for c in columns
    ]
    schema = "VARIABLE string, N long, NMISS long, " + ", ".join(
        f"{s} double" for s in stats[2:]
    )
    return spark.createDataFrame(rows, schema)


outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlierPercentiles = {
    "P1": 0.01, "P5": 0.05, "P25": 0.25, "MEDIAN": 0.50,
    "P75": 0.75, "P95": 0.95, "P99": 0.99,
}

print("\n" + "=" * 60)
print("Step 4: Summary Statistics for Outlier Detection")
print("=" * 60)
proc_means(df_filtered, outlierVars, outlierPercentiles).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Create the final clean dataset
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: In SAS, missing LTV (.) fails "LTV > 0"; in Spark, a null
# comparison is not true, so filter() drops those rows the same way.
# ------------------------------------------------------------------
df_final = df_filtered.filter(
    (F.col("LTV") > 0) & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Step 5: Final Clean Dataset (work.home_equity_final)")
print("=" * 60)
print(f"Number of rows: {df_final.count()}")

# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
proc_means(df_final, ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]).show(truncate=False)

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
df_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
