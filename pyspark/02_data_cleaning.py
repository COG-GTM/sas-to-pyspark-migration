"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
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

# ------------------------------------------------------------------
# Load source data (work.home_equity from 01_data_loading)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#       label LTV = "Loan to Value Ratio"
#             LOAN_OUTCOME = "Loan Outcome";
#       format LTV percent8.2;
#   run;
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

# Note: PySpark has no native column labels or display formats like SAS.
# We document them as metadata in dictionaries instead.
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",  # e.g. 0.8123 displays as 81.23%
}

print("=" * 60)
print("Derived Column Labels and Formats (SAS-style)")
print("=" * 60)
for col, label in columnLabels.items():
    fmt = columnFormats.get(col, "")
    print(f"  {col:12s} -> {label}" + (f" (format {fmt})" if fmt else ""))

# ------------------------------------------------------------------
# Step 2: Flag missing values
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
# Note: a Python loop over a column list replaces the SAS arrays.
# ------------------------------------------------------------------
num_vars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

df_imputed = df_clean
for col in num_vars:
    df_imputed = df_imputed.withColumn(
        f"{col}_MISS",
        F.when(F.col(col).isNull(), 1).otherwise(0)
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
df_filtered = df_imputed.na.drop(subset=["LOAN", "VALUE", "BAD"])

print(f"\nRows before filtering missing critical fields: {df_imputed.count()}")
print(f"Rows after filtering missing critical fields:  {df_filtered.count()}")


def summary_stats(data, columns, stats):
    """Return a PROC MEANS-style DataFrame: one row per variable, one column per statistic."""
    percentiles = {
        "p1": 0.01, "p5": 0.05, "p25": 0.25, "median": 0.50,
        "p75": 0.75, "p95": 0.95, "p99": 0.99,
    }
    aggs = []
    for c in columns:
        for s in stats:
            if s == "n":
                expr = F.count(F.col(c))
            elif s == "nmiss":
                expr = F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
            elif s == "mean":
                expr = F.mean(c)
            elif s == "std":
                expr = F.stddev(c)
            elif s == "min":
                expr = F.min(c)
            elif s == "max":
                expr = F.max(c)
            else:
                expr = F.percentile_approx(c, percentiles[s], 100000)
            aggs.append(expr.alias(f"{c}__{s}"))
    row = data.agg(*aggs).first()
    records = [
        tuple([c] + [
            None if row[f"{c}__{s}"] is None
            else int(row[f"{c}__{s}"]) if s in ("n", "nmiss")
            else float(row[f"{c}__{s}"])
            for s in stats
        ])
        for c in columns
    ]
    return spark.createDataFrame(records, ["Variable"] + [s.upper() for s in stats])


# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
#
# Note: percentiles use percentile_approx; SAS computes exact percentiles.
# ------------------------------------------------------------------
outlier_vars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlier_stats = ["n", "nmiss", "mean", "std", "min", "p1", "p5", "p25",
                 "median", "p75", "p95", "p99", "max"]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
summary_stats(df_filtered, outlier_vars, outlier_stats).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Create the final clean dataset
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
# ------------------------------------------------------------------
df_final = df_filtered.filter(
    (F.col("LTV") > 0) & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

print(f"Number of rows in final clean dataset: {df_final.count()}")

# ------------------------------------------------------------------
# Clean dataset summary
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
summary_stats(
    df_final,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["n", "nmiss", "mean", "std", "min", "max"]
).show(truncate=False)

# ------------------------------------------------------------------
# Preview first 10 observations
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
df_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
