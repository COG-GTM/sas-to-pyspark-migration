"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas

Re-migrated from scratch using sas/02_data_cleaning.sas as the authoritative
spec. Reproduces the SAS DATA-step logic idiomatically in PySpark:
  - Derived columns (LTV, LOAN_OUTCOME)
  - Proper-cased CITY (propcase -> initcap)
  - Array-based missing-value flags
  - Subsetting-IF filters for critical-null and outlier rows
  - PROC MEANS-equivalent summary statistics for QA
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, initcap, lit

# ------------------------------------------------------------------
# Self-contained SparkSession (no shared helper module by design).
# SAS equivalent: starting a SAS session.
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("HomeEquity_DataCleaning")
    .master("local[*]")
    .getOrCreate()
)

# Load the source dataset (produced upstream by 01_data_loading).
# SAS equivalent: work.home_equity created via PROC IMPORT.
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# Numeric variables flagged for missingness by the SAS array (Step 2).
NUM_VARS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

# ------------------------------------------------------------------
# Step 1: Create derived columns.
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
#   run;
# Notes: a `when` without `otherwise` yields null, matching the SAS
# missing (.) fallthrough for LTV and the '' fallthrough for LOAN_OUTCOME.
# ------------------------------------------------------------------
df_clean = (
    df
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull()
            & col("MORTDUE").isNotNull()
            & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ),
    )
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default")),
    )
    .withColumn("CITY", initcap(col("CITY")))  # SAS: propcase(CITY)
)

# ------------------------------------------------------------------
# Step 2: Flag missing values (before any imputation).
# SAS equivalent:
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS MORTDUE_MISS ... NINQ_MISS;
#   do i = 1 to 8;
#       if num_vars{i} = . then num_flags{i} = 1;
#       else num_flags{i} = 0;
#   end;
# The SAS array loop becomes one per-column indicator (<VAR>_MISS).
# ------------------------------------------------------------------
df_imputed = df_clean
for var in NUM_VARS:
    df_imputed = df_imputed.withColumn(
        f"{var}_MISS",
        when(col(var).isNull(), lit(1)).otherwise(lit(0)),
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields.
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
df_filtered = df_imputed.filter(
    col("LOAN").isNotNull()
    & col("VALUE").isNotNull()
    & col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Outlier-detection summary (QA only, no rows dropped).
# SAS equivalent:
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# describe() covers count/mean/std/min/max; approxQuantile supplies the
# SAS percentiles (p1 p5 p25 median p75 p95 p99).
# ------------------------------------------------------------------
outlier_vars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]

print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("(equivalent to PROC MEANS with percentiles)")
print("=" * 60)
df_filtered.select(*outlier_vars).describe().show()

percentiles = [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]
percentile_labels = ["p1", "p5", "p25", "median", "p75", "p95", "p99"]
print("Approximate Percentiles (SAS p1 p5 p25 median p75 p95 p99):")
for var in outlier_vars:
    quantiles = df_filtered.approxQuantile(var, percentiles, 0.01)
    formatted = ", ".join(
        f"{label}={value}" for label, value in zip(percentile_labels, quantiles)
    )
    print(f"  {var:8s}: {formatted}")

# ------------------------------------------------------------------
# Step 5: Create the final clean dataset (drop extreme outliers).
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;   /* LTV sanity check */
#       if LOAN > 0;              /* positive loan amounts */
#       if VALUE > 0;             /* positive property values */
#   run;
# In SAS a missing LTV fails `LTV > 0`; null > 0 is null (dropped) in
# Spark too, so null-LTV rows are excluded identically.
# ------------------------------------------------------------------
df_final = df_filtered.filter(
    (col("LTV") > 0)
    & (col("LTV") < 5)
    & (col("LOAN") > 0)
    & (col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Original rows:    {df.count()}")
print(f"After filtering:  {df_filtered.count()}")
print(f"Final clean rows: {df_final.count()}")

# SAS equivalent:
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
df_final.select("LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC").describe().show()

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
df_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON",
).show(10, truncate=False)

# Clean up
spark.stop()
