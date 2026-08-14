"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
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
# Load the source data
# SAS equivalent: work.home_equity created by 01_data_loading.sas
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
#
# Note: PySpark has no native column labels or formats. As in program 01,
# the SAS labels are documented here instead:
#   LTV          -> "Loan to Value Ratio"  (SAS format percent8.2)
#   LOAN_OUTCOME -> "Loan Outcome"
# ------------------------------------------------------------------
home_equity_clean = df \
    .withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull()
            & F.col("MORTDUE").isNotNull()
            & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE")
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid"))
         .when(F.col("BAD") == 1, F.lit("Default"))
         .otherwise(F.lit(""))
    ) \
    .withColumn("CITY", F.initcap(F.col("CITY")))

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
# Note: the SAS array / do-loop is replaced by a Python loop that builds
# one flag column per numeric variable.
# ------------------------------------------------------------------
numericVars = ["LOAN", "MORTDUE", "VALUE", "YOJ",
               "DEROG", "DELINQ", "CLAGE", "NINQ"]

home_equity_imputed = home_equity_clean
for var in numericVars:
    home_equity_imputed = home_equity_imputed.withColumn(
        f"{var}_MISS",
        F.when(F.col(var).isNull(), F.lit(1)).otherwise(F.lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
home_equity_filtered = home_equity_imputed.filter(
    F.col("LOAN").isNotNull()
    & F.col("VALUE").isNotNull()
    & F.col("BAD").isNotNull()
)

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
# Note: summary() gives count/mean/stddev/min/percentiles/max. The SAS
# NMISS statistic and the extra percentiles are computed separately with
# aggregations and percentile_approx.
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
               "LTV", "CLAGE", "DEROG", "DELINQ"]

print("=" * 60)
print("Summary Statistics for Outlier Detection (PROC MEANS)")
print("=" * 60)
home_equity_filtered.select(outlierVars).summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).show(truncate=False)

print("Missing value counts (NMISS) and extreme percentiles")
home_equity_filtered.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_NMISS")
     for c in outlierVars]
).show(truncate=False)

home_equity_filtered.select(
    [F.percentile_approx(F.col(c), [0.01, 0.05, 0.95, 0.99]).alias(
        f"{c}_P1_P5_P95_P99") for c in outlierVars]
).show(truncate=False)

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
home_equity_final = home_equity_filtered.filter(
    (F.col("LTV") > 0)
    & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

# ------------------------------------------------------------------
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
# ------------------------------------------------------------------
finalVars = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]

print("\n" + "=" * 60)
print("Clean Dataset Summary (PROC MEANS)")
print("=" * 60)
print(f"Number of rows: {home_equity_final.count()}")
home_equity_final.select(finalVars).summary(
    "count", "mean", "stddev", "min", "max"
).show(truncate=False)

home_equity_final.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_NMISS")
     for c in finalVars]
).show(truncate=False)

# ------------------------------------------------------------------
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
home_equity_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
