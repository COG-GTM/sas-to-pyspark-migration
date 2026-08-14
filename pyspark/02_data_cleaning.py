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
# Load the source dataset
# SAS equivalent:
#   set work.home_equity;   /* output of 01_data_loading.sas */
#
# Each PySpark script is standalone, so the CSV is re-read here in the
# same way as pyspark/01_data_loading.py.
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
# Note: SAS labels ("Loan to Value Ratio", "Loan Outcome") and formats
# (LTV percent8.2) have no native PySpark equivalent; as in
# 01_data_loading.py they are documented here rather than applied.
# ------------------------------------------------------------------
dfClean = df.withColumn(
    "LTV",
    F.when(
        F.col("VALUE").isNotNull()
        & F.col("MORTDUE").isNotNull()
        & (F.col("VALUE") > 0),
        F.col("MORTDUE") / F.col("VALUE")
    ).otherwise(F.lit(None))
).withColumn(
    "LOAN_OUTCOME",
    F.when(F.col("BAD") == 0, F.lit("Paid"))
    .when(F.col("BAD") == 1, F.lit("Default"))
    .otherwise(F.lit(""))
).withColumn(
    "CITY",
    F.initcap(F.col("CITY"))
)

print("=" * 60)
print("Step 1: Derived Columns (LTV, LOAN_OUTCOME, proper-case CITY)")
print("=" * 60)
dfClean.select(
    "BAD", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "CITY"
).show(10, truncate=False)

# ------------------------------------------------------------------
# Step 2: Flag missing values (SAS array processing)
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
# The SAS array loop becomes a Python loop over the column list.
# ------------------------------------------------------------------
numericVars = [
    "LOAN", "MORTDUE", "VALUE", "YOJ",
    "DEROG", "DELINQ", "CLAGE", "NINQ"
]

dfImputed = dfClean
for numericVar in numericVars:
    dfImputed = dfImputed.withColumn(
        f"{numericVar}_MISS",
        F.when(F.col(numericVar).isNull(), F.lit(1)).otherwise(F.lit(0))
    )

print("=" * 60)
print("Step 2: Missing-Value Flags")
print("=" * 60)
dfImputed.select([f"{v}_MISS" for v in numericVars]).show(10)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    F.col("LOAN").isNotNull()
    & F.col("VALUE").isNotNull()
    & F.col("BAD").isNotNull()
)

print("=" * 60)
print("Step 3: Filter Records with Missing Critical Fields")
print("=" * 60)
print(f"Rows before filtering: {dfImputed.count()}")
print(f"Rows after filtering:  {dfFiltered.count()}")

# ------------------------------------------------------------------
# Step 4: Check for outliers using summary statistics
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
outlierVars = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "LTV", "CLAGE", "DEROG", "DELINQ"
]

print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
dfFiltered.select(outlierVars).summary(
    "count", "mean", "stddev", "min",
    "1%", "5%", "25%", "50%", "75%", "95%", "99%", "max"
).show(truncate=False)

# PROC MEANS also reports NMISS; summary() has no equivalent statistic.
dfFiltered.select([
    F.sum(F.col(v).isNull().cast("int")).alias(f"{v}_NMISS")
    for v in outlierVars
]).show(truncate=False)

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
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (F.col("LTV") > 0)
    & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

finalVars = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]

print("=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Final row count: {dfFinal.count()}")
dfFinal.select(finalVars).summary(
    "count", "mean", "stddev", "min", "max"
).show(truncate=False)

dfFinal.select([
    F.sum(F.col(v).isNull().cast("int")).alias(f"{v}_NMISS")
    for v in finalVars
]).show(truncate=False)

print("=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
