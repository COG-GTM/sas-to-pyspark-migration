"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load the source dataset
# SAS equivalent: set work.home_equity;
# (each pipeline script is standalone, so the CSV is re-read here)
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
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
# ------------------------------------------------------------------
dfClean = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
).withColumn(
    "CITY",
    initcap(col("CITY"))
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
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS ... NINQ_MISS;
#   do i = 1 to 8;
#       if num_vars{i} = . then num_flags{i} = 1;
#       else num_flags{i} = 0;
#   end;
# ------------------------------------------------------------------
numericVars = [
    "LOAN", "MORTDUE", "VALUE", "YOJ",
    "DEROG", "DELINQ", "CLAGE", "NINQ"
]

dfImputed = dfClean
for numericVar in numericVars:
    dfImputed = dfImputed.withColumn(
        f"{numericVar}_MISS",
        when(col(numericVar).isNull(), lit(1)).otherwise(lit(0))
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
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

print("=" * 60)
print("Step 3: Filter Records with Missing Critical Fields")
print("=" * 60)
print(f"Rows before filtering: {dfImputed.count()}")
print(f"Rows after filtering:  {dfFiltered.count()}")

# ------------------------------------------------------------------
# Step 4: Check for outliers using summary statistics
# SAS equivalent:
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
print("Step 4: Summary Statistics for Outlier Detection")
print("=" * 60)
dfFiltered.select(outlierVars).summary(
    "count", "mean", "stddev", "min",
    "1%", "5%", "25%", "50%", "75%", "95%", "99%", "max"
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
#
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Step 5: Clean Dataset Summary")
print("=" * 60)
print(f"Final row count: {dfFinal.count()}")
dfFinal.select(
    "LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"
).describe().show(truncate=False)

print("=" * 60)
print("First 10 Observations of the Final Clean Dataset")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
