"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lit, when

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load CSV data (input to the cleaning process)
# SAS equivalent: set work.home_equity;
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
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    # SAS equivalent: CITY = propcase(CITY);
    "CITY",
    initcap(col("CITY"))
)

# Note: PySpark has no native labels/formats like SAS
# (label LTV = "Loan to Value Ratio"; format LTV percent8.2;)
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

print("=" * 60)
print("Derived Column Labels (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:12s} -> {label}")

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

print("\n" + "=" * 60)
print("Missing Value Counts (flag columns)")
print("=" * 60)
for numericVar in numericVars:
    missing = dfImputed.filter(col(f"{numericVar}_MISS") == 1).count()
    print(f"  {numericVar:12s} -> {missing} missing")

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

print(f"\nRows before filtering: {dfImputed.count()}")
print(f"Rows after filtering:  {dfFiltered.count()}")

# ------------------------------------------------------------------
# Step 4: Summary statistics for outlier detection
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

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (PROC MEANS)")
print("=" * 60)
dfFiltered.select(outlierVars).summary(
    "count", "mean", "stddev", "min",
    "1%", "5%", "25%", "50%", "75%", "95%", "99%", "max"
).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Create the final clean dataset (remove extreme outliers)
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
print("\n" + "=" * 60)
print("Clean Dataset Summary (PROC MEANS)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")
print(f"Number of columns: {len(dfFinal.columns)}")
dfFinal.select("LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC").describe().show(
    truncate=False
)

print("\n" + "=" * 60)
print("Clean Dataset Schema")
print("=" * 60)
dfFinal.printSchema()

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
