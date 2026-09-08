"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, initcap, lit

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load the input data
# SAS equivalent:
#   set work.home_equity;   /* dataset created by 01_data_loading.sas */
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
#   run;
# ------------------------------------------------------------------
home_equity_clean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull()
            & col("MORTDUE").isNotNull()
            & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

# SAS equivalent:
#   label LTV = "Loan to Value Ratio"
#         LOAN_OUTCOME = "Loan Outcome";
#
# Note: PySpark does not have native column labels like SAS.
# We document them as metadata in a dictionary.
derivedColumnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

print("=" * 60)
print("Derived Column Labels (SAS-style variable labels)")
print("=" * 60)
for column_name, label in derivedColumnLabels.items():
    print(f"  {column_name:12s} -> {label}")

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
    "DEROG", "DELINQ", "CLAGE", "NINQ",
]

home_equity_imputed = home_equity_clean
for column_name in numericVars:
    home_equity_imputed = home_equity_imputed.withColumn(
        f"{column_name}_MISS",
        when(col(column_name).isNull(), lit(1)).otherwise(lit(0))
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
    col("LOAN").isNotNull()
    & col("VALUE").isNotNull()
    & col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
outlierVars = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "LTV", "CLAGE", "DEROG", "DELINQ",
]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
home_equity_filtered.select(*outlierVars).summary(
    "count", "mean", "stddev", "min", "1%", "5%", "25%",
    "50%", "75%", "95%", "99%", "max"
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
    (col("LTV") > 0) & (col("LTV") < 5)
    & (col("LOAN") > 0)
    & (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 6: Summarize and preview the final clean dataset
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
home_equity_final.select(
    "LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"
).summary("count", "mean", "stddev", "min", "max").show(truncate=False)

print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
home_equity_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
