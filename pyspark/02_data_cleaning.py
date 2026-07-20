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
# Load input data
# SAS equivalent: set work.home_equity;
#   (work.home_equity is produced by 01_data_loading.sas)
# As in 01_data_loading.py, read the CSV with header and inferred schema.
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
# Note: PySpark has no native column labels/formats like SAS.
# The SAS label and format statements are captured as comments only.
# ------------------------------------------------------------------

# Calculate Loan-to-Value ratio
df = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
)

# Create descriptive loan outcome
df = df.withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
)

# Proper-case city names (SAS propcase)
df = df.withColumn("CITY", initcap(col("CITY")))

# ------------------------------------------------------------------
# Step 2: Handle missing values using array processing
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
num_vars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

# Flag missing values before imputation (equivalent to the SAS do-loop)
for var in num_vars:
    df = df.withColumn(
        f"{var}_MISS",
        when(col(var).isNull(), lit(1)).otherwise(lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
df = df.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers using summary statistics
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
# ------------------------------------------------------------------
print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
outlier_vars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
# summary() reports count, mean, stddev, min, quartiles (25/50/75%) and max
df.select(*outlier_vars).summary().show()

# ------------------------------------------------------------------
# Step 5: Create the final clean dataset (outlier removal)
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;   /* LTV ratio sanity check */
#       if LOAN > 0;               /* Positive loan amounts only */
#       if VALUE > 0;              /* Positive property values only */
#   run;
# ------------------------------------------------------------------
df_final = df.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Final clean dataset summary statistics
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
df_final.select("LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC").describe().show()

# ------------------------------------------------------------------
# Preview first 10 observations of the final dataset
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
df_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
