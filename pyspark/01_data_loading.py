"""
PySpark Script: 01_data_loading.py
Purpose: Load and explore the HOME_EQUITY dataset
Equivalent SAS Program: sas/01_data_loading.sas
"""

from pyspark.sql import SparkSession

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataLoading") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 1: Load CSV data
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#       guessingrows=5960;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 2: Apply column descriptions (labels)
# SAS equivalent:
#   proc datasets lib=work;
#       modify home_equity;
#       label BAD="Loan Status (1=Default, 0=Paid)"
#             LOAN="Amount of Loan Request" ...;
#   quit;
#
# Note: PySpark does not have native column labels like SAS.
# We document them as metadata in a dictionary and add as table comment.
# ------------------------------------------------------------------
columnLabels = {
    "BAD": "Loan Status (1=Default, 0=Paid)",
    "LOAN": "Amount of Loan Request",
    "MORTDUE": "Amount Due on Existing Mortgage",
    "VALUE": "Value of Current Property",
    "REASON": "Loan Purpose (HomeImp or DebtCon)",
    "JOB": "Job Category",
    "YOJ": "Years at Present Job",
    "DEROG": "Number of Derogatory Reports",
    "DELINQ": "Number of Delinquent Credit Lines",
    "CLAGE": "Age of Oldest Credit Line (months)",
    "NINQ": "Number of Recent Credit Inquiries",
    "CLNO": "Number of Credit Lines",
    "DEBTINC": "Debt to Income Ratio",
    "APPDATE": "Loan Application Date",
    "CITY": "City",
    "STATE": "State",
    "DIVISION": "Census Division",
    "REGION": "Census Region",
}

# Print labels for documentation
print("=" * 60)
print("Column Labels (SAS-style variable labels)")
print("=" * 60)
for col, label in columnLabels.items():
    print(f"  {col:12s} -> {label}")

# ------------------------------------------------------------------
# Step 3: Display dataset metadata
# SAS equivalent:
#   proc contents data=work.home_equity;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Dataset Schema (equivalent to PROC CONTENTS)")
print("=" * 60)
df.printSchema()

print(f"Number of rows: {df.count()}")
print(f"Number of columns: {len(df.columns)}")

# ------------------------------------------------------------------
# Step 4: Preview first 20 observations
# SAS equivalent:
#   proc print data=work.home_equity(obs=20);
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("First 20 Observations (equivalent to PROC PRINT obs=20)")
print("=" * 60)
df.show(20, truncate=False)

# Clean up
spark.stop()
