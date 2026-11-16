"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, initcap, lit, when

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load the raw dataset (work.home_equity, created by 01_data_loading)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
# Note: SAS keeps work.home_equity in the WORK library between programs.
# Spark has no shared WORK library, so each script reloads the CSV.
# The path is resolved from this file so the script runs from any CWD.
# ------------------------------------------------------------------
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

df = spark.read.csv(
    dataPath,
    header=True,
    inferSchema=True
)
rawCount = df.count()


def printHeader(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ------------------------------------------------------------------
# Step 1: Create derived columns using DATA step
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
# Notes:
#   - SAS missing (.) maps to Spark null; `when` without a match
#     returns null, matching LTV = . in the ELSE branch.
#   - propcase() maps to initcap().
#   - PySpark has no native column labels or display formats. The labels
#     are documented below; LTV is kept as a raw ratio (e.g. 0.6627).
#     percent8.2 only changes how SAS displays the value (66.27%), not
#     the stored number, so no conversion is applied.
# ------------------------------------------------------------------
columnLabels = {
    "LTV": "Loan to Value Ratio",          # SAS format: percent8.2 (display only)
    "LOAN_OUTCOME": "Loan Outcome",
}

dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull()
            & col("MORTDUE").isNotNull()
            & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

printHeader("Step 1: Derived columns (work.home_equity_clean)")
for colName, label in columnLabels.items():
    print(f"  {colName:12s} -> {label}")
print(f"Rows: {dfClean.count()}")
dfClean.select("BAD", "LOAN_OUTCOME", "MORTDUE", "VALUE", "LTV", "CITY") \
    .show(5, truncate=False)

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
#
# Note: The SAS array + DO loop becomes a Python list comprehension that
# builds one flag column per variable in a single select (no loop index
# column to drop).
# ------------------------------------------------------------------
missFlagVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean.select(
    "*",
    *[
        when(col(v).isNull(), lit(1)).otherwise(lit(0)).alias(f"{v}_MISS")
        for v in missFlagVars
    ]
)

printHeader("Step 2: Missing-value flags (work.home_equity_imputed)")
dfImputed.agg(
    *[F.sum(f"{v}_MISS").alias(f"{v}_MISS") for v in missFlagVars]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull()
    & col("VALUE").isNotNull()
    & col("BAD").isNotNull()
)

filteredCount = dfFiltered.count()
printHeader("Step 3: Filter missing critical fields (work.home_equity_filtered)")
print(f"Rows before: {rawCount}")
print(f"Rows after:  {filteredCount}")
print(f"Removed:     {rawCount - filteredCount}")

# ------------------------------------------------------------------
# Step 4: Check for outliers using PROC MEANS
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
#
# Notes:
#   - stddev() is the sample standard deviation, same as SAS STD.
#   - approxQuantile with relativeError=0.0 computes exact percentiles.
#     Spark returns an actual data value for each quantile, while SAS
#     (QNTLDEF=5) may average two neighbouring values, so percentiles can
#     differ slightly on even-sized groups.
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
percentiles = [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]
percentileNames = ["P1", "P5", "P25", "Median", "P75", "P95", "P99"]


def procMeans(data, variables, withPercentiles):
    """Build a PROC MEANS-style table: one row per variable."""
    aggExprs = []
    for v in variables:
        aggExprs += [
            F.count(v).alias(f"{v}__N"),
            F.sum(when(col(v).isNull(), 1).otherwise(0)).alias(f"{v}__NMiss"),
            F.mean(v).alias(f"{v}__Mean"),
            F.stddev(v).alias(f"{v}__StdDev"),
            F.min(v).alias(f"{v}__Min"),
            F.max(v).alias(f"{v}__Max"),
        ]
    stats = data.agg(*aggExprs).first().asDict()

    quantiles = {}
    if withPercentiles:
        # approxQuantile ignores nulls, like PROC MEANS
        for v, q in zip(variables, data.approxQuantile(variables, percentiles, 0.0)):
            quantiles[v] = q

    def asFloat(x):
        return None if x is None else float(x)

    rows = []
    for v in variables:
        values = [stats[f"{v}__Mean"], stats[f"{v}__StdDev"], stats[f"{v}__Min"]]
        if withPercentiles:
            values += quantiles[v]
        values.append(stats[f"{v}__Max"])
        rows.append((v, stats[f"{v}__N"], stats[f"{v}__NMiss"],
                     *[asFloat(x) for x in values]))

    columns = ["Variable", "N", "NMiss", "Mean", "StdDev", "Min"]
    if withPercentiles:
        columns += percentileNames
    columns.append("Max")
    return spark.createDataFrame(rows, columns)


printHeader("Step 4: Summary Statistics for Outlier Detection (PROC MEANS)")
procMeans(dfFiltered, outlierVars, withPercentiles=True).show(truncate=False)

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
# Note: In SAS a missing LTV compares as smaller than any number, so
# "LTV > 0" drops rows with missing LTV. A Spark comparison with null is
# null, which filter() also drops, so the result is the same.
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5)
    & (col("LOAN") > 0)
    & (col("VALUE") > 0)
)

finalCount = dfFinal.count()
printHeader("Step 5: Final clean dataset (work.home_equity_final)")
print(f"Rows before: {filteredCount}")
print(f"Rows after:  {finalCount}")
print(f"Removed:     {filteredCount - finalCount}")

# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
printHeader("Clean Dataset Summary (PROC MEANS)")
procMeans(dfFinal, ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
          withPercentiles=False).show(truncate=False)

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
printHeader("First 10 Observations (equivalent to PROC PRINT obs=10)")
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
