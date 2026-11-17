"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import StringType

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load CSV data (same input as 01_data_loading)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
# ------------------------------------------------------------------
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

df = spark.read.csv(
    dataPath,
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
#   - IF/ELSE IF/ELSE -> when().when().otherwise()
#   - SAS missing (.) -> null; when() without otherwise() yields null
#   - propcase() -> propcase UDF below. Spark's initcap() only splits on
#     spaces, so "winston-salem" would become "Winston-salem" instead of
#     SAS's "Winston-Salem". The UDF uses SAS's default delimiters
#     (blank, slash, hyphen, open parenthesis, period, tab).
#   - PySpark has no native labels or display formats, so they are
#     documented in dictionaries below (percent8.2 is applied only
#     when printing, as in SAS).
# ------------------------------------------------------------------
PROPCASE_PATTERN = re.compile(r"(^|[ /\-(.\t])([a-z])")


@udf(returnType=StringType())
def propcase(value):
    if value is None:
        return None
    return PROPCASE_PATTERN.sub(lambda m: m.group(1) + m.group(2).upper(), value.lower())


dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() &
            col("MORTDUE").isNotNull() &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", propcase(col("CITY")))

columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

columnFormats = {
    "LTV": "percent8.2",
}

print("=" * 60)
print("Derived Column Labels and Formats (SAS label / format)")
print("=" * 60)
for colName, label in columnLabels.items():
    fmt = columnFormats.get(colName, "")
    print(f"  {colName:12s} -> {label:22s} {fmt}")

# ------------------------------------------------------------------
# Step 2: Flag missing values using array processing
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
# Note: SAS arrays + DO loop -> a Python list driving withColumn()
# calls. No index variable is created, so there is nothing to drop.
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean
for varName in numVars:
    dfImputed = dfImputed.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#
# Note: Subsetting IF -> filter()
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

print("\n" + "=" * 60)
print("Record Counts")
print("=" * 60)
print(f"  Input rows:              {df.count()}")
print(f"  After critical filter:   {dfFiltered.count()}")

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
# Note: df.summary() covers count/mean/stddev/min/percentiles/max
# (percentiles are approximate). NMISS has no summary() equivalent,
# so it is computed separately and added as a row.
# ------------------------------------------------------------------
def procMeans(data, varNames, stats):
    """Print a PROC MEANS-style table: one row per statistic, one column per variable."""
    summaryStats = [s for s in stats if s != "nmiss"]
    summaryDf = data.select(varNames).summary(*summaryStats)
    rows = {r["summary"]: r for r in summaryDf.collect()}

    totalRows = data.count()
    nmiss = {v: totalRows - int(rows["count"][v]) for v in varNames}

    statLabels = {
        "count": "N", "nmiss": "N Miss", "mean": "Mean", "stddev": "Std Dev",
        "min": "Minimum", "1%": "1st Pctl", "5%": "5th Pctl",
        "25%": "25th Pctl", "50%": "Median", "75%": "75th Pctl",
        "95%": "95th Pctl", "99%": "99th Pctl", "max": "Maximum",
    }

    header = f"{'Statistic':<11s}" + "".join(f"{v:>14s}" for v in varNames)
    print(header)
    print("-" * len(header))
    for stat in stats:
        if stat == "nmiss":
            values = [f"{nmiss[v]:>14d}" for v in varNames]
        elif stat == "count":
            values = [f"{int(rows[stat][v]):>14d}" for v in varNames]
        else:
            values = [f"{float(rows[stat][v]):>14.4f}" if rows[stat][v] is not None
                      else f"{'.':>14s}" for v in varNames]
        print(f"{statLabels[stat]:<11s}" + "".join(values))


outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlierStats = ["count", "nmiss", "mean", "stddev", "min", "1%", "5%", "25%",
                "50%", "75%", "95%", "99%", "max"]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (equivalent to PROC MEANS)")
print("=" * 60)
procMeans(dfFiltered, outlierVars, outlierStats)

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
# Note: Comparisons with null are not true in Spark, so rows with a
# missing LTV are dropped, matching SAS (missing is not > 0 here).
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print(f"\n  Final clean rows:        {dfFinal.count()}")

# ------------------------------------------------------------------
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Clean Dataset Summary (equivalent to PROC MEANS)")
print("=" * 60)
procMeans(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["count", "nmiss", "mean", "stddev", "min", "max"]
)

# ------------------------------------------------------------------
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME",
    "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
