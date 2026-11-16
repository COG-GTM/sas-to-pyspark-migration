"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Flag missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, regexp_replace, format_number, concat,
    count, mean, stddev, min as spark_min, max as spark_max, percentile_approx
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Resolve the dataset relative to the project root so the script runs from any CWD
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

# SAS: PROC IMPORT -> spark.read.csv (replicates 01_data_loading.py inline)
df = spark.read.csv(dataPath, header=True, inferSchema=True)


def propcase(column):
    """SAS propcase(): capitalize after blank, '/', '-', '(' and '.'.

    initcap() only treats whitespace as a word boundary, so a space is
    temporarily inserted after the extra delimiters (e.g. 'winston-salem'
    -> 'Winston-Salem', matching SAS rather than 'Winston-salem').
    """
    spaced = regexp_replace(column, r"([-/(.])", "$1 ")
    return regexp_replace(initcap(spaced), r"([-/(.]) ", "$1")


def procMeans(data, variables, stats):
    """SAS: PROC MEANS VAR ... -> one agg() row per variable, stacked with unionByName."""
    statExprs = {
        "N": lambda c: count(col(c)),
        "NMiss": lambda c: count(when(col(c).isNull(), 1)),
        "Mean": lambda c: mean(col(c)),
        "StdDev": lambda c: stddev(col(c)),
        "Min": lambda c: spark_min(col(c)).cast("double"),
        "P1": lambda c: percentile_approx(col(c), 0.01).cast("double"),
        "P5": lambda c: percentile_approx(col(c), 0.05).cast("double"),
        "P25": lambda c: percentile_approx(col(c), 0.25).cast("double"),
        "Median": lambda c: percentile_approx(col(c), 0.5).cast("double"),
        "P75": lambda c: percentile_approx(col(c), 0.75).cast("double"),
        "P95": lambda c: percentile_approx(col(c), 0.95).cast("double"),
        "P99": lambda c: percentile_approx(col(c), 0.99).cast("double"),
        "Max": lambda c: spark_max(col(c)).cast("double"),
    }
    rows = [
        data.agg(
            lit(v).alias("Variable"),
            *[statExprs[s](v).alias(s) for s in stats]
        )
        for v in variables
    ]
    result = rows[0]
    for r in rows[1:]:
        result = result.unionByName(r)
    return result


# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#       label LTV = "Loan to Value Ratio" LOAN_OUTCOME = "Loan Outcome";
#       format LTV percent8.2;
#   run;
# ------------------------------------------------------------------
# SAS: IF/THEN/ELSE -> when().otherwise(); missing (.) -> null
dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        # SAS blank character value '' (missing) -> null
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .withColumn("CITY", propcase(col("CITY")))  # SAS: propcase() -> initcap()

# SAS: LABEL -> metadata dictionary (no native column labels in Spark)
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
# Step 2: Flag missing values using array processing
# SAS equivalent:
#   data work.home_equity_imputed;
#       set work.home_equity_clean;
#       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#       array num_flags{8} LOAN_MISS ... NINQ_MISS;
#       do i = 1 to 8;
#           if num_vars{i} = . then num_flags{i} = 1;
#           else num_flags{i} = 0;
#       end;
#       drop i;
#   run;
# ------------------------------------------------------------------
# SAS: ARRAY + DO loop -> Python loop over a column list with withColumn
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
numFlags = [f"{v}_MISS" for v in numVars]

dfImputed = dfClean
for varName, flagName in zip(numVars, numFlags):
    dfImputed = dfImputed.withColumn(
        flagName, when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
# SAS: subsetting IF -> filter()
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
# SAS: PROC MEANS percentiles -> percentile_approx (median() needs Spark 3.4+)
print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (PROC MEANS)")
print("=" * 60)
procMeans(
    dfFiltered,
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"],
    ["N", "NMiss", "Mean", "StdDev", "Min", "P1", "P5", "P25",
     "Median", "P75", "P95", "P99", "Max"]
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
# SAS: subsetting IFs -> filter(); a null LTV fails "LTV > 0" in both SAS and Spark
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# SAS equivalent:
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
print("\n" + "=" * 60)
print("Clean Dataset Summary (PROC MEANS)")
print("=" * 60)
procMeans(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["N", "NMiss", "Mean", "StdDev", "Min", "Max"]
).show(truncate=False)

print(f"Rows: input={df.count()}, filtered={dfFiltered.count()}, final={dfFinal.count()}")

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# SAS: FORMAT LTV percent8.2 -> applied at display time only
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE",
    concat(format_number(col("LTV") * 100, 2), lit("%")).alias("LTV"),
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
