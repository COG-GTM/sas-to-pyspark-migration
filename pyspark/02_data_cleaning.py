"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap, count, mean, stddev, min as sparkMin, max as sparkMax

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid'; else if BAD = 1 then ...;
#       CITY = propcase(CITY);
#   run;
#
# Note: a SAS DATA step assigns columns row by row; PySpark chains
# withColumn transformations that Catalyst evaluates lazily.
# ------------------------------------------------------------------
dfClean = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    # SAS propcase() -> Spark initcap()
    "CITY", initcap(col("CITY"))
)

# SAS labels have no PySpark equivalent; documented as metadata
derivedLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

# ------------------------------------------------------------------
# Step 2: Flag missing values
# SAS equivalent:
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS ... NINQ_MISS;
#   do i = 1 to 8; if num_vars{i} = . then num_flags{i} = 1; else 0; end;
#
# A SAS array loop becomes a Python loop that builds columns.
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
# SAS equivalent (subsetting IF):
#   if LOAN ne . and VALUE ne . and BAD ne .;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Summary statistics for outlier detection
# SAS equivalent:
#   proc means data=... n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#
# PROC MEANS percentiles map to DataFrame.approxQuantile.
# ------------------------------------------------------------------
statVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
percentiles = [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]


def describeVars(dataFrame, columns, title):
    """PROC MEANS equivalent: N, NMiss, Mean, Std, Min, percentiles, Max."""
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)
    totalRows = dataFrame.count()
    header = f"{'Variable':10s} {'N':>6s} {'NMiss':>6s} {'Mean':>12s} {'Std':>12s} {'Min':>12s}"
    header += "".join(f"{f'P{int(p * 100)}':>12s}" for p in percentiles)
    header += f"{'Max':>12s}"
    print(header)
    for varName in columns:
        stats = dataFrame.agg(
            count(col(varName)).alias("n"),
            mean(col(varName)).alias("mean"),
            stddev(col(varName)).alias("std"),
            sparkMin(col(varName)).alias("min"),
            sparkMax(col(varName)).alias("max"),
        ).collect()[0]
        quantiles = dataFrame.approxQuantile(varName, percentiles, 0.001)
        row = f"{varName:10s} {stats['n']:6d} {totalRows - stats['n']:6d}"
        for value in [stats["mean"], stats["std"], stats["min"]] + quantiles + [stats["max"]]:
            row += f"{value:12.2f}" if value is not None else f"{'.':>12s}"
        print(row)


describeVars(dfFiltered, statVars, "Summary Statistics for Outlier Detection")

# ------------------------------------------------------------------
# Step 5: Final clean dataset - remove extreme outliers
# SAS equivalent:
#   if LTV > 0 and LTV < 5;   /* subsetting IF also drops missing LTV */
#   if LOAN > 0;
#   if VALUE > 0;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)

describeVars(dfFinal, ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"], "Clean Dataset Summary")

print(f"\nRows after cleaning: {dfFinal.count()} (from {df.count()} raw records)")

# ------------------------------------------------------------------
# Step 6: Preview the clean dataset
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("First 10 Observations of the Clean Dataset")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

spark.stop()
