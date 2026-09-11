"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev,
    min as spark_min, max as spark_max, percentile_approx,
    sum as spark_sum
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

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
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfClean = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() &
        col("MORTDUE").isNotNull() &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY",
    initcap(col("CITY"))
)

# SAS labels and formats have no direct PySpark equivalent.
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

print("=" * 60)
print("Derived Column Labels (SAS labels/formats)")
print("=" * 60)
for columnName, label in columnLabels.items():
    print(f"  {columnName:12s} -> {label}")
print("  LTV format: percent8.2 (SAS format; no direct PySpark equivalent)")

# ------------------------------------------------------------------
# Step 2: Create missing-value indicator columns
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
#   run;
# ------------------------------------------------------------------
numVars = [
    "LOAN", "MORTDUE", "VALUE", "YOJ",
    "DEROG", "DELINQ", "CLAGE", "NINQ"
]

# SAS array/do-loop maps to a Python loop over the numeric columns.
dfImputed = dfClean
for c in numVars:
    dfImputed = dfImputed.withColumn(
        f"{c}_MISS",
        when(col(c).isNull(), 1).otherwise(0)
    )

# ------------------------------------------------------------------
# Step 3: Filter records with missing critical fields
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

print("\n" + "=" * 60)
print("Filtering Missing Critical Fields")
print("=" * 60)
print(f"Rows before filtering: {dfImputed.count()}")
print(f"Rows after filtering: {dfFiltered.count()}")

# ------------------------------------------------------------------
# Step 4: Calculate summary statistics for outlier detection
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
summaryCols = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "LTV", "CLAGE", "DEROG", "DELINQ"
]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
dfFiltered.describe(*summaryCols).show()

percentiles = [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]
summaryRows = []
for c in summaryCols:
    percentileValues = percentile_approx(col(c), percentiles, 10000)
    summaryRow = dfFiltered.agg(
        lit(c).alias("Variable"),
        count(col(c)).alias("N"),
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias("NMISS"),
        mean(col(c)).alias("Mean"),
        stddev(col(c)).alias("StdDev"),
        spark_min(col(c)).alias("Min"),
        percentileValues.alias("Percentiles"),
        spark_max(col(c)).alias("Max")
    ).select(
        "Variable",
        "N",
        "NMISS",
        "Mean",
        "StdDev",
        "Min",
        col("Percentiles")[0].alias("P1"),
        col("Percentiles")[1].alias("P5"),
        col("Percentiles")[2].alias("P25"),
        col("Percentiles")[3].alias("Median"),
        col("Percentiles")[4].alias("P75"),
        col("Percentiles")[5].alias("P95"),
        col("Percentiles")[6].alias("P99"),
        "Max"
    )
    summaryRows.append(summaryRow)

outlierStats = summaryRows[0]
for summaryRow in summaryRows[1:]:
    outlierStats = outlierStats.unionByName(summaryRow)
outlierStats.show(truncate=False)

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
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) &
    (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
dfFinal.describe(
    "LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"
).show()
print(f"Final row count: {dfFinal.count()}")

print("\n" + "=" * 60)
print("First 10 Clean Dataset Observations")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
