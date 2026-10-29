"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load CSV data (input to this program)
# SAS equivalent:
#   work.home_equity, created by sas/01_data_loading.sas via PROC IMPORT
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)


def procMeans(data, columns, statistics):
    """Build a PROC MEANS-style table: one row per variable, one column per statistic.

    SAS N counts non-missing values and NMISS counts missing values.
    STD is the sample standard deviation (SAS default, VARDEF=DF).
    Percentiles use percentile_approx (SAS computes exact percentiles).
    """
    statExprs = {
        "N": lambda c: F.count(F.col(c)),
        "NMISS": lambda c: F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)),
        "MEAN": lambda c: F.mean(c),
        "STD": lambda c: F.stddev_samp(c),
        "MIN": lambda c: F.min(c),
        "P1": lambda c: F.percentile_approx(c, 0.01, 100000),
        "P5": lambda c: F.percentile_approx(c, 0.05, 100000),
        "P25": lambda c: F.percentile_approx(c, 0.25, 100000),
        "MEDIAN": lambda c: F.percentile_approx(c, 0.50, 100000),
        "P75": lambda c: F.percentile_approx(c, 0.75, 100000),
        "P95": lambda c: F.percentile_approx(c, 0.95, 100000),
        "P99": lambda c: F.percentile_approx(c, 0.99, 100000),
        "MAX": lambda c: F.max(c),
    }
    aggExprs = [
        statExprs[stat](c)
        .cast("long" if stat in ("N", "NMISS") else "double")
        .alias(f"{c}__{stat}")
        for c in columns
        for stat in statistics
    ]
    result = data.agg(*aggExprs).first()
    rows = [
        tuple([c] + [result[f"{c}__{stat}"] for stat in statistics])
        for c in columns
    ]
    return spark.createDataFrame(rows, ["VARIABLE"] + statistics)


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
# Note: SAS propcase() capitalizes after blanks, slashes, hyphens,
# parentheses, periods and tabs. F.initcap() only splits on whitespace,
# so "winston-salem" becomes "Winston-salem" here vs "Winston-Salem" in SAS.
#
# Note: PySpark has no column labels or display formats. The labels are
# documented below; LTV is stored as a raw ratio (SAS percent8.2 would
# display 0.6627 as "66.27%").
# ------------------------------------------------------------------
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

dfClean = df \
    .withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() &
            F.col("MORTDUE").isNotNull() &
            (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE")
        ).otherwise(F.lit(None).cast("double"))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid"))
        .when(F.col("BAD") == 1, F.lit("Default"))
        .otherwise(F.lit(""))
    ) \
    .withColumn("CITY", F.initcap(F.col("CITY")))

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
#
# Note: SAS arrays become a Python loop over column names.
# ------------------------------------------------------------------
numericVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean
for colName in numericVars:
    dfImputed = dfImputed.withColumn(
        f"{colName}_MISS",
        F.when(F.col(colName).isNull(), 1).otherwise(0)
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    F.col("LOAN").isNotNull() &
    F.col("VALUE").isNotNull() &
    F.col("BAD").isNotNull()
)

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
# Note: df.summary() has no NMISS statistic, so explicit aggregations
# are used (see procMeans above).
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlierStats = ["N", "NMISS", "MEAN", "STD", "MIN", "P1", "P5", "P25",
                "MEDIAN", "P75", "P95", "P99", "MAX"]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (equivalent to PROC MEANS)")
print("=" * 60)
procMeans(dfFiltered, outlierVars, outlierStats).show(truncate=False)

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
# Note: a missing LTV fails the SAS subsetting IF (missing is not > 0),
# and a null LTV fails the Spark filter, so both drop those rows.
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (F.col("LTV") > 0) & (F.col("LTV") < 5) &
    (F.col("LOAN") > 0) &
    (F.col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 6: Summarize and preview the final dataset
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
#
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
finalVars = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]
finalStats = ["N", "NMISS", "MEAN", "STD", "MIN", "MAX"]

print("\n" + "=" * 60)
print("Clean Dataset Summary (equivalent to PROC MEANS)")
print("=" * 60)
procMeans(dfFinal, finalVars, finalStats).show(truncate=False)

print(f"Rows after critical-field filter: {dfFiltered.count()}")
print(f"Rows in final clean dataset: {dfFinal.count()}")

print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10)

# Clean up
spark.stop()
