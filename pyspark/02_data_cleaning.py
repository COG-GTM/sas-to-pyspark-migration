"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as spark_min,
    max as spark_max, percentile_approx
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load CSV data (same as 01_data_loading.py)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
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
#
# Note: IF/ELSE IF/ELSE maps to chained when(...).otherwise(...).
# SAS missing values (. for numeric, '' for character) map to null.
# propcase -> initcap is the closest built-in equivalent. initcap only
# capitalizes after spaces, while propcase also treats - ( . as word
# delimiters (e.g. SAS gives "Winston-Salem", initcap "Winston-salem").
# ------------------------------------------------------------------
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
        .otherwise(lit(None))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

# SAS equivalent:
#   label LTV = "Loan to Value Ratio"
#         LOAN_OUTCOME = "Loan Outcome";
#   format LTV percent8.2;
#
# Note: PySpark has no native column labels or display formats.
# We document them as metadata; percent formatting is applied only
# when presenting results.
derivedColumnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
derivedColumnFormats = {
    "LTV": "percent8.2",
}

print("=" * 60)
print("Derived Column Labels / Formats (SAS-style metadata)")
print("=" * 60)
for colName, label in derivedColumnLabels.items():
    fmt = derivedColumnFormats.get(colName, "")
    print(f"  {colName:12s} -> {label} {fmt}")

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
# Note: The SAS array / do i=1 to 8 loop is replaced by a Python loop
# over (variable, flag) pairs. Each iteration adds one column with
# withColumn; Spark builds a single plan, so no row-by-row loop runs.
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
numFlags = [f"{v}_MISS" for v in numVars]

dfImputed = dfClean
for varName, flagName in zip(numVars, numFlags):
    dfImputed = dfImputed.withColumn(
        flagName,
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
# Note: A subsetting IF maps to filter().
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)


def procMeans(data, variables, stats):
    """Return a PROC MEANS-style DataFrame: one row per variable."""
    percentiles = {
        "p1": 0.01, "p5": 0.05, "p25": 0.25, "median": 0.5,
        "p75": 0.75, "p95": 0.95, "p99": 0.99,
    }
    statExprs = {
        "n": lambda c: count(col(c)),
        "nmiss": lambda c: count(when(col(c).isNull(), lit(1))),
        "mean": lambda c: mean(col(c)),
        "std": lambda c: stddev(col(c)),
        "min": lambda c: spark_min(col(c)),
        "max": lambda c: spark_max(col(c)),
    }
    for name, pct in percentiles.items():
        statExprs[name] = lambda c, p=pct: percentile_approx(col(c), p, 100000)

    # One pass over the data computes every statistic for every variable
    aggExprs = [
        statExprs[s](v).alias(f"{v}__{s}") for v in variables for s in stats
    ]
    result = data.agg(*aggExprs).collect()[0]

    def toNumber(stat, value):
        if value is None:
            return None
        return int(value) if stat in ("n", "nmiss") else float(value)

    rows = [
        tuple([v] + [toNumber(s, result[f"{v}__{s}"]) for s in stats])
        for v in variables
    ]
    return spark.createDataFrame(rows, ["Variable"] + list(stats))


# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#
# Note: Percentiles use percentile_approx (high accuracy setting).
# SAS uses its own percentile definition (QNTLDEF=5), so values can
# differ slightly at tied or sparse points.
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlierStats = [
    "n", "nmiss", "mean", "std", "min", "p1", "p5", "p25",
    "median", "p75", "p95", "p99", "max",
]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (PROC MEANS)")
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
# Note: Comparisons with null evaluate to null in Spark, so rows with a
# missing LTV are dropped, the same as SAS treats . > 0 as false.
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 6: Summarize the final dataset
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Clean Dataset Summary (PROC MEANS)")
print("=" * 60)
print(f"Rows: input={df.count()}, filtered={dfFiltered.count()}, final={dfFinal.count()}")
procMeans(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["n", "nmiss", "mean", "std", "min", "max"],
).show(truncate=False)

# ------------------------------------------------------------------
# Step 7: Preview the final dataset
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
