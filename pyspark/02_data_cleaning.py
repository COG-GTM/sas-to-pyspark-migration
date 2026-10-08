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
from pyspark.sql.functions import col, when, initcap

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load source data (produced by 01_data_loading in SAS as work.home_equity)
dataPath = "data/home_equity.csv"
df = spark.read.csv(dataPath, header=True, inferSchema=True)

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
# Note: PySpark has no native column labels or display formats.
# Labels are documented in a dictionary; percent8.2 is a display-only
# format, so LTV is kept as a raw ratio (format at presentation time).
# ------------------------------------------------------------------
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(None)
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, "Paid")
        .when(col("BAD") == 1, "Default")
        .otherwise(None)
    ) \
    .withColumn("CITY", initcap(col("CITY")))

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
#       array num_flags{8} LOAN_MISS MORTDUE_MISS ... NINQ_MISS;
#       do i = 1 to 8;
#           if num_vars{i} = . then num_flags{i} = 1;
#           else num_flags{i} = 0;
#       end;
#       drop i;
#   run;
#
# Note: SAS ARRAY + DO loop becomes a Python list comprehension that
# builds all flag columns in a single select (no row-level loop).
# ------------------------------------------------------------------
missingFlagCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean.select(
    "*",
    *[when(col(c).isNull(), 1).otherwise(0).alias(f"{c}_MISS") for c in missingFlagCols]
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
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)


def summaryStats(data, columns, percentiles=False):
    """Build a PROC MEANS-style table: one row per variable."""
    statRows = None
    for c in columns:
        aggs = [
            F.lit(c).alias("VARIABLE"),
            F.count(col(c)).alias("N"),
            F.sum(when(col(c).isNull(), 1).otherwise(0)).alias("NMISS"),
            F.mean(col(c)).alias("MEAN"),
            F.stddev(col(c)).alias("STD"),
            F.min(col(c)).alias("MIN"),
        ]
        if percentiles:
            pctLabels = ["P1", "P5", "P25", "MEDIAN", "P75", "P95", "P99"]
            pctValues = F.percentile_approx(
                col(c), [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
            )
            aggs += [pctValues[i].alias(name) for i, name in enumerate(pctLabels)]
        aggs.append(F.max(col(c)).alias("MAX"))
        row = data.agg(*aggs)
        statRows = row if statRows is None else statRows.unionByName(row)
    return statRows


# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#
# Note: percentile_approx gives approximate percentiles (distributed
# friendly); values may differ slightly from SAS's exact definition.
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]

print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (equivalent to PROC MEANS)")
print("=" * 60)
print(f"Number of rows: {dfFiltered.count()}")
summaryStats(dfFiltered, outlierVars, percentiles=True).show(truncate=False)

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
# Note: a null LTV fails the comparison, so those rows are dropped,
# matching SAS subsetting-IF behaviour for missing values.
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 6: Summarize and preview the final dataset
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
finalVars = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]

print("\n" + "=" * 60)
print("Clean Dataset Summary (equivalent to PROC MEANS)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")
summaryStats(dfFinal, finalVars).show(truncate=False)

print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
