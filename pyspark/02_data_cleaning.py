"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Flag missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap, sum as spark_sum

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load the raw data (output of 01_data_loading.py / work.home_equity)
# Note: empty CSV fields load as null, which plays the role of SAS missing (.)
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)


def meansSummary(data, columns, statistics):
    """PROC MEANS-style table: df.summary() statistics plus an nmiss row."""
    stats = data.select(columns).summary(*statistics)
    nmiss = data.select(
        lit("nmiss").alias("summary"),
        *[spark_sum(col(c).isNull().cast("int")).cast("string").alias(c)
          for c in columns]
    )
    countRow = stats.filter(col("summary") == "count")
    otherRows = stats.filter(col("summary") != "count")
    return countRow.unionByName(nmiss).unionByName(otherRows)


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
    .withColumn("CITY", initcap(col("CITY")))

# Note: PySpark does not have native column labels or formats like SAS.
# We document them as metadata in dictionaries.
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",
}

print("=" * 60)
print("Derived Column Labels and Formats (SAS-style)")
print("=" * 60)
for colName, label in columnLabels.items():
    fmt = columnFormats.get(colName, "")
    print(f"  {colName:12s} -> {label}" + (f" [format {fmt}]" if fmt else ""))

# ------------------------------------------------------------------
# Step 2: Flag missing values
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
#
# Note: The SAS array/DO loop becomes a single select with one column
# expression per variable, evaluated in parallel by Spark.
# ------------------------------------------------------------------
missingFlagColumns = [
    "LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"
]
dfImputed = dfClean.select(
    "*",
    *[when(col(c).isNull(), lit(1)).otherwise(lit(0)).alias(f"{c}_MISS")
      for c in missingFlagColumns]
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
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#
# Note: df.summary() percentiles are approximate (Greenwald-Khanna);
# nmiss has no summary() equivalent, so null counts are added as a row.
# ------------------------------------------------------------------
outlierColumns = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"
]
print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection")
print("=" * 60)
print(f"Number of rows: {dfFiltered.count()}")
meansSummary(
    dfFiltered,
    outlierColumns,
    ["count", "mean", "stddev", "min", "1%", "5%", "25%", "50%",
     "75%", "95%", "99%", "max"]
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
#
# Note: Comparisons with null evaluate to null in Spark, so rows with a
# missing LTV are dropped, matching SAS where missing (.) fails LTV > 0.
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# Keep the final dataset available for downstream steps in this session
# SAS equivalent: work.home_equity_final
dfFinal.cache()
dfFinal.createOrReplaceTempView("home_equity_final")

# ------------------------------------------------------------------
# Step 6: Summarize the clean dataset
# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")
meansSummary(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["count", "mean", "stddev", "min", "max"]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 7: Preview first 10 observations
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
