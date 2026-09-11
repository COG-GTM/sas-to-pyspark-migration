"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, initcap, lit, sum as spark_sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load data
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)


def describeWithNmiss(frame, cols):
    """describe() plus a PROC MEANS-style NMISS row (missing count per variable)."""
    selected = frame.select(*cols)
    nmissRow = selected.select(
        lit("nmiss").alias("summary"),
        *[spark_sum(when(col(c).isNull(), 1).otherwise(0)).cast("string").alias(c) for c in cols]
    )
    return selected.describe().union(nmissRow)


# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
# ------------------------------------------------------------------
dfClean = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .withColumn(
        "CITY",
        initcap(col("CITY"))  # SAS equivalent: propcase(CITY)
    )

# ------------------------------------------------------------------
# Step 2: Flag missing values
# SAS equivalent:
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS MORTDUE_MISS ...;
#   do i = 1 to 8;
#       if num_vars{i} = . then num_flags{i} = 1;
#       else num_flags{i} = 0;
#   end;
# ------------------------------------------------------------------
numCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

for colName in numCols:
    flagName = f"{colName}_MISS"
    dfClean = dfClean.withColumn(
        flagName,
        when(col(colName).isNull(), lit(1)).otherwise(lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("(equivalent to PROC MEANS with percentiles)")
print("=" * 60)
outlierCols = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
describeWithNmiss(dfFiltered, outlierCols).show()

# Exact percentiles (relativeError=0) for every PROC MEANS variable
print("Percentiles:")
for quantileCol in outlierCols:
    quantiles = dfFiltered.approxQuantile(
        quantileCol, [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99], 0.0
    )
    print(f"  {quantileCol}: p1={quantiles[0]}, p5={quantiles[1]}, "
          f"p25={quantiles[2]}, median={quantiles[3]}, "
          f"p75={quantiles[4]}, p95={quantiles[5]}, p99={quantiles[6]}")

# ------------------------------------------------------------------
# Step 5: Create final clean dataset
# SAS equivalent:
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Original rows: {df.count()}")
print(f"After filtering: {dfFiltered.count()}")
print(f"Final clean rows: {dfFinal.count()}")

describeWithNmiss(dfFinal, ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]).show()

dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
