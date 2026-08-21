"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
         (derived columns, missing-value flags, filtering, outlier detection)
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, initcap, lit, count, mean, stddev, min as sparkMin,
    max as sparkMax, round as sparkRound
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load the source dataset produced by 01_data_loading.py
# SAS equivalent: set work.home_equity;
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
# ------------------------------------------------------------------
dfClean = df \
    .withColumn(
        "LTV",
        # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0
        # when() without otherwise() yields null, matching SAS LTV = .
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        # SAS: if/else if chain becomes when().when()
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .withColumn(
        "CITY",
        initcap(col("CITY"))  # SAS equivalent: CITY = propcase(CITY);
    )

# ------------------------------------------------------------------
# Step 2: Flag missing values (SAS array + do-loop)
# SAS equivalent:
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
#                      DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
#   do i = 1 to 8;
#       if num_vars{i} = . then num_flags{i} = 1;
#       else num_flags{i} = 0;
#   end;
#
# The SAS array/do-loop becomes a Python loop over column names.
# ------------------------------------------------------------------
numCols = [
    "LOAN", "MORTDUE", "VALUE", "YOJ",
    "DEROG", "DELINQ", "CLAGE", "NINQ"
]

for numCol in numCols:
    dfClean = dfClean.withColumn(
        f"{numCol}_MISS",
        when(col(numCol).isNull(), lit(1)).otherwise(lit(0))
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


def summaryStats(dataFrame, columns, withPercentiles=False):
    """PROC MEANS equivalent: one row of statistics per analysis variable.

    SAS equivalent:
        proc means data=... n nmiss mean std min p1 p5 p25 median p75 p95
                            p99 max;
            var <columns>;
        run;
    """
    totalRows = dataFrame.count()
    percentileLabels = ["P1", "P5", "P25", "Median", "P75", "P95", "P99"]
    schema = ["Variable", "N", "NMiss", "Mean", "StdDev", "Min"]
    if withPercentiles:
        schema += percentileLabels
    schema.append("Max")

    rows = []
    for statCol in columns:
        stats = dataFrame.agg(
            count(col(statCol)).alias("N"),
            sparkRound(mean(col(statCol)), 4).alias("Mean"),
            sparkRound(stddev(col(statCol)), 4).alias("StdDev"),
            sparkRound(sparkMin(col(statCol)), 4).alias("Min"),
            sparkRound(sparkMax(col(statCol)), 4).alias("Max"),
        ).collect()[0]

        row = {
            "Variable": statCol,
            "N": stats["N"],
            "NMiss": totalRows - stats["N"],
            "Mean": float(stats["Mean"]),
            "StdDev": float(stats["StdDev"]),
            "Min": float(stats["Min"]),
            "Max": float(stats["Max"]),
        }

        if withPercentiles:
            # SAS p1 p5 p25 median p75 p95 p99 -> approxQuantile
            probabilities = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
            quantiles = dataFrame.approxQuantile(statCol, probabilities, 0.001)
            for label, value in zip(percentileLabels, quantiles):
                row[label] = float(round(value, 4))

        rows.append(tuple(row[name] for name in schema))

    return spark.createDataFrame(rows, schema=schema)


# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("(equivalent to PROC MEANS with percentiles)")
print("=" * 60)
summaryStats(
    dfFiltered,
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"],
    withPercentiles=True
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
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Original rows: {df.count()}")
print(f"After filtering missing critical fields: {dfFiltered.count()}")
print(f"Final clean rows: {dfFinal.count()}\n")

# SAS equivalent:
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
summaryStats(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]
).show(truncate=False)

# ------------------------------------------------------------------
# Preview the final dataset
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
