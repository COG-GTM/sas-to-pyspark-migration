"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, median, stddev, min as spark_min, max as spark_max,
    round as spark_round, lit, when, initcap
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from script 02 so that df matches
# work.home_equity_final, including the LTV sanity filter 0 < LTV < 5)
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
df = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default"))
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull() &
        (col("LOAN") > 0) & (col("VALUE") > 0) &
        (col("LTV") > 0) & (col("LTV") < 5)
    )

# Register as temp view for SQL queries
df.createOrReplaceTempView("home_equity")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Frequency Tables (equivalent to PROC FREQ)")
print("=" * 60)

for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\n--- {catCol} ---")
    # PROC FREQ excludes missing values from the table and from the percent
    # denominator by default, and reports them in a "Frequency Missing" footer.
    nonMissing = df.filter(col(catCol).isNotNull())
    totalCount = nonMissing.count()
    nonMissing.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(col("Frequency").desc()) \
        .show(truncate=False)
    print(f"Frequency Missing = {df.count() - totalCount}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Summary Statistics by Loan Outcome (equivalent to PROC MEANS)")
print("=" * 60)

df.groupBy("LOAN_OUTCOME") \
    .agg(
        count("LOAN").alias("N"),
        spark_round(mean("LOAN"), 2).alias("Mean_LOAN"),
        spark_round(median("LOAN"), 2).alias("Median_LOAN"),
        spark_round(stddev("LOAN"), 2).alias("Std_LOAN"),
        spark_round(spark_min("LOAN"), 2).alias("Min_LOAN"),
        spark_round(spark_max("LOAN"), 2).alias("Max_LOAN"),
        spark_round(mean("MORTDUE"), 2).alias("Mean_MORTDUE"),
        spark_round(median("MORTDUE"), 2).alias("Median_MORTDUE"),
        spark_round(mean("VALUE"), 2).alias("Mean_VALUE"),
        spark_round(median("VALUE"), 2).alias("Median_VALUE"),
        spark_round(mean("DEBTINC"), 2).alias("Mean_DEBTINC"),
        spark_round(median("DEBTINC"), 2).alias("Median_DEBTINC"),
    ) \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2);
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Cross-tabulation: Default Rate by JOB x REGION")
print("(equivalent to PROC TABULATE)")
print("=" * 60)

# SAS CLASS processing drops rows whose class variable is missing
dfJobRegion = df.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Use crosstab for a pivot-style view
crossTab = dfJobRegion.stat.crosstab("JOB", "REGION")
crossTab.show(truncate=False)

# Detailed default rates by JOB and REGION
dfJobRegion.groupBy("JOB", "REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("JOB", "REGION") \
    .show(50, truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount
# SAS equivalent:
#   proc sql outobs=10;
#       select STATE, count(*) as num_loans,
#              mean(LOAN) as avg_loan,
#              mean(VALUE) as avg_property_value,
#              mean(BAD) as default_rate
#       from work.home_equity_final
#       group by STATE
#       having count(*) >= 10
#       order by avg_loan desc;
#   quit;
# ------------------------------------------------------------------
print("=" * 60)
print("Top 10 States by Average Loan Amount (equivalent to PROC SQL)")
print("=" * 60)

spark.sql("""
    SELECT
        STATE,
        COUNT(*) AS num_loans,
        ROUND(AVG(LOAN), 2) AS avg_loan,
        ROUND(AVG(VALUE), 2) AS avg_property_value,
        ROUND(AVG(BAD) * 100, 2) AS default_rate_pct
    FROM home_equity
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Additional reporting - Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Loan Distribution by Reason and Outcome")
print("=" * 60)

df.filter(col("REASON").isNotNull()) \
    .groupBy("REASON", "LOAN_OUTCOME") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("LOAN"), 2).alias("Mean_LOAN"),
        spark_round(median("LOAN"), 2).alias("Median_LOAN"),
        spark_round(stddev("LOAN"), 2).alias("Std_LOAN"),
        spark_round(mean("LTV"), 4).alias("Mean_LTV"),
        spark_round(median("LTV"), 4).alias("Median_LTV"),
        spark_round(mean("DEBTINC"), 2).alias("Mean_DEBTINC"),
        spark_round(median("DEBTINC"), 2).alias("Median_DEBTINC")
    ) \
    .orderBy("REASON", "LOAN_OUTCOME") \
    .show(truncate=False)

# Clean up
spark.stop()
