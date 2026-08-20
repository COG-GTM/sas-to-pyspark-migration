"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, median, stddev, min as spark_min, max as spark_max,
    round as spark_round, lit, when
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from script 02)
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
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) & (col("VALUE") > 0)
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
    # PROC FREQ excludes missing values and lists levels in ascending order
    nonMissing = df.filter(col(catCol).isNotNull())
    nonMissingCount = nonMissing.count()
    nonMissing.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(nonMissingCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(catCol) \
        .show(truncate=False)

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

for numVar in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    print(f"\n--- {numVar} ---")
    df.groupBy("LOAN_OUTCOME") \
        .agg(
            count(numVar).alias("N"),
            spark_round(mean(numVar), 2).alias("Mean"),
            spark_round(median(numVar), 2).alias("Median"),
            spark_round(stddev(numVar), 2).alias("Std"),
            spark_round(spark_min(numVar), 2).alias("Min"),
            spark_round(spark_max(numVar), 2).alias("Max"),
        ) \
        .orderBy("LOAN_OUTCOME") \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2) all='Total' * BAD * (n mean*f=percent8.2);
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Cross-tabulation: Default Rate by JOB x REGION")
print("(equivalent to PROC TABULATE)")
print("=" * 60)

# PROC TABULATE excludes observations with missing CLASS values
tabDf = df.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Use crosstab for a pivot-style view of counts
crossTab = tabDf.stat.crosstab("JOB", "REGION")
crossTab.show(truncate=False)

# Detailed default rates by JOB and REGION
tabDf.groupBy("JOB", "REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("JOB", "REGION") \
    .show(50, truncate=False)

# all='Total' column: per-JOB totals across all regions
print("Totals by JOB (all regions):")
tabDf.groupBy("JOB") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("JOB") \
    .show(truncate=False)

# all='Total' row: per-REGION totals across all job categories
print("Totals by REGION (all job categories):")
tabDf.groupBy("REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("REGION") \
    .show(truncate=False)

# Grand total (Total row x Total column)
print("Grand Total:")
tabDf.agg(
    count("*").alias("N"),
    spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
).show(truncate=False)

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

# PROC MEANS excludes observations with missing CLASS values
reasonDf = df.filter(col("REASON").isNotNull() & col("LOAN_OUTCOME").isNotNull())

for numVar in ["LOAN", "LTV", "DEBTINC"]:
    print(f"\n--- {numVar} ---")
    reasonDf.groupBy("REASON", "LOAN_OUTCOME") \
        .agg(
            count(numVar).alias("N"),
            spark_round(mean(numVar), 4).alias("Mean"),
            spark_round(stddev(numVar), 4).alias("Std"),
            spark_round(median(numVar), 4).alias("Median"),
        ) \
        .orderBy("REASON", "LOAN_OUTCOME") \
        .show(truncate=False)

# Clean up
spark.stop()
