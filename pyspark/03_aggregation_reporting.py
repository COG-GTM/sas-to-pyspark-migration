"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    round as spark_round, lit, when, initcap, percentile_approx, coalesce
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
    .withColumn("CITY", initcap(col("CITY"))) \
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

# PROC FREQ drops missing values from the table and computes percentages over
# the non-missing observations, so each column is filtered before counting.
for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\n--- {catCol} ---")
    dfCat = df.filter(col(catCol).isNotNull())
    totalCount = dfCat.count()
    dfCat.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(col("Frequency").desc()) \
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

# SAS PROC MEANS reports n / mean / median / std / min / max for every analysis
# variable, so each VAR gets its own table of the same six statistics.
for analysisCol in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    print(f"\n--- {analysisCol} ---")
    df.groupBy("LOAN_OUTCOME") \
        .agg(
            count(analysisCol).alias("N"),
            spark_round(mean(analysisCol), 2).alias("Mean"),
            spark_round(percentile_approx(col(analysisCol), 0.5), 2).alias("Median"),
            spark_round(stddev(analysisCol), 2).alias("Std"),
            spark_round(spark_min(analysisCol), 2).alias("Min"),
            spark_round(spark_max(analysisCol), 2).alias("Max"),
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
#             REGION * BAD * (n mean*f=percent8.2) all='Total' * BAD *
#             (n mean*f=percent8.2);
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Cross-tabulation: Default Rate by JOB x REGION")
print("(equivalent to PROC TABULATE)")
print("=" * 60)

# Use crosstab for a pivot-style view
crossTab = df.stat.crosstab("JOB", "REGION")
crossTab.show(truncate=False)

# PROC TABULATE drops observations with a missing CLASS value
dfTab = df.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Detailed default rates by JOB and REGION, including the SAS all='Total'
# margins on both dimensions (cube produces the row, column and grand totals)
tabulate = dfTab.cube("JOB", "REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .withColumn("JOB", coalesce(col("JOB"), lit("Total"))) \
    .withColumn("REGION", coalesce(col("REGION"), lit("Total")))

tabulate.orderBy("JOB", "REGION").show(100, truncate=False)

# Same figures laid out as a PROC TABULATE-style pivot (default rate %)
dfTab.groupBy("JOB").pivot("REGION").agg(
    spark_round(mean("BAD") * 100, 2)
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

# n / mean / std / median for each analysis variable, one table per VAR
for analysisCol, decimals in [("LOAN", 2), ("LTV", 4), ("DEBTINC", 2)]:
    print(f"\n--- {analysisCol} ---")
    df.filter(col("REASON").isNotNull() & col("LOAN_OUTCOME").isNotNull()) \
        .groupBy("REASON", "LOAN_OUTCOME") \
        .agg(
            count(analysisCol).alias("N"),
            spark_round(mean(analysisCol), decimals).alias("Mean"),
            spark_round(stddev(analysisCol), decimals).alias("Std"),
            spark_round(percentile_approx(col(analysisCol), 0.5), decimals).alias("Median")
        ) \
        .orderBy("REASON", "LOAN_OUTCOME") \
        .show(truncate=False)

# Clean up
spark.stop()
