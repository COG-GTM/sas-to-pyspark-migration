"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
  - PROC FREQ for categorical distributions
  - PROC MEANS for numeric summaries by group
  - PROC TABULATE for cross-tabulations
  - PROC SQL for ad hoc queries
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev,
    expr, round as spark_round,
    min as spark_min, max as spark_max
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load CSV data
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
# Reproduce the cleaning from 02_data_cleaning.sas inline so this
# script is self-contained. It builds work.home_equity_final.
# SAS equivalent (DATA steps):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#       LTV = MORTDUE / VALUE;
#   else LTV = .;
#   if BAD = 0 then LOAN_OUTCOME = 'Paid';
#   else if BAD = 1 then LOAN_OUTCOME = 'Default';
#   /* keep only records with non-missing LOAN, VALUE, BAD */
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LOAN > 0; if VALUE > 0;
#
# Note: PySpark has no native SAS labels/formats. The SAS labels
# were LTV = "Loan to Value Ratio" and LOAN_OUTCOME = "Loan Outcome"
# (LTV formatted percent8.2); documented here as comments only.
# ------------------------------------------------------------------
home_equity_final = df \
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
    .filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull() &
        (col("LOAN") > 0) &
        (col("VALUE") > 0)
    )

# Cache since we reuse this dataset across every report below
home_equity_final.cache()
print("=" * 60)
print("Working dataset: home_equity_final")
print("=" * 60)
print(f"Number of rows: {home_equity_final.count()}")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables (equivalent to PROC FREQ)")
print("=" * 60)
totalRows = home_equity_final.count()
for column in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\nFrequency of {column}:")
    home_equity_final.groupBy(column) \
        .agg(count(lit(1)).alias("Frequency")) \
        .withColumn(
            "Percent",
            spark_round(col("Frequency") / lit(totalRows) * 100, 2)
        ) \
        .orderBy(col("Frequency").desc()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#
# Note: median uses percentile_approx (equivalent to SAS median).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome (equivalent to PROC MEANS)")
print("=" * 60)
for var in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    print(f"\nStatistics for {var} by LOAN_OUTCOME:")
    home_equity_final.groupBy("LOAN_OUTCOME").agg(
        count(col(var)).alias("N"),
        spark_round(mean(col(var)), 2).alias("Mean"),
        spark_round(expr(f"percentile_approx({var}, 0.5)"), 2).alias("Median"),
        spark_round(stddev(col(var)), 2).alias("Std"),
        spark_round(spark_min(col(var)), 2).alias("Min"),
        spark_round(spark_max(col(var)), 2).alias("Max"),
    ).orderBy("LOAN_OUTCOME").show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2) ...;
#   run;
#
# BAD is the default flag (1=Default, 0=Paid), so mean(BAD)*100 is
# the default rate as a percentage.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region (equivalent to PROC TABULATE)")
print("=" * 60)

print("\nCrosstab of counts (JOB x REGION):")
home_equity_final.stat.crosstab("JOB", "REGION").show(truncate=False)

print("\nDefault rate (%) by JOB and REGION:")
home_equity_final.groupBy("JOB", "REGION").agg(
    count(lit(1)).alias("N"),
    spark_round(mean(col("BAD")) * 100, 2).alias("Default_Rate_Pct")
).orderBy("JOB", "REGION").show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using Spark SQL
# SAS equivalent:
#   proc sql outobs=10;
#       select STATE, count(*) as num_loans,
#              mean(LOAN) as avg_loan format=dollar12.,
#              mean(VALUE) as avg_property_value format=dollar12.,
#              mean(BAD) as default_rate format=percent8.2
#       from work.home_equity_final
#       group by STATE
#       having count(*) >= 10
#       order by avg_loan desc;
#   quit;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount (equivalent to PROC SQL)")
print("=" * 60)

# Register a temp view so we can query it with Spark SQL
home_equity_final.createOrReplaceTempView("home_equity_final")

spark.sql("""
    SELECT STATE,
           COUNT(*) AS num_loans,
           ROUND(AVG(LOAN), 2) AS avg_loan,
           ROUND(AVG(VALUE), 2) AS avg_property_value,
           ROUND(AVG(BAD) * 100, 2) AS default_rate_pct
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Loan amount distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome (equivalent to PROC MEANS)")
print("=" * 60)
for var in ["LOAN", "LTV", "DEBTINC"]:
    print(f"\nStatistics for {var} by REASON and LOAN_OUTCOME:")
    home_equity_final.groupBy("REASON", "LOAN_OUTCOME").agg(
        count(col(var)).alias("N"),
        spark_round(mean(col(var)), 4).alias("Mean"),
        spark_round(stddev(col(var)), 4).alias("Std"),
        spark_round(expr(f"percentile_approx({var}, 0.5)"), 4).alias("Median"),
    ).orderBy("REASON", "LOAN_OUTCOME").show(truncate=False)

# Clean up
spark.stop()
