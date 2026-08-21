"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
         (PROC FREQ, PROC MEANS, PROC TABULATE, PROC SQL)
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, initcap, lit, count, mean, stddev, expr,
    min as sparkMin, max as sparkMax, round as sparkRound
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# Load the source dataset
# SAS equivalent: proc import datafile="/data/home_equity.csv" ...
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Recreate work.home_equity_final from 02_data_cleaning.sas so this
# script is self-contained.
# SAS equivalent:
#   data work.home_equity_clean;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
#   run;
# ------------------------------------------------------------------
dfFinal = df \
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
    .withColumn("CITY", initcap(col("CITY"))) \
    .filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    ) \
    .filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) & (col("VALUE") > 0)
    )

dfFinal.cache()

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# PROC FREQ -> groupBy().count(); percentages via a window-free
# division by the total row count.
# ------------------------------------------------------------------
totalRows = dfFinal.count()

print("=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
for colName in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\nFrequency of {colName}")
    dfFinal.groupBy(colName) \
        .count() \
        .withColumn(
            "percent",
            sparkRound(col("count") / lit(totalRows) * 100, 2)
        ) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#
# PROC MEANS with CLASS -> groupBy().agg(); the SAS MEDIAN statistic
# maps to percentile_approx(col, 0.5) (portable across Spark versions).
# ------------------------------------------------------------------
meansVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]

statsExprs = []
for varName in meansVars:
    statsExprs += [
        count(col(varName)).alias(f"{varName}_n"),
        sparkRound(mean(col(varName)), 2).alias(f"{varName}_mean"),
        sparkRound(
            expr(f"percentile_approx({varName}, 0.5)"), 2
        ).alias(f"{varName}_median"),
        sparkRound(stddev(col(varName)), 2).alias(f"{varName}_std"),
        sparkMin(col(varName)).alias(f"{varName}_min"),
        sparkMax(col(varName)).alias(f"{varName}_max"),
    ]

print("=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)
dfFinal.groupBy("LOAN_OUTCOME").agg(*statsExprs).show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2)
#             all='Total' * BAD * (n mean*f=percent8.2);
#   run;
#
# PROC TABULATE -> groupBy().pivot().agg(); the SAS ALL row/column
# is produced with separate aggregations and unioned in as a "Total".
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by Job Category and Region (counts)")
print("=" * 60)
dfFinal.groupBy("JOB").pivot("REGION").agg(count(col("BAD"))).show(truncate=False)

print("Default Rates by Job Category and Region (mean of BAD)")
dfFinal.groupBy("JOB").pivot("REGION") \
    .agg(sparkRound(mean(col("BAD")), 4)) \
    .show(truncate=False)

# SAS: all='Total' row/column margins
print("Total row: default rate by REGION (all job categories)")
dfFinal.groupBy("REGION") \
    .agg(
        count(col("BAD")).alias("n"),
        sparkRound(mean(col("BAD")), 4).alias("default_rate")
    ) \
    .orderBy("REGION") \
    .show(truncate=False)

print("Total column: default rate by JOB (all regions)")
dfFinal.groupBy("JOB") \
    .agg(
        count(col("BAD")).alias("n"),
        sparkRound(mean(col("BAD")), 4).alias("default_rate")
    ) \
    .orderBy("JOB") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount
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
#
# PROC SQL -> createOrReplaceTempView() + spark.sql(); OUTOBS=10 -> LIMIT 10.
# ------------------------------------------------------------------
dfFinal.createOrReplaceTempView("home_equity_final")

print("=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)
spark.sql("""
    SELECT STATE,
           COUNT(*) AS num_loans,
           ROUND(AVG(LOAN), 2) AS avg_loan,
           ROUND(AVG(VALUE), 2) AS avg_property_value,
           ROUND(AVG(BAD), 4) AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
#
# Multiple CLASS variables -> groupBy() with several columns.
# ------------------------------------------------------------------
distVars = ["LOAN", "LTV", "DEBTINC"]

distExprs = []
for varName in distVars:
    distExprs += [
        count(col(varName)).alias(f"{varName}_n"),
        sparkRound(mean(col(varName)), 4).alias(f"{varName}_mean"),
        sparkRound(stddev(col(varName)), 4).alias(f"{varName}_std"),
        sparkRound(
            expr(f"percentile_approx({varName}, 0.5)"), 4
        ).alias(f"{varName}_median"),
    ]

print("=" * 60)
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)
dfFinal.groupBy("REASON", "LOAN_OUTCOME") \
    .agg(*distExprs) \
    .orderBy("REASON", "LOAN_OUTCOME") \
    .show(truncate=False)

# Clean up
spark.stop()
