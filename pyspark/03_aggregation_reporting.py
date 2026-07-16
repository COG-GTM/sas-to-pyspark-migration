"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas

The SAS program runs against work.home_equity_final, the cleaned dataset
produced by 01_data_loading.sas + 02_data_cleaning.sas. To keep this script
runnable on its own, we first reconstruct home_equity_final from the raw CSV
(the derived columns and filters from 02_data_cleaning.sas), then perform the
aggregation and reporting steps that mirror 03_aggregation_reporting.sas.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as sMin, max as sMax,
    expr, round as sRound
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Reconstruct work.home_equity_final
# SAS equivalent (from 01_data_loading.sas + 02_data_cleaning.sas):
#   proc import datafile="/data/home_equity.csv" dbms=csv
#       out=work.home_equity replace; guessingrows=5960;
#   run;
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   ... filter non-missing LOAN/VALUE/BAD ...
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;  if LOAN > 0;  if VALUE > 0;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# Derived columns (from 02_data_cleaning.sas Step 1)
df = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY", initcap(col("CITY"))
)

# Filter non-missing critical fields (02_data_cleaning.sas Step 3)
df = df.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# Final dataset: outlier / sanity filters (02_data_cleaning.sas Step 5)
# SAS drops rows where LTV is missing because a missing value fails the
# comparison "LTV > 0 and LTV < 5".
homeEquityFinal = df.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) & (col("VALUE") > 0)
)

homeEquityFinal.cache()
print("=" * 60)
print("work.home_equity_final reconstructed")
print("=" * 60)
print(f"Rows in home_equity_final: {homeEquityFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# PROC FREQ -> groupBy(<var>).count(). One frequency table per variable,
# ordered by count descending (nocum: no cumulative columns).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
for freqCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\nFrequency of {freqCol}:")
    homeEquityFinal.groupBy(freqCol).count() \
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
# PROC MEANS with CLASS -> groupBy(...).agg(...). SAS median maps to
# percentile_approx(x, 0.5); SAS std maps to sample stddev.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)
meansByOutcome = homeEquityFinal.groupBy("LOAN_OUTCOME")
aggExprs = []
for v in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    aggExprs += [
        count(v).alias(f"N_{v}"),
        sRound(mean(v), 2).alias(f"Mean_{v}"),
        sRound(expr(f"percentile_approx({v}, 0.5)"), 2).alias(f"Median_{v}"),
        sRound(stddev(v), 2).alias(f"Std_{v}"),
        sRound(sMin(v), 2).alias(f"Min_{v}"),
        sRound(sMax(v), 2).alias(f"Max_{v}"),
    ]
meansByOutcome.agg(*aggExprs).orderBy("LOAN_OUTCOME").show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2) all='Total' * ...;
#   run;
#
# PROC TABULATE (count + mean of BAD by JOB x REGION) -> groupBy pivot.
# Default rate = mean(BAD); rounded to 4 decimals.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
print("\nLoan counts by JOB x REGION:")
homeEquityFinal.groupBy("JOB").pivot("REGION").agg(
    count("BAD")
).orderBy("JOB").show(truncate=False)
print("Default rate (mean BAD) by JOB x REGION:")
homeEquityFinal.groupBy("JOB").pivot("REGION").agg(
    sRound(mean("BAD"), 4)
).orderBy("JOB").show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using PROC SQL
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
#
# PROC SQL -> spark.sql on a temp view. outobs=10 -> LIMIT 10.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)
homeEquityFinal.createOrReplaceTempView("home_equity_final")
topStates = spark.sql("""
    SELECT STATE,
           COUNT(*)               AS num_loans,
           ROUND(AVG(LOAN), 2)    AS avg_loan,
           ROUND(AVG(VALUE), 2)   AS avg_property_value,
           ROUND(AVG(BAD), 4)     AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""")
topStates.show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Additional reporting - Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
#
# Two CLASS variables -> groupBy(REASON, LOAN_OUTCOME).agg(...).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)
distAggExprs = []
for v in ["LOAN", "LTV", "DEBTINC"]:
    distAggExprs += [
        count(v).alias(f"N_{v}"),
        sRound(mean(v), 4).alias(f"Mean_{v}"),
        sRound(stddev(v), 4).alias(f"Std_{v}"),
        sRound(expr(f"percentile_approx({v}, 0.5)"), 4).alias(f"Median_{v}"),
    ]
homeEquityFinal.groupBy("REASON", "LOAN_OUTCOME").agg(*distAggExprs) \
    .orderBy("REASON", "LOAN_OUTCOME").show(truncate=False)

# Clean up
spark.stop()
