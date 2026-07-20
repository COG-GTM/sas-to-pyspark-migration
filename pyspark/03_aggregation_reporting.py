"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
         - PROC FREQ for categorical distributions
         - PROC MEANS for numeric summaries by group
         - PROC TABULATE for cross-tabulations
         - PROC SQL for ad hoc queries
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, expr, lit, when, initcap,
    round as spark_round,
    min as spark_min,
    max as spark_max,
    percentile_approx,
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_Aggregation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Data preparation: rebuild work.home_equity_final
# SAS equivalent: the DATA steps in sas/02_data_cleaning.sas which
# produce work.home_equity_final consumed by this program.
# This script reloads data/home_equity.csv (same as 01_data_loading.py)
# and re-derives only the columns and filters needed here so it is
# self-contained and runnable on its own.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# SAS equivalent (02, Step 1 - DATA work.home_equity_clean):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   else LTV = .;
#   if BAD = 0 then LOAN_OUTCOME = 'Paid';
#   else if BAD = 1 then LOAN_OUTCOME = 'Default';
#   else LOAN_OUTCOME = '';
#   CITY = propcase(CITY);
#   format LTV percent8.2;  (SAS format - no functional PySpark equivalent)
df = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
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

# SAS equivalent (02, Step 3 - DATA work.home_equity_filtered):
#   if LOAN ne . and VALUE ne . and BAD ne .;
# SAS equivalent (02, Step 5 - DATA work.home_equity_final):
#   if LTV > 0 and LTV < 5;   /* LTV ratio sanity check */
#   if LOAN > 0;              /* Positive loan amounts only */
#   if VALUE > 0;             /* Positive property values only */
df = df.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)

# Register as a temp view for the PROC SQL step
df.createOrReplaceTempView("home_equity_final")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 70)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("(equivalent to PROC FREQ / nocum)")
print("=" * 70)

totalCount = df.count()
for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\n--- {catCol} ---")
    df.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(col(catCol).asc_nulls_last()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 70)
print("Summary Statistics by Loan Outcome (equivalent to PROC MEANS)")
print("=" * 70)

for var in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    print(f"\n--- {var} ---")
    df.groupBy("LOAN_OUTCOME") \
        .agg(
            count(var).alias("N"),
            spark_round(mean(var), 2).alias("Mean"),
            spark_round(percentile_approx(col(var), 0.5), 2).alias("Median"),
            spark_round(stddev(var), 2).alias("Std"),
            spark_round(spark_min(var), 2).alias("Min"),
            spark_round(spark_max(var), 2).alias("Max"),
        ) \
        .orderBy("LOAN_OUTCOME") \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   title "Default Rates by Job Category and Region";
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2) all='Total' * BAD * (n mean*f=percent8.2);
#   run;
# Note: PROC TABULATE has no direct PySpark equivalent; we reproduce the
# N and default-rate (mean of BAD) statistics grouped by JOB x REGION,
# plus JOB and overall totals.
# ------------------------------------------------------------------
print("=" * 70)
print("Default Rates by Job Category and Region (equivalent to PROC TABULATE)")
print("=" * 70)

# REGION * BAD * (n mean) within each JOB
df.groupBy("JOB", "REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy(col("JOB").asc_nulls_last(), col("REGION").asc_nulls_last()) \
    .show(50, truncate=False)

# JOB all='Total' row (default rate per JOB across all regions)
print("--- Totals by JOB (all='Total') ---")
df.groupBy("JOB") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy(col("JOB").asc_nulls_last()) \
    .show(truncate=False)

# Overall total (all='Total' * BAD)
print("--- Overall Total ---")
df.agg(
    count("*").alias("N"),
    spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using PROC SQL
# SAS equivalent:
#   title "Top 10 States by Average Loan Amount";
#   proc sql outobs=10;
#       select STATE,
#              count(*) as num_loans,
#              mean(LOAN) as avg_loan format=dollar12.,
#              mean(VALUE) as avg_property_value format=dollar12.,
#              mean(BAD) as default_rate format=percent8.2
#       from work.home_equity_final
#       group by STATE
#       having count(*) >= 10
#       order by avg_loan desc;
#   quit;
# Note: SAS dollar12./percent8.2 formats are display-only (no PySpark
# equivalent); default_rate is expressed as a percentage value here.
# ------------------------------------------------------------------
print("=" * 70)
print("Top 10 States by Average Loan Amount (equivalent to PROC SQL)")
print("=" * 70)

spark.sql("""
    SELECT
        STATE,
        COUNT(*) AS num_loans,
        ROUND(AVG(LOAN), 2) AS avg_loan,
        ROUND(AVG(VALUE), 2) AS avg_property_value,
        ROUND(AVG(BAD) * 100, 2) AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Additional reporting - Loan distribution by reason and outcome
# SAS equivalent:
#   title "Loan Amount Distribution by Reason and Outcome";
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 70)
print("Loan Amount Distribution by Reason and Outcome (equivalent to PROC MEANS)")
print("=" * 70)

for var in ["LOAN", "LTV", "DEBTINC"]:
    print(f"\n--- {var} ---")
    df.groupBy("REASON", "LOAN_OUTCOME") \
        .agg(
            count(var).alias("N"),
            spark_round(mean(var), 4).alias("Mean"),
            spark_round(stddev(var), 4).alias("Std"),
            spark_round(percentile_approx(col(var), 0.5), 4).alias("Median"),
        ) \
        .orderBy(col("REASON").asc_nulls_last(), col("LOAN_OUTCOME")) \
        .show(truncate=False)

# Clean up
spark.stop()
