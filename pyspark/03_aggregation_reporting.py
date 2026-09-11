"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    round as spark_round, lit, when, coalesce, expr
)


def median(colName):
    # Exact median (SAS PCTLDEF=5): average of the two middle values for even N
    return expr(f"percentile({colName}, 0.5)")


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
    nonMissing = df.filter(col(catCol).isNotNull())
    totalCount = nonMissing.count()
    nonMissing.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(col("Frequency").desc()) \
        .show(truncate=False)
    print(f"Frequency Missing = {df.count() - totalCount}\n")

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

summaryByOutcome = None
for var in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
    varStats = df.groupBy("LOAN_OUTCOME") \
        .agg(
            lit(var).alias("Variable"),
            count(var).alias("N"),
            spark_round(mean(var), 2).alias("Mean"),
            spark_round(median(var), 2).alias("Median"),
            spark_round(stddev(var), 2).alias("Std"),
            spark_round(spark_min(var), 2).alias("Min"),
            spark_round(spark_max(var), 2).alias("Max")
        )
    summaryByOutcome = varStats if summaryByOutcome is None else summaryByOutcome.union(varStats)

summaryByOutcome.orderBy("LOAN_OUTCOME", "Variable").show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2) all='Total' * BAD * (n mean*f=percent8.2);
#   run;
# CLASS variables drop rows with a missing class value; cube() supplies the
# 'Total' margins.
# ------------------------------------------------------------------
print("=" * 60)
print("Cross-tabulation: Default Rate by JOB x REGION")
print("(equivalent to PROC TABULATE)")
print("=" * 60)

tabulateBase = df.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Use crosstab for a pivot-style view
crossTab = tabulateBase.stat.crosstab("JOB", "REGION")
crossTab.show(truncate=False)

# Detailed default rates by JOB and REGION, with 'Total' margins
tabulateBase.cube("JOB", "REGION") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .select(
        coalesce(col("JOB"), lit("Total")).alias("JOB"),
        coalesce(col("REGION"), lit("Total")).alias("REGION"),
        col("N"), col("Default_Rate_Pct")
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

reasonBase = df.filter(col("REASON").isNotNull())
loanDistribution = None
for var in ["LOAN", "LTV", "DEBTINC"]:
    varStats = reasonBase.groupBy("REASON", "LOAN_OUTCOME") \
        .agg(
            lit(var).alias("Variable"),
            count(var).alias("N"),
            spark_round(mean(var), 4).alias("Mean"),
            spark_round(stddev(var), 4).alias("Std"),
            spark_round(median(var), 4).alias("Median")
        )
    loanDistribution = varStats if loanDistribution is None else loanDistribution.union(varStats)

loanDistribution.orderBy("REASON", "LOAN_OUTCOME", "Variable").show(truncate=False)

# Clean up
spark.stop()
