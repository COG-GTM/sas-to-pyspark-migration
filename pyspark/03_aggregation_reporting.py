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
    col, count, expr, initcap, lit, mean, round as spark_round, stddev, when,
    min as spark_min, max as spark_max
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Input dataset: work.home_equity_final
# The SAS program reads the cleaned dataset produced by
# sas/02_data_cleaning.sas. The cleaning is reproduced inline here so
# this script is self-contained and runnable from the repo root.
# SAS equivalent (02_data_cleaning.sas Steps 1, 3 and 5):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   if BAD = 0 then LOAN_OUTCOME = 'Paid';
#   else if BAD = 1 then LOAN_OUTCOME = 'Default'; else LOAN_OUTCOME = '';
#   CITY = propcase(CITY);
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfFinal = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY",
    initcap(col("CITY"))
).filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# Cache: every reporting step below scans the same dataset
dfFinal.cache()
totalRows = dfFinal.count()
print("=" * 60)
print("Input Dataset (work.home_equity_final)")
print("=" * 60)
print(f"Number of rows: {totalRows}")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: /nocum suppresses cumulative columns, so only Frequency and
# Percent are reported. PROC FREQ orders groups by formatted value.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)

frequencyVars = ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]
for frequencyVar in frequencyVars:
    print(f"\n--- {frequencyVar} ---")
    dfFinal.groupBy(frequencyVar) \
        .agg(count(lit(1)).alias("Frequency")) \
        .withColumn(
            "Percent",
            spark_round(col("Frequency") / lit(totalRows) * 100, 2)
        ) \
        .orderBy(col(frequencyVar).asc_nulls_first()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#
# Note: PROC MEANS reports one row per analysis variable per CLASS
# level; here each variable gets its own grouped table.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome (PROC MEANS)")
print("=" * 60)


def procMeans(inputDf, classVars, analysisVars):
    """Emit one PROC MEANS-style table per analysis variable."""
    for analysisVar in analysisVars:
        print(f"\n--- {analysisVar} by {', '.join(classVars)} ---")
        inputDf.groupBy(*classVars).agg(
            count(col(analysisVar)).alias("N"),
            spark_round(mean(col(analysisVar)), 2).alias("Mean"),
            spark_round(
                expr(f"percentile_approx({analysisVar}, 0.5)"), 2
            ).alias("Median"),
            spark_round(stddev(col(analysisVar)), 2).alias("StdDev"),
            spark_min(col(analysisVar)).alias("Min"),
            spark_max(col(analysisVar)).alias("Max"),
        ).orderBy(*[col(c).asc_nulls_first() for c in classVars]) \
            .show(truncate=False)


procMeans(
    dfFinal,
    ["LOAN_OUTCOME"],
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]
)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   title "Default Rates by Job Category and Region";
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2)
#             all='Total' * BAD * (n mean*f=percent8.2);
#   run;
#
# Note: PROC TABULATE has no direct equivalent; groupBy().pivot()
# reproduces the REGION columns, and the all='Total' row/column are
# emitted as extra aggregations.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region (PROC TABULATE)")
print("=" * 60)

print("\n--- N of BAD by JOB (rows) x REGION (columns) ---")
dfFinal.groupBy("JOB").pivot("REGION").agg(count(col("BAD"))).orderBy(
    col("JOB").asc_nulls_first()
).show(truncate=False)

print("\n--- Mean of BAD (default rate %) by JOB x REGION ---")
dfFinal.groupBy("JOB").pivot("REGION").agg(
    spark_round(mean(col("BAD")) * 100, 2)
).orderBy(col("JOB").asc_nulls_first()).show(truncate=False)

print("\n--- Total column: all REGIONs, by JOB ---")
dfFinal.groupBy("JOB").agg(
    count(col("BAD")).alias("N"),
    spark_round(mean(col("BAD")) * 100, 2).alias("Default_Rate_Pct")
).orderBy(col("JOB").asc_nulls_first()).show(truncate=False)

print("\n--- Total row: all JOBs, by REGION ---")
dfFinal.groupBy("REGION").agg(
    count(col("BAD")).alias("N"),
    spark_round(mean(col("BAD")) * 100, 2).alias("Default_Rate_Pct")
).orderBy(col("REGION").asc_nulls_first()).show(truncate=False)

print("\n--- Grand total: all JOBs and all REGIONs ---")
dfFinal.agg(
    count(col("BAD")).alias("N"),
    spark_round(mean(col("BAD")) * 100, 2).alias("Default_Rate_Pct")
).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount
# SAS equivalent:
#   title "Top 10 States by Average Loan Amount";
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
# Note: the SAS source used PROC SQL, so Spark SQL is used here.
# outobs=10 maps to LIMIT 10; the SAS dollar/percent formats are
# display-only and reproduced by rounding.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount (PROC SQL)")
print("=" * 60)

dfFinal.createOrReplaceTempView("home_equity_final")

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
# Step 5: Loan distribution by reason and outcome
# SAS equivalent:
#   title "Loan Amount Distribution by Reason and Outcome";
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome (PROC MEANS)")
print("=" * 60)

procMeans(
    dfFinal,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"]
)

# Clean up
dfFinal.unpersist()
spark.stop()
