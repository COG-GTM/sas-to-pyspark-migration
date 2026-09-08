"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
  - PROC FREQ for categorical distributions
  - PROC MEANS for numeric summaries by group
  - PROC TABULATE for cross-tabulations
  - PROC SQL for ad hoc queries
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, median, stddev, min, max,
    round as spark_round, format_string, coalesce, grouping
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild work.home_equity_final
# SAS equivalent:
#   The SAS program reads work.home_equity_final, which is produced by
#   sas/02_data_cleaning.sas and persists in the WORK library between
#   programs. Each PySpark script starts from the raw CSV, so the
#   cleaning steps that define home_equity_final are replayed here:
#     LTV          = MORTDUE / VALUE   (when VALUE, MORTDUE non-missing, VALUE > 0)
#     LOAN_OUTCOME = 'Paid' / 'Default' / ''
#     keep LOAN, VALUE, BAD non-missing; 0 < LTV < 5; LOAN > 0; VALUE > 0
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

homeEquityFinal = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() &
        col("MORTDUE").isNotNull() &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5)
).filter(
    col("LOAN") > 0
).filter(
    col("VALUE") > 0
).cache()

print("=" * 60)
print(f"work.home_equity_final rebuilt: {homeEquityFinal.count()} rows")
print("=" * 60)

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#   title;
#
# Mapping:
#   PROC FREQ -> groupBy().count() per variable
#   Frequency/Percent columns are computed explicitly; NOCUM means no
#   cumulative columns, so none are added. PROC FREQ excludes missing
#   values from the table by default, so nulls are filtered out.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)

freqVars = ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]

for freqVar in freqVars:
    nonMissing = homeEquityFinal.filter(col(freqVar).isNotNull())
    totalNonMissing = nonMissing.count()
    freqTable = nonMissing.groupBy(freqVar).agg(
        count(lit(1)).alias("Frequency")
    ).withColumn(
        "Percent",
        spark_round(col("Frequency") / lit(totalNonMissing) * 100, 2)
    ).orderBy(freqVar)

    print(f"\nFrequency table for {freqVar} (equivalent to PROC FREQ)")
    freqTable.show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#   title;
#
# Mapping:
#   PROC MEANS with CLASS -> groupBy(class).agg() per analysis variable,
#   stacked into one long table (one row per class level x variable) to
#   mirror the PROC MEANS report layout. N counts non-missing values of
#   each variable. Missing CLASS values are excluded, as in SAS.
# ------------------------------------------------------------------


def summaryByClass(dataFrame, classVars, analysisVars, stats):
    """Emulate PROC MEANS with CLASS: one row per class level and variable."""
    statFuncs = {
        "N": lambda c: count(c),
        "Mean": lambda c: mean(c),
        "Median": lambda c: median(c),
        "Std": lambda c: stddev(c),
        "Min": lambda c: min(c),
        "Max": lambda c: max(c),
    }
    classFilter = reduce(
        lambda a, b: a & b, [col(c).isNotNull() for c in classVars]
    )
    perVariable = []
    for analysisVar in analysisVars:
        aggs = [
            statFuncs[stat](col(analysisVar)).alias(stat) for stat in stats
        ]
        perVariable.append(
            dataFrame.filter(classFilter)
            .groupBy(*classVars)
            .agg(*aggs)
            .withColumn("Variable", lit(analysisVar))
            .select(*classVars, "Variable", *stats)
        )
    return reduce(DataFrame.unionByName, perVariable) \
        .orderBy(*classVars, "Variable")


print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome (equivalent to PROC MEANS)")
print("=" * 60)
summaryByOutcome = summaryByClass(
    homeEquityFinal,
    classVars=["LOAN_OUTCOME"],
    analysisVars=["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    stats=["N", "Mean", "Median", "Std", "Min", "Max"],
)
summaryByOutcome.show(truncate=False)

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
#   title;
#
# Mapping:
#   PROC TABULATE -> cube("JOB", "REGION") so that every combination of
#   JOB x REGION, JOB x all, all x REGION and all x all is produced
#   (the ALL='Total' row and column). grouping() tells the totals apart
#   from real levels and labels them 'Total'. mean*f=percent8.2 is
#   rendered as a formatted percent string; the pivot() view then lays
#   REGION out across the columns as in the SAS report.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region (equivalent to PROC TABULATE)")
print("=" * 60)

defaultRatesLong = homeEquityFinal.filter(
    col("JOB").isNotNull() & col("REGION").isNotNull()
).cube("JOB", "REGION").agg(
    count("BAD").alias("N"),
    mean("BAD").alias("MeanBAD"),
    grouping("JOB").alias("jobIsTotal"),
    grouping("REGION").alias("regionIsTotal"),
).select(
    when(col("jobIsTotal") == 1, lit("Total")).otherwise(col("JOB")).alias("JOB"),
    when(col("regionIsTotal") == 1, lit("Total")).otherwise(col("REGION")).alias("REGION"),
    col("N"),
    format_string("%.2f%%", col("MeanBAD") * 100).alias("Default_Rate"),
    col("jobIsTotal"),
    col("regionIsTotal"),
)

print("\nLong format (one row per JOB x REGION cell):")
defaultRatesLong.orderBy("jobIsTotal", "JOB", "regionIsTotal", "REGION") \
    .select("JOB", "REGION", "N", "Default_Rate") \
    .show(n=100, truncate=False)

print("\nCross-tab layout (REGION across, JOB down):")
defaultRatesLong.groupBy("jobIsTotal", "JOB").pivot("REGION").agg(
    max("N").alias("N"),
    max("Default_Rate").alias("Default_Rate"),
).orderBy("jobIsTotal", "JOB").drop("jobIsTotal").show(truncate=False)

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
#   title;
#
# Mapping:
#   PROC SQL -> spark.sql() over a temp view; outobs=10 -> LIMIT 10;
#   mean() -> AVG(). The dollar12./percent8.2 formats are applied as a
#   separate formatted view so the raw numeric result stays available.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount (equivalent to PROC SQL)")
print("=" * 60)

homeEquityFinal.createOrReplaceTempView("home_equity_final")

topStates = spark.sql("""
    SELECT STATE,
           COUNT(*)   AS num_loans,
           AVG(LOAN)  AS avg_loan,
           AVG(VALUE) AS avg_property_value,
           AVG(BAD)   AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""")

topStates.show(truncate=False)

print("Formatted (dollar12. / percent8.2 equivalents):")
topStates.select(
    "STATE",
    "num_loans",
    format_string("$%,.0f", col("avg_loan")).alias("avg_loan"),
    format_string("$%,.0f", col("avg_property_value")).alias("avg_property_value"),
    format_string("%.2f%%", col("default_rate") * 100).alias("default_rate"),
).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Additional reporting - Loan distribution by reason and outcome
# SAS equivalent:
#   title "Loan Amount Distribution by Reason and Outcome";
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
#   title;
#
# Mapping:
#   PROC MEANS with two CLASS variables -> same helper as Step 2,
#   grouped by REASON and LOAN_OUTCOME with the requested statistics.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome (equivalent to PROC MEANS)")
print("=" * 60)
summaryByReasonOutcome = summaryByClass(
    homeEquityFinal,
    classVars=["REASON", "LOAN_OUTCOME"],
    analysisVars=["LOAN", "LTV", "DEBTINC"],
    stats=["N", "Mean", "Std", "Median"],
)
summaryByReasonOutcome.show(truncate=False)

# Clean up
spark.stop()
