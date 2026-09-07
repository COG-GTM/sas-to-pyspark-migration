"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    median, round as spark_round, lit, when, format_string, concat
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild work.home_equity_final
# The SAS program reports on work.home_equity_final, produced by
# sas/02_data_cleaning.sas. Re-derive the prerequisite columns and
# filters here so this script is self-contained.
# SAS equivalent (02_data_cleaning.sas, Steps 1, 3 and 5):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   else LTV = .;
#   if BAD = 0 then LOAN_OUTCOME = 'Paid';
#   else if BAD = 1 then LOAN_OUTCOME = 'Default';
#   else LOAN_OUTCOME = '';
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LTV > 0 and LTV < 5;
#   if LOAN > 0;
#   if VALUE > 0;
#
# Note: SAS treats a missing LTV as less than 0, so `LTV > 0` drops
# rows with missing LTV. Spark's null comparison also evaluates to
# false in a filter, so the row counts match.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfFinal = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    ) \
    .filter((col("LTV") > 0) & (col("LTV") < 5)) \
    .filter(col("LOAN") > 0) \
    .filter(col("VALUE") > 0)

dfFinal.cache()
totalCount = dfFinal.count()
print("=" * 60)
print(f"work.home_equity_final rebuilt: {totalCount} rows")
print("=" * 60)

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from the table and the
# percent denominator (they are reported as "Frequency Missing").
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("(equivalent to PROC FREQ / nocum)")
print("=" * 60)

for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    nonMissing = dfFinal.filter(col(catCol).isNotNull())
    nonMissingCount = nonMissing.count()
    print(f"\n--- {catCol} ---")
    nonMissing.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(nonMissingCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(catCol) \
        .show(truncate=False)
    print(f"Frequency Missing = {totalCount - nonMissingCount}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#
# Note: PROC MEANS reports one block per analysis variable; N counts
# non-missing values of that variable within the class level.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome (equivalent to PROC MEANS)")
print("=" * 60)


def summaryByClass(data, classCols, varCols, stats):
    """Emulate PROC MEANS with CLASS: one output table per VAR variable."""
    statFuncs = {
        "n": lambda c: count(c).alias("N"),
        "mean": lambda c: spark_round(mean(c), 2).alias("Mean"),
        "median": lambda c: spark_round(median(c), 2).alias("Median"),
        "std": lambda c: spark_round(stddev(c), 2).alias("Std"),
        "min": lambda c: spark_round(spark_min(c), 2).alias("Min"),
        "max": lambda c: spark_round(spark_max(c), 2).alias("Max"),
    }
    for varCol in varCols:
        print(f"\n--- Analysis Variable: {varCol} ---")
        data.groupBy(*classCols) \
            .agg(*[statFuncs[s](varCol) for s in stats]) \
            .orderBy(*classCols) \
            .show(truncate=False)


summaryByClass(
    dfFinal,
    classCols=["LOAN_OUTCOME"],
    varCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    stats=["n", "mean", "median", "std", "min", "max"],
)

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
#
# Rows: JOB plus a 'Total' row (all=). Columns: each REGION plus a
# 'Total' column, each showing N and mean(BAD) formatted as percent.
# PROC TABULATE drops rows with a missing CLASS variable.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region (equivalent to PROC TABULATE)")
print("=" * 60)

dfTab = dfFinal.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Add 'Total' as an extra level of both class variables (all='Total')
dfTabRows = dfTab.union(dfTab.withColumn("JOB", lit("Total")))
dfTabCells = dfTabRows.union(dfTabRows.withColumn("REGION", lit("Total")))

# Combine N and percent-formatted mean into one cell per REGION column
cellExpr = concat(
    lit("N="), count("BAD"),
    lit(" Mean="), format_string("%.2f%%", mean("BAD") * 100)
)

regions = sorted(
    row["REGION"] for row in dfTab.select("REGION").distinct().collect()
) + ["Total"]

crossTab = dfTabCells.groupBy("JOB") \
    .pivot("REGION", regions) \
    .agg(cellExpr) \
    .withColumn("_sort", when(col("JOB") == "Total", lit(1)).otherwise(lit(0))) \
    .orderBy("_sort", "JOB") \
    .drop("_sort")
crossTab.show(truncate=False)

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
#
# Note: outobs=10 maps to LIMIT 10. SAS formats are display-only,
# so the numeric columns are kept as numbers here.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount (equivalent to PROC SQL)")
print("=" * 60)

dfFinal.createOrReplaceTempView("home_equity_final")

topStates = spark.sql("""
    SELECT STATE,
           COUNT(*)                AS num_loans,
           ROUND(AVG(LOAN), 2)     AS avg_loan,
           ROUND(AVG(VALUE), 2)    AS avg_property_value,
           ROUND(AVG(BAD), 4)      AS default_rate
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
#   title "Loan Amount Distribution by Reason and Outcome";
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome (equivalent to PROC MEANS)")
print("=" * 60)

summaryByClass(
    dfFinal.filter(col("REASON").isNotNull()),
    classCols=["REASON", "LOAN_OUTCOME"],
    varCols=["LOAN", "LTV", "DEBTINC"],
    stats=["n", "mean", "std", "median"],
)

# Clean up
spark.stop()
