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
    col, when, lit, initcap, count, mean, median, stddev, min, max,
    round, coalesce, first, format_number, concat
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild work.home_equity_final (upstream dependency)
# The SAS program reads work.home_equity_final, which is created by
# sas/01_data_loading.sas and sas/02_data_cleaning.sas. This script is
# self-contained, so the required upstream steps are reproduced here.
#
# SAS equivalent (sas/01_data_loading.sas):
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
# SAS equivalent (sas/02_data_cleaning.sas, Steps 1, 3 and 5):
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: the *_MISS flag columns from 02 Step 2 are not used by this
# report, so they are not recreated here.
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
            col("VALUE").isNotNull() &
            col("MORTDUE").isNotNull() &
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
    .filter((col("LTV") > 0) & (col("LTV") < 5)) \
    .filter(col("LOAN") > 0) \
    .filter(col("VALUE") > 0) \
    .cache()

print("=" * 60)
print("work.home_equity_final rebuilt from data/home_equity.csv")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")


def classMeans(data, classCols, varCols, stats):
    """
    Equivalent of PROC MEANS with a CLASS statement.
    Like SAS, observations with a missing CLASS value are excluded and
    one row is produced per combination of the CLASS variables.
    """
    statFuncs = {
        "N": count,
        "Mean": mean,
        "Median": median,
        "Std": stddev,
        "Min": min,
        "Max": max,
    }
    classFilter = None
    for c in classCols:
        cond = col(c).isNotNull()
        classFilter = cond if classFilter is None else classFilter & cond

    aggExprs = [count(lit(1)).alias("N_Obs")]
    for v in varCols:
        for s in stats:
            expr = statFuncs[s](v)
            if s != "N":
                expr = round(expr, 4)
            aggExprs.append(expr.alias(f"{v}_{s}"))

    return data.filter(classFilter) \
        .groupBy(*classCols) \
        .agg(*aggExprs) \
        .orderBy(*classCols)


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# PROC FREQ excludes missing values from the table and the percentages
# and reports them as "Frequency Missing"; NOCUM drops the cumulative
# columns, so only Frequency and Percent are shown.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)

for freqVar in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    nonMissing = dfFinal.filter(col(freqVar).isNotNull())
    totalNonMissing = nonMissing.count()
    freqTable = nonMissing.groupBy(freqVar) \
        .agg(count(lit(1)).alias("Frequency")) \
        .withColumn(
            "Percent",
            round(col("Frequency") / lit(totalNonMissing) * 100, 2)
        ) \
        .orderBy(freqVar)

    print(f"\nPROC FREQ: {freqVar}")
    freqTable.show(truncate=False)
    missingCount = dfFinal.filter(col(freqVar).isNull()).count()
    print(f"Frequency Missing = {missingCount}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)

outcomeVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]
outcomeStatNames = ["N", "Mean", "Median", "Std", "Min", "Max"]
outcomeStats = classMeans(
    dfFinal, ["LOAN_OUTCOME"], outcomeVars, outcomeStatNames
)
# One block per analysis variable, mirroring the PROC MEANS layout
for v in outcomeVars:
    outcomeStats.select(
        "LOAN_OUTCOME", "N_Obs",
        *[f"{v}_{s}" for s in outcomeStatNames]
    ).show(truncate=False)

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
# PROC TABULATE drops observations with a missing CLASS value. cube()
# produces every JOB x REGION cell plus the ALL='Total' margins, and
# pivot() lays REGION out across the columns. mean(BAD) with
# percent8.2 is shown as a percentage rounded to 2 decimals.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)

tabData = dfFinal.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

tabCells = tabData.cube("JOB", "REGION").agg(
    count("BAD").alias("N"),
    round(mean("BAD") * 100, 2).alias("DEFAULT_PCT")
).select(
    coalesce(col("JOB"), lit("Total")).alias("JOB"),
    coalesce(col("REGION"), lit("Total")).alias("REGION"),
    "N",
    "DEFAULT_PCT"
)

regionOrder = sorted(
    row["REGION"] for row in tabData.select("REGION").distinct().collect()
) + ["Total"]

defaultTab = tabCells.groupBy("JOB") \
    .pivot("REGION", regionOrder) \
    .agg(first("N").alias("N"), first("DEFAULT_PCT").alias("DEFAULT_PCT")) \
    .withColumn("_ORDER", when(col("JOB") == "Total", lit(1)).otherwise(lit(0))) \
    .orderBy("_ORDER", "JOB") \
    .drop("_ORDER")

defaultTab.show(truncate=False)

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
# OUTOBS=10 maps to LIMIT 10. The dollar12. / percent8.2 display
# formats are applied after the query so ordering stays numeric.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)

dfFinal.createOrReplaceTempView("home_equity_final")

topStates = spark.sql("""
    SELECT STATE,
           COUNT(*) AS num_loans,
           AVG(LOAN) AS avg_loan,
           AVG(VALUE) AS avg_property_value,
           AVG(BAD) AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""")

topStates.select(
    "STATE",
    "num_loans",
    concat(lit("$"), format_number("avg_loan", 0)).alias("avg_loan"),
    concat(lit("$"), format_number("avg_property_value", 0))
    .alias("avg_property_value"),
    concat(format_number(col("default_rate") * 100, 2), lit("%"))
    .alias("default_rate")
).show(truncate=False)

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
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)

reasonVars = ["LOAN", "LTV", "DEBTINC"]
reasonStatNames = ["N", "Mean", "Std", "Median"]
reasonStats = classMeans(
    dfFinal, ["REASON", "LOAN_OUTCOME"], reasonVars, reasonStatNames
)
for v in reasonVars:
    reasonStats.select(
        "REASON", "LOAN_OUTCOME", "N_Obs",
        *[f"{v}_{s}" for s in reasonStatNames]
    ).show(truncate=False)

# Clean up
spark.stop()
