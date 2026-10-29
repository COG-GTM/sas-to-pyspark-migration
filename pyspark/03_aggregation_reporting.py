"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas

The SAS program reads work.home_equity_final, produced by
sas/02_data_cleaning.sas. This script is self-contained: it loads the raw CSV
and reproduces the 02 cleaning steps inline before the reporting logic.
"""

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, median, stddev, min, max,
    round, format_string
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load data and reproduce work.home_equity_final
# SAS equivalent (sas/01_data_loading.sas + sas/02_data_cleaning.sas):
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: the 02 missing-value flag columns (*_MISS) are not used by any
# report in this program, so they are not recreated here.
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
print("Input dataset: work.home_equity_final (rebuilt from 02 cleaning)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")


def classSummary(data, classVars, analysisVars, stats):
    """PROC MEANS with a CLASS statement: one row per class level and variable.

    Like SAS, rows with a missing value in any CLASS variable are excluded.
    """
    statExprs = {
        "N": lambda c: count(c),
        "Mean": lambda c: round(mean(c), 4),
        "Median": lambda c: round(median(c), 4),
        "Std Dev": lambda c: round(stddev(c), 4),
        "Minimum": lambda c: round(min(c), 4),
        "Maximum": lambda c: round(max(c), 4),
    }
    classData = data.filter(
        reduce(lambda a, b: a & b, [col(c).isNotNull() for c in classVars])
    )
    perVar = [
        classData.groupBy(*classVars).agg(
            count(lit(1)).alias("N Obs"),
            *[statExprs[s](var).alias(s) for s in stats]
        ).withColumn("Variable", lit(var))
        for var in analysisVars
    ]
    return reduce(lambda a, b: a.unionByName(b), perVar) \
        .select(*classVars, "N Obs", "Variable", *stats) \
        .orderBy(*classVars, "Variable")


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: like PROC FREQ, percentages are based on non-missing values and the
# missing count is reported separately as "Frequency Missing".
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
for freqVar in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    nonMissing = dfFinal.filter(col(freqVar).isNotNull())
    nonMissingTotal = nonMissing.count()
    freqTable = nonMissing.groupBy(freqVar).agg(count(lit(1)).alias("Frequency")) \
        .withColumn("Percent", round(col("Frequency") * 100 / nonMissingTotal, 2)) \
        .orderBy(freqVar)
    print(f"\nTable of {freqVar}")
    freqTable.show(truncate=False)
    print(f"Frequency Missing = {dfFinal.count() - nonMissingTotal}")

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
classSummary(
    dfFinal,
    ["LOAN_OUTCOME"],
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    ["N", "Mean", "Median", "Std Dev", "Minimum", "Maximum"]
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
# Note: PROC TABULATE drops rows with a missing CLASS value, so rows with a
# missing JOB or REGION are excluded. The 'all' row and column are built
# with a total row (groupBy without JOB) and a total column (join on JOB).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
tabData = dfFinal.filter(col("JOB").isNotNull() & col("REGION").isNotNull()) \
    .withColumn("JOB", col("JOB").cast("string"))
regions = sorted(r["REGION"] for r in tabData.select("REGION").distinct().collect())


def tabulateRows(data, rowKeys):
    """Pivot REGION into N / default-rate columns, plus the 'Total' column."""
    byRegion = data.groupBy(*rowKeys).pivot("REGION", regions).agg(
        count("BAD").alias("N"),
        mean("BAD").alias("Mean")
    )
    totals = data.groupBy(*rowKeys).agg(
        count("BAD").alias("Total_N"),
        mean("BAD").alias("Total_Mean")
    )
    joined = byRegion.join(totals, rowKeys) if rowKeys else byRegion.crossJoin(totals)
    return joined


tabBody = tabulateRows(tabData, ["JOB"]).orderBy("JOB")
tabTotal = tabulateRows(tabData, []).withColumn("JOB", lit("Total"))
tabulateTable = tabBody.unionByName(tabTotal)

pctCols = [f"{r}_Mean" for r in regions] + ["Total_Mean"]
for pctCol in pctCols:
    tabulateTable = tabulateTable.withColumn(
        pctCol, format_string("%.2f%%", col(pctCol) * 100)
    )
tabulateTable = tabulateTable.select(
    "JOB",
    *[c for r in regions for c in (f"{r}_N", f"{r}_Mean")],
    "Total_N", "Total_Mean"
)
for r in regions + ["Total"]:
    tabulateTable = tabulateTable \
        .withColumnRenamed(f"{r}_N", f"{r} BAD N") \
        .withColumnRenamed(f"{r}_Mean", f"{r} BAD Mean")
tabulateTable.show(truncate=False)

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
# Note: outobs=10 -> LIMIT 10; dollar12. / percent8.2 formats -> format_string.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)
dfFinal.createOrReplaceTempView("home_equity_final")
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
topStates.select(
    "STATE",
    "num_loans",
    format_string("$%,.0f", col("avg_loan")).alias("avg_loan"),
    format_string("$%,.0f", col("avg_property_value")).alias("avg_property_value"),
    format_string("%.2f%%", col("default_rate") * 100).alias("default_rate")
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
classSummary(
    dfFinal,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "Std Dev", "Median"]
).show(50, truncate=False)

# Clean up
spark.stop()
