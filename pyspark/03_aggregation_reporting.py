"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# Resolve the dataset relative to the project root so the script runs from any CWD
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")


def printTitle(title):
    # SAS equivalent: title "..."; ... title;
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ------------------------------------------------------------------
# Upstream: rebuild work.home_equity_final inline (sas/01 + sas/02)
# so this script runs independently of the other stages.
# ------------------------------------------------------------------
def loadHomeEquity():
    # SAS: PROC IMPORT dbms=csv -> spark.read.csv (empty fields become null)
    return spark.read.csv(dataPath, header=True, inferSchema=True)


def buildHomeEquityFinal(df):
    # SAS: DATA step IF/THEN/ELSE -> when().otherwise(); missing "." -> null
    dfClean = df \
        .withColumn(
            "LTV",
            F.when(
                F.col("VALUE").isNotNull() &
                F.col("MORTDUE").isNotNull() &
                (F.col("VALUE") > 0),
                F.col("MORTDUE") / F.col("VALUE")
            )
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            F.when(F.col("BAD") == 0, F.lit("Paid"))
            .when(F.col("BAD") == 1, F.lit("Default"))
        ) \
        .withColumn("CITY", F.initcap(F.col("CITY")))  # SAS: propcase() -> initcap()

    # SAS: subsetting IF (LOAN ne . and VALUE ne . and BAD ne .) -> filter(isNotNull)
    dfFiltered = dfClean.filter(
        F.col("LOAN").isNotNull() &
        F.col("VALUE").isNotNull() &
        F.col("BAD").isNotNull()
    )

    # SAS: subsetting IFs for outliers -> filter (null LTV fails "LTV > 0", as in SAS)
    return dfFiltered.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) &
        (F.col("LOAN") > 0) &
        (F.col("VALUE") > 0)
    )


# ------------------------------------------------------------------
# Reusable PROC MEANS equivalent
# ------------------------------------------------------------------
STAT_EXPRS = {
    "N": lambda c: F.count(c),
    "Mean": lambda c: F.mean(c),
    # SAS median (QNTLDEF=5) averages the two middle values for even N;
    # percentile() interpolates the same way and is exact.
    "Median": lambda c: F.expr(f"percentile(`{c}`, 0.5)"),
    "Std Dev": lambda c: F.stddev_samp(c),  # SAS VARDEF=DF (n-1)
    "Minimum": lambda c: F.min(c),
    "Maximum": lambda c: F.max(c),
}


def procMeans(df, classVars, analysisVars, stats):
    # SAS: PROC MEANS CLASS ... VAR ... -> groupBy(class).agg() per analysis variable.
    # CLASS drops observations with a missing class value (no MISSING option).
    dfClass = df.dropna(subset=classVars)
    perVar = [
        dfClass.groupBy(*classVars)
        .agg(*[F.round(STAT_EXPRS[s](v), 4).alias(s) for s in stats])
        .withColumn("Variable", F.lit(v))
        .select(*classVars, "Variable", *stats)
        for v in analysisVars
    ]
    varOrder = F.array_position(F.array(*[F.lit(v) for v in analysisVars]), F.col("Variable"))
    return reduce(DataFrame.unionByName, perVar).orderBy(*classVars, varOrder)


def formatPercent(c):
    # SAS: format percent8.2 -> format_string
    return F.format_string("%.2f%%", F.col(c) * 100)


df = buildHomeEquityFinal(loadHomeEquity()).cache()

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
printTitle("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
for freqVar in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    # SAS: PROC FREQ -> groupBy().count(); percent excludes missing, reported separately
    nonMissing = df.filter(F.col(freqVar).isNotNull())
    total = nonMissing.count()
    freqTable = nonMissing.groupBy(freqVar).count() \
        .withColumnRenamed("count", "Frequency") \
        .withColumn("Percent", F.round(F.col("Frequency") / F.lit(total) * 100, 2)) \
        .orderBy(freqVar)
    freqTable.show(truncate=False)
    print(f"Frequency Missing = {df.count() - total}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
printTitle("Summary Statistics by Loan Outcome")
procMeans(
    df,
    classVars=["LOAN_OUTCOME"],
    analysisVars=["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    stats=["N", "Mean", "Median", "Std Dev", "Minimum", "Maximum"],
).show(truncate=False)

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
# ------------------------------------------------------------------
printTitle("Default Rates by Job Category and Region")
# SAS: TABULATE CLASS drops observations with a missing JOB or REGION
dfTab = df.dropna(subset=["JOB", "REGION"])
regions = [r["REGION"] for r in dfTab.select("REGION").distinct().orderBy("REGION").collect()]


def tabulateRow(dfGroup, *groupCols):
    # SAS: REGION * BAD * (n mean) -> groupBy().pivot(REGION).agg(count, mean)
    byRegion = dfGroup.groupBy(*groupCols).pivot("REGION", regions).agg(
        F.count("BAD").alias("N"),
        F.mean("BAD").alias("Mean"),
    )
    # SAS: all='Total' * BAD * (n mean) -> ungrouped agg over the same rows
    allRegions = dfGroup.groupBy(*groupCols).agg(
        F.count("BAD").alias("Total_N"),
        F.mean("BAD").alias("Total_Mean"),
    )
    return byRegion.join(allRegions, list(groupCols)) if groupCols \
        else byRegion.crossJoin(allRegions)


# SAS: row dimension "JOB all='Total'" -> per-JOB rows unioned with a Total row
jobRows = tabulateRow(dfTab, "JOB").orderBy("JOB")
totalRow = tabulateRow(dfTab).withColumn("JOB", F.lit("Total"))
tabulate = jobRows.unionByName(totalRow)

meanCols = [f"{r}_Mean" for r in regions] + ["Total_Mean"]
tabulate.select(
    "JOB",
    *[formatPercent(c).alias(c) if c in meanCols else F.col(c)
      for c in tabulate.columns if c != "JOB"]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using PROC SQL
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
printTitle("Top 10 States by Average Loan Amount")
# SAS: work.home_equity_final in PROC SQL -> temp view + spark.sql(); OUTOBS=10 -> LIMIT 10
df.createOrReplaceTempView("home_equity_final")
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
    # SAS: format dollar12. -> format_number with a "$" prefix
    F.concat(F.lit("$"), F.format_number("avg_loan", 0)).alias("avg_loan"),
    F.concat(F.lit("$"), F.format_number("avg_property_value", 0)).alias("avg_property_value"),
    formatPercent("default_rate").alias("default_rate"),
).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
printTitle("Loan Amount Distribution by Reason and Outcome")
procMeans(
    df,
    classVars=["REASON", "LOAN_OUTCOME"],
    analysisVars=["LOAN", "LTV", "DEBTINC"],
    stats=["N", "Mean", "Std Dev", "Median"],
).show(truncate=False)

# Clean up
spark.stop()
