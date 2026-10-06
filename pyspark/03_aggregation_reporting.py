"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as spark_min,
    max as spark_max, percentile_approx, format_string
)

# Resolve the dataset path from the project root, not the CWD
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()


def printTitle(title):
    # SAS: title "..."; -> printed banner
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ------------------------------------------------------------------
# Upstream steps (replicated inline so this script is self-contained)
# SAS equivalent: work.home_equity_final produced by
#   sas/01_data_loading.sas and sas/02_data_cleaning.sas
# ------------------------------------------------------------------
def loadData():
    # SAS: proc import datafile="/data/home_equity.csv" dbms=csv ... -> spark.read.csv
    return spark.read.csv(dataPath, header=True, inferSchema=True)


def buildFinalDataset(df):
    # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
    #      else LTV = .;  -> when().otherwise(null)
    df = df.withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    )
    # SAS: if BAD = 0 then LOAN_OUTCOME = 'Paid'; else if BAD = 1 then 'Default';
    df = df.withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default"))
    )
    # SAS: CITY = propcase(CITY); -> initcap
    df = df.withColumn("CITY", initcap(col("CITY")))

    # SAS: array num_vars{8} ...; do i = 1 to 8; if num_vars{i} = . then flag = 1;
    #      -> Python loop over columns with withColumn
    for varName in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
        df = df.withColumn(
            f"{varName}_MISS", when(col(varName).isNull(), lit(1)).otherwise(lit(0))
        )

    # SAS: if LOAN ne . and VALUE ne . and BAD ne .; -> filter isNotNull
    dfFiltered = df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )

    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
    dfFinal = dfFiltered.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )
    return dfFiltered, dfFinal


dfRaw = loadData()
dfFiltered, df = buildFinalDataset(dfRaw)
df.cache()

printTitle("Row Counts (raw -> filtered -> final)")
print(f"  work.home_equity          : {dfRaw.count()}")
print(f"  work.home_equity_filtered : {dfFiltered.count()}")
print(f"  work.home_equity_final    : {df.count()}")

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from the table and reports
# them as "Frequency Missing", so nulls are filtered before groupBy.
# ------------------------------------------------------------------
printTitle("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
for varName in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    nonMissing = df.filter(col(varName).isNotNull())
    total = nonMissing.count()
    # SAS: PROC FREQ tables X / nocum -> groupBy().count() + percent
    freq = nonMissing.groupBy(varName).count() \
        .withColumnRenamed("count", "Frequency") \
        .withColumn("Percent", format_string("%.2f", col("Frequency") * 100 / lit(total))) \
        .orderBy(varName)
    print(f"\n{varName}")
    freq.show(truncate=False)
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


def summarize(data, classVars, analysisVars, stats):
    # SAS: PROC MEANS with CLASS -> groupBy().agg(); rows with a missing
    # CLASS value are excluded, matching the SAS default.
    statFns = {
        "N": lambda c: count(c),
        "Mean": lambda c: mean(c),
        # percentile_approx instead of median() for compatibility with Spark < 3.4
        "Median": lambda c: percentile_approx(c, 0.5),
        "StdDev": lambda c: stddev(c),
        "Min": lambda c: spark_min(c),
        "Max": lambda c: spark_max(c),
    }
    data = data.dropna(subset=classVars)
    result = None
    for varName in analysisVars:
        aggExprs = [statFns[s](col(varName)).alias(s) for s in stats]
        part = data.groupBy(*classVars).agg(*aggExprs) \
            .withColumn("Variable", lit(varName))
        result = part if result is None else result.unionByName(part)
    return result.select(*classVars, "Variable", *stats).orderBy(*classVars, "Variable")


summarize(
    df, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    ["N", "Mean", "Median", "StdDev", "Min", "Max"]
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
#
# Note: PROC TABULATE drops rows with missing CLASS values by default.
# ------------------------------------------------------------------
printTitle("Default Rates by Job Category and Region")
tabData = df.dropna(subset=["JOB", "REGION"])
regions = sorted(r["REGION"] for r in tabData.select("REGION").distinct().collect())

# SAS: JOB all='Total' (row dimension) -> union of per-JOB rows and a Total row
tabRows = tabData.unionByName(tabData.withColumn("JOB", lit("Total")))

# SAS: REGION * BAD * (n mean) -> groupBy().pivot().agg()
byRegion = tabRows.groupBy("JOB").pivot("REGION", regions).agg(
    count("BAD").alias("N"), mean("BAD").alias("DefaultRate")
)
# SAS: all='Total' * BAD * (n mean) (column dimension)
allRegions = tabRows.groupBy("JOB").agg(
    count("BAD").alias("Total_N"), mean("BAD").alias("Total_DefaultRate")
)

tabulate = byRegion.join(allRegions, on="JOB")
# SAS: f=percent8.2 -> format_string
for c in [f"{r}_DefaultRate" for r in regions] + ["Total_DefaultRate"]:
    tabulate = tabulate.withColumn(c, format_string("%.2f%%", col(c) * 100))
tabulate.orderBy(when(col("JOB") == "Total", 1).otherwise(0), "JOB") \
    .show(truncate=False)

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
# SAS: work.home_equity_final table -> temp view
df.createOrReplaceTempView("home_equity_final")
# SAS: outobs=10 -> LIMIT 10; format=dollar12./percent8.2 -> format_string
topStates = spark.sql("""
    SELECT STATE,
           num_loans,
           format_string('$%,.0f', avg_loan)             AS avg_loan,
           format_string('$%,.0f', avg_property_value)   AS avg_property_value,
           format_string('%.2f%%', default_rate * 100)   AS default_rate
    FROM (
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
    ) t
    ORDER BY t.avg_loan DESC
""")
topStates.show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
printTitle("Loan Amount Distribution by Reason and Outcome")
summarize(
    df, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "StdDev", "Median"]
).show(truncate=False)

# Clean up
df.unpersist()
spark.stop()
