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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, median, stddev, min, max, round
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild work.home_equity_final (output of 02_data_cleaning)
# SAS equivalent:
#   data work.home_equity_clean;  /* LTV, LOAN_OUTCOME, propcase(CITY) */
#   data work.home_equity_imputed; /* <COL>_MISS flags via ARRAY */
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
#
# Note: SAS chains off WORK datasets from earlier programs. This script
# is self-contained, so the 02 cleaning logic is reproduced inline.
# ------------------------------------------------------------------
dataPath = "data/home_equity.csv"
df = spark.read.csv(dataPath, header=True, inferSchema=True)
sourceCount = df.count()

dfClean = df.withColumn(
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
    .otherwise(lit(None))
).withColumn("CITY", initcap(col("CITY")))

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)
filteredCount = dfFiltered.count()

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
).cache()
finalCount = dfFinal.count()

print("=" * 60)
print("Rebuilt work.home_equity_final (02_data_cleaning logic)")
print("=" * 60)
print(f"Source rows: {sourceCount}")
print(f"Rows after LOAN/VALUE/BAD non-null filter: {filteredCount}")
print(f"Number of rows: {finalCount}")


def procMeans(data, classCols, varCols, stats):
    """PROC MEANS with CLASS: one output row per class level and variable.
    Rows with a missing CLASS value are excluded, as in SAS."""
    classData = data.filter(
        reduce(lambda a, b: a & b, [col(c).isNotNull() for c in classCols])
    )
    statFuncs = {
        "N": lambda c: count(c),
        "Mean": lambda c: round(mean(c), 4),
        "Median": lambda c: round(median(c), 4),
        "Std": lambda c: round(stddev(c), 4),
        "Min": lambda c: min(c),
        "Max": lambda c: max(c),
    }
    perVar = [
        classData.groupBy(*classCols).agg(
            count(lit(1)).alias("N_Obs"),
            *[
                (statFuncs[s](col(varName)) if s == "N"
                 else statFuncs[s](col(varName)).cast("double")).alias(s)
                for s in stats
            ]
        ).withColumn("Variable", lit(varName))
        .withColumn("_varOrder", lit(i))
        for i, varName in enumerate(varCols)
    ]
    return reduce(lambda a, b: a.unionByName(b), perVar) \
        .orderBy(*classCols, "_varOrder") \
        .select(*classCols, "N_Obs", "Variable", *stats)


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from percentages and reports
# them separately as "Frequency Missing".
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
freqVars = ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]
for varName in freqVars:
    nonMissing = dfFinal.filter(col(varName).isNotNull())
    nonMissingCount = nonMissing.count()
    print(f"\nPROC FREQ: {varName}")
    nonMissing.groupBy(varName).count() \
        .withColumnRenamed("count", "Frequency") \
        .withColumn("Percent", round(col("Frequency") / nonMissingCount * 100, 2)) \
        .orderBy(varName) \
        .show(truncate=False)
    print(f"Frequency Missing = {finalCount - nonMissingCount}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)
procMeans(
    dfFinal,
    ["LOAN_OUTCOME"],
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    ["N", "Mean", "Median", "Std", "Min", "Max"]
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
# Note: PROC TABULATE drops rows with a missing CLASS value; mean(BAD)
# formatted as percent8.2 is shown as a percentage rounded to 2 dp.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
tabData = dfFinal.filter(col("JOB").isNotNull() & col("REGION").isNotNull())
regions = sorted(r["REGION"] for r in tabData.select("REGION").distinct().collect())
tabStats = [
    count("BAD").alias("N"),
    round(mean("BAD") * 100, 2).alias("PCT_DEFAULT"),
]

byJobRegion = tabData.groupBy("JOB").pivot("REGION", regions).agg(*tabStats)
byJobTotal = tabData.groupBy("JOB").agg(
    count("BAD").alias("Total_N"),
    round(mean("BAD") * 100, 2).alias("Total_PCT_DEFAULT")
)
jobRows = byJobRegion.join(byJobTotal, on="JOB").orderBy("JOB")

totalRow = tabData.groupBy().pivot("REGION", regions).agg(*tabStats) \
    .withColumn("JOB", lit("Total")) \
    .crossJoin(tabData.agg(
        count("BAD").alias("Total_N"),
        round(mean("BAD") * 100, 2).alias("Total_PCT_DEFAULT")
    ))

jobRows.unionByName(totalRow).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using PROC SQL
# SAS equivalent:
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
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)
dfFinal.createOrReplaceTempView("home_equity_final")
topStates = spark.sql("""
    SELECT STATE,
           COUNT(*) AS num_loans,
           ROUND(AVG(LOAN), 2) AS avg_loan,
           ROUND(AVG(VALUE), 2) AS avg_property_value,
           ROUND(AVG(BAD) * 100, 2) AS default_rate
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
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
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)
procMeans(
    dfFinal,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "Std", "Median"]
).show(50, truncate=False)

# Clean up
spark.stop()
