"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, median, stddev, min, max, round,
    format_string, format_number, concat
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild work.home_equity_final (upstream programs 01-02)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#   data work.home_equity_clean; ... LTV, LOAN_OUTCOME, propcase(CITY)
#   data work.home_equity_imputed; ... <COL>_MISS flags
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
#   run;
#
# Note: SAS work.* datasets do not persist between PySpark scripts, so
# the upstream cleaning is reproduced inline to keep this script standalone.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfClean = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY", initcap(col("CITY"))
)

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFinal = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)
dfFinal.cache()

print("=" * 60)
print("work.home_equity_final (rebuilt inline)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")


def summarizeByClass(data, classVars, analysisVars, stats):
    """PROC MEANS with CLASS: one output row per class level and variable."""
    statExprs = {
        "N": lambda c: count(col(c)),
        "Mean": lambda c: round(mean(col(c)), 4),
        "Median": lambda c: round(median(col(c)), 4),
        "Std Dev": lambda c: round(stddev(col(c)), 4),
        "Minimum": lambda c: min(col(c)),
        "Maximum": lambda c: max(col(c)),
    }
    classData = data
    for classVar in classVars:
        # PROC MEANS drops observations with a missing CLASS value by default
        classData = classData.filter(col(classVar).isNotNull() & (col(classVar) != ""))

    result = None
    for varName in analysisVars:
        aggExprs = [count(lit(1)).alias("N Obs")] + [
            statExprs[stat](varName).alias(stat) if stat == "N"
            else statExprs[stat](varName).cast("double").alias(stat)
            for stat in stats
        ]
        varSummary = classData.groupBy(*classVars).agg(*aggExprs) \
            .withColumn("Variable", lit(varName))
        result = varSummary if result is None else result.unionByName(varSummary)

    return result.select(*classVars, "N Obs", "Variable", *stats) \
        .orderBy(*classVars, "Variable")


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from the table and percentages,
# reporting them separately as "Frequency Missing".
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
freqVars = ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]
for freqVar in freqVars:
    nonMissing = dfFinal.filter(col(freqVar).isNotNull() & (col(freqVar) != ""))
    totalNonMissing = nonMissing.count()
    freqTable = nonMissing.groupBy(freqVar).agg(count(lit(1)).alias("Frequency")) \
        .withColumn("Percent", round(col("Frequency") / lit(totalNonMissing) * 100, 2)) \
        .orderBy(freqVar)
    print(f"\nTable of {freqVar}")
    freqTable.show(truncate=False)
    print(f"Frequency Missing = {dfFinal.count() - totalNonMissing}")

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
outcomeStats = summarizeByClass(
    dfFinal,
    ["LOAN_OUTCOME"],
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    ["N", "Mean", "Median", "Std Dev", "Minimum", "Maximum"]
)
outcomeStats.show(truncate=False)

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
# Note: PROC TABULATE drops observations with a missing CLASS value, so
# rows with a null JOB or REGION are excluded (including from totals).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
tabData = dfFinal.filter(col("JOB").isNotNull() & col("REGION").isNotNull())
regions = sorted(row["REGION"] for row in tabData.select("REGION").distinct().collect())


def pctFormat(c):
    # SAS format percent8.2
    return format_string("%.2f%%", c * 100)


def defaultRateRow(data, groupCols):
    byRegion = data.groupBy(*groupCols).pivot("REGION", regions).agg(
        count("BAD").alias("N"),
        mean("BAD").alias("PctBAD")
    )
    overall = data.groupBy(*groupCols).agg(
        count("BAD").alias("Total_N"),
        mean("BAD").alias("Total_PctBAD")
    )
    return byRegion.join(overall, groupCols) if groupCols else byRegion.crossJoin(overall)


jobRows = defaultRateRow(tabData, ["JOB"]).orderBy("JOB")
totalRow = defaultRateRow(tabData, []).withColumn("JOB", lit("Total"))
defaultRates = jobRows.unionByName(totalRow)

tabColumns = [col("JOB")]
for prefix in regions + ["Total"]:
    tabColumns.append(col(f"{prefix}_N"))
    tabColumns.append(pctFormat(col(f"{prefix}_PctBAD")).alias(f"{prefix}_PctBAD"))
defaultRates.select(*tabColumns).show(truncate=False)

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
    col("STATE"),
    col("num_loans"),
    concat(lit("$"), format_number(col("avg_loan"), 0)).alias("avg_loan"),
    concat(lit("$"), format_number(col("avg_property_value"), 0)).alias("avg_property_value"),
    pctFormat(col("default_rate")).alias("default_rate")
).show(truncate=False)

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
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)
reasonOutcomeStats = summarizeByClass(
    dfFinal,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "Std Dev", "Median"]
)
reasonOutcomeStats.show(50, truncate=False)

# Clean up
spark.stop()
