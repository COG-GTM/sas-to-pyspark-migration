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
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, initcap

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load data and reproduce work.home_equity_final
# SAS equivalent (sas/02_data_cleaning.sas):
#   data work.home_equity_clean;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)
sourceCount = df.count()

dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)
filteredCount = dfFiltered.count()

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
).cache()
finalCount = dfFinal.count()

print("=" * 60)
print("Input: work.home_equity_final (cleaning from 02_data_cleaning)")
print("=" * 60)
print(f"Source rows: {sourceCount}")
print(f"Rows after LOAN/VALUE/BAD null filter: {filteredCount}")
print(f"Number of rows: {finalCount}")


def isPresent(colName):
    """SAS treats null numerics and blank character values as missing."""
    return col(colName).isNotNull() & (F.trim(col(colName).cast("string")) != "")


def fmtPercent(c):
    """SAS percent8.2 format."""
    return F.concat(F.format_number(c * 100, 2), lit("%"))


def fmtDollar(c):
    """SAS dollar12. format."""
    return F.concat(lit("$"), F.format_number(F.round(c, 0), 0))


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
#
# Note: like PROC FREQ, missing values are excluded from the table and
# percentages, and reported as "Frequency Missing".
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
for freqVar in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    present = dfFinal.filter(isPresent(freqVar))
    nonMissing = present.count()
    freqTable = present.groupBy(freqVar).count() \
        .withColumnRenamed("count", "Frequency") \
        .withColumn("Percent", F.round(col("Frequency") * 100.0 / nonMissing, 2)) \
        .orderBy(freqVar)
    print(f"\nTable of {freqVar}")
    freqTable.show(truncate=False)
    print(f"Frequency Missing = {finalCount - nonMissing}")

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


def classMeans(data, classVars, analysisVars, stats):
    """PROC MEANS with CLASS: one row per class level and analysis variable."""
    statExprs = {
        "N": lambda v: F.count(v),
        "Mean": lambda v: F.round(F.mean(v), 4),
        "Median": lambda v: F.round(F.median(v), 4),
        "Std Dev": lambda v: F.round(F.stddev(v), 4),
        "Minimum": lambda v: F.min(v),
        "Maximum": lambda v: F.max(v),
    }
    classed = data
    for classVar in classVars:
        classed = classed.filter(isPresent(classVar))
    nObs = classed.groupBy(*classVars).agg(F.count(lit(1)).alias("N Obs"))
    result = None
    for v in analysisVars:
        part = classed.groupBy(*classVars).agg(
            *[statExprs[s](v).alias(s) for s in stats]
        ).withColumn("Variable", lit(v))
        result = part if result is None else result.unionByName(part)
    varOrder = F.create_map(*[x for i, v in enumerate(analysisVars) for x in (lit(v), lit(i))])
    return result.join(nObs, classVars) \
        .orderBy(*classVars, varOrder[col("Variable")]) \
        .select(*classVars, "N Obs", "Variable", *stats)


print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)
classMeans(
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
# Note: PROC TABULATE drops observations with a missing CLASS value.
# groupBy().pivot() builds the REGION columns; the 'all' row and column
# are computed as extra aggregations.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
dfTab = dfFinal.filter(isPresent("JOB") & isPresent("REGION"))
regions = [r["REGION"] for r in dfTab.select("REGION").distinct().orderBy("REGION").collect()]

cellAggs = [F.count("BAD").alias("N"), F.mean("BAD").alias("Mean")]
byJob = dfTab.groupBy("JOB").pivot("REGION", regions).agg(*cellAggs)
byJobTotal = dfTab.groupBy("JOB").agg(*cellAggs) \
    .withColumnRenamed("N", "Total_N").withColumnRenamed("Mean", "Total_Mean")
allRow = dfTab.groupBy(lit("Total").alias("JOB")).pivot("REGION", regions).agg(*cellAggs)
allRowTotal = dfTab.agg(*cellAggs).withColumn("JOB", lit("Total")) \
    .withColumnRenamed("N", "Total_N").withColumnRenamed("Mean", "Total_Mean")

tabulate = byJob.join(byJobTotal, "JOB").orderBy("JOB") \
    .unionByName(allRow.join(allRowTotal, "JOB"))

tabCols = [col("JOB")]
for region in regions + ["Total"]:
    tabCols.append(F.coalesce(col(f"{region}_N"), lit(0)).alias(f"{region} BAD N"))
    tabCols.append(
        F.coalesce(fmtPercent(col(f"{region}_Mean")), lit(".")).alias(f"{region} BAD Mean")
    )
tabulate.select(*tabCols).show(truncate=False)

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
    "STATE",
    "num_loans",
    fmtDollar(col("avg_loan")).alias("avg_loan"),
    fmtDollar(col("avg_property_value")).alias("avg_property_value"),
    fmtPercent(col("default_rate")).alias("default_rate")
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
classMeans(
    dfFinal,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "Std Dev", "Median"]
).show(truncate=False)

# Clean up
spark.stop()
