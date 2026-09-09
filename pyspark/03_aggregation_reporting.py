"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Frequency tables, summary statistics, cross-tabulations and ad hoc queries
Equivalent SAS Program: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, expr,
    min as sparkMin, max as sparkMax, round as sparkRound
)

spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Rebuild work.home_equity_final (see 02_data_cleaning.py).
# SAS chains programs through the WORK library; each PySpark script is
# self-contained and re-derives the cleaned DataFrame.
# ------------------------------------------------------------------
dfFinal = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True) \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")).otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY"))) \
    .filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()) \
    .filter((col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)) \
    .cache()

totalRows = dfFinal.count()

# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 70)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 70)
for varName in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\n{varName}")
    dfFinal.groupBy(varName).agg(
        count(lit(1)).alias("Frequency")
    ).withColumn(
        "Percent", sparkRound(col("Frequency") / lit(totalRows) * 100, 2)
    ).orderBy(varName).show(truncate=False)

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   proc means data=... n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
#
# PROC MEANS CLASS -> groupBy; median -> percentile_approx.
# ------------------------------------------------------------------
def meansByClass(dataFrame, classCols, varCols, title):
    """PROC MEANS with CLASS: N, Mean, Median, Std, Min, Max per group."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    aggregations = []
    for varName in varCols:
        aggregations += [
            count(col(varName)).alias(f"{varName}_N"),
            sparkRound(mean(col(varName)), 2).alias(f"{varName}_Mean"),
            sparkRound(expr(f"percentile_approx({varName}, 0.5)"), 2).alias(f"{varName}_Median"),
            sparkRound(stddev(col(varName)), 2).alias(f"{varName}_Std"),
            sparkRound(sparkMin(col(varName)), 2).alias(f"{varName}_Min"),
            sparkRound(sparkMax(col(varName)), 2).alias(f"{varName}_Max"),
        ]
    dataFrame.groupBy(*classCols).agg(*aggregations).orderBy(*classCols).show(truncate=False)


meansByClass(
    dfFinal, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    "Summary Statistics by Loan Outcome"
)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION; var BAD;
#       table JOB all='Total', REGION * BAD * (n mean) all * BAD * (n mean);
#   run;
#
# PROC TABULATE maps to groupBy().pivot() for the column dimension.
# ------------------------------------------------------------------
print("\n" + "=" * 70)
print("Default Rates by Job Category and Region (N and mean of BAD)")
print("=" * 70)
regions = [row["REGION"] for row in dfFinal.select("REGION").distinct().orderBy("REGION").collect()]
dfFinal.groupBy("JOB").pivot("REGION", regions).agg(
    count(col("BAD")).alias("N"),
    sparkRound(mean(col("BAD")) * 100, 2).alias("DefaultPct")
).orderBy("JOB").show(truncate=False)

print("Totals by Job Category")
dfFinal.groupBy("JOB").agg(
    count(col("BAD")).alias("N"),
    sparkRound(mean(col("BAD")) * 100, 2).alias("DefaultPct")
).orderBy("JOB").show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount
# SAS equivalent:
#   proc sql outobs=10;
#       select STATE, count(*), mean(LOAN), mean(VALUE), mean(BAD)
#       from work.home_equity_final group by STATE
#       having count(*) >= 10 order by avg_loan desc;
#   quit;
#
# PROC SQL maps directly to Spark SQL over a temp view.
# ------------------------------------------------------------------
dfFinal.createOrReplaceTempView("home_equity_final")

print("\n" + "=" * 70)
print("Top 10 States by Average Loan Amount")
print("=" * 70)
spark.sql("""
    SELECT STATE,
           COUNT(*)                        AS num_loans,
           ROUND(AVG(LOAN), 2)             AS avg_loan,
           ROUND(AVG(VALUE), 2)            AS avg_property_value,
           ROUND(AVG(BAD) * 100, 2)        AS default_rate_pct
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
""").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Loan distribution by reason and outcome
# SAS equivalent:
#   proc means data=... n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
meansByClass(
    dfFinal, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"],
    "Loan Amount Distribution by Reason and Outcome"
)

spark.stop()
