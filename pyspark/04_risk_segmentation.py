"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalent)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, sum as spark_sum, round as spark_round
)

# Resolve the dataset relative to the project root so the script runs from any CWD
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()


# ------------------------------------------------------------------
# Upstream steps replicated inline so this script runs standalone
# (01_data_loading -> work.home_equity, 02_data_cleaning -> work.home_equity_final)
# ------------------------------------------------------------------
def loadHomeEquity(sparkSession, path):
    """SAS: PROC IMPORT dbms=csv -> spark.read.csv"""
    return sparkSession.read.csv(path, header=True, inferSchema=True)


def buildHomeEquityFinal(df):
    """Replicates sas/02_data_cleaning.sas up to work.home_equity_final."""
    # SAS: DATA step IF/THEN/ELSE -> when().otherwise(); missing (.) -> null
    dfClean = df.withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ).withColumn(
        # SAS: propcase() -> initcap()
        "CITY", initcap(col("CITY"))
    )

    # SAS: ARRAY num_vars / num_flags + DO loop -> Python loop with withColumn
    for varName in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
        dfClean = dfClean.withColumn(
            f"{varName}_MISS", when(col(varName).isNull(), lit(1)).otherwise(lit(0))
        )

    # SAS: subsetting IF (LOAN, VALUE, BAD non-missing) -> filter()
    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
    #      (null comparisons are false in Spark, matching SAS dropping missing LTV)
    return dfClean.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    ).filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )


def showFrequency(df, colName):
    """SAS: PROC FREQ tables <var> / nocum -> groupBy().count() with percent.
    Like SAS, missing values are excluded from percentages and reported separately."""
    nonMissing = df.filter(col(colName).isNotNull())
    total = nonMissing.count()
    # SAS orders character levels by unformatted value (alphabetical)
    nonMissing.groupBy(colName).count() \
        .withColumnRenamed("count", "Frequency") \
        .withColumn("Percent", spark_round(col("Frequency") * 100.0 / lit(total), 2)) \
        .orderBy(colName) \
        .show(truncate=False)
    missing = df.filter(col(colName).isNull()).count()
    if missing > 0:
        print(f"Frequency Missing = {missing}\n")


baseDf = loadHomeEquity(spark, dataPath)
homeEquityFinal = buildHomeEquityFinal(baseDf)

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low'  0.60 -< 0.80 = 'Medium'  0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low'  30 -< 40 = 'Medium'  40 -< 50 = 'High'  50 - high = 'Very High';
#       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'  4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk'  3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk'  7 - high = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each value -> label range becomes a
# when().otherwise() chain; null input stays null (SAS leaves the char var blank).
# ------------------------------------------------------------------
ltvRiskCat = when(col("LTV").isNull(), lit(None)) \
    .when(col("LTV") < 0.60, lit("Low")) \
    .when(col("LTV") < 0.80, lit("Medium")) \
    .otherwise(lit("High"))

dtiRiskCat = when(col("DEBTINC").isNull(), lit(None)) \
    .when(col("DEBTINC") < 30, lit("Low")) \
    .when(col("DEBTINC") < 40, lit("Medium")) \
    .when(col("DEBTINC") < 50, lit("High")) \
    .otherwise(lit("Very High"))

delinqRiskCat = when(col("DELINQ").isNull(), lit(None)) \
    .when(col("DELINQ") == 0, lit("None")) \
    .when(col("DELINQ") == 1, lit("Low")) \
    .when(col("DELINQ") <= 3, lit("Medium")) \
    .otherwise(lit("High"))

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do; ... LTV_RISK_CAT = ...; end;
#       RISK_SCORE = 0;
#       if LTV ne . then do; if LTV >= 0.80 then RISK_SCORE + 3; ... end;
#       ...
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk'; ...
#   run;
#
# The accumulating RISK_SCORE becomes a sum of four component expressions;
# a missing component contributes 0 (the SAS "if X ne . then do" guard).
# ------------------------------------------------------------------
# LTV component (0-3 points)
ltvScore = when(col("LTV").isNull(), lit(0.0)) \
    .when(col("LTV") >= 0.80, lit(3.0)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0.0))

# DTI component (0-3 points)
dtiScore = when(col("DEBTINC").isNull(), lit(0.0)) \
    .when(col("DEBTINC") >= 50, lit(3.0)) \
    .when(col("DEBTINC") >= 40, lit(2.0)) \
    .when(col("DEBTINC") >= 30, lit(1.0)) \
    .otherwise(lit(0.0))

# Delinquency component (0-2 points)
delinqScore = when(col("DELINQ").isNull(), lit(0.0)) \
    .when(col("DELINQ") >= 4, lit(2.0)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0.0))

# Derogatory reports component (0-2 points)
derogScore = when(col("DEROG").isNull(), lit(0.0)) \
    .when(col("DEROG") >= 3, lit(2.0)) \
    .when(col("DEROG") >= 1, lit(1.0)) \
    .otherwise(lit(0.0))

homeEquityRisk = homeEquityFinal \
    .withColumn("LTV_RISK_CAT", ltvRiskCat) \
    .withColumn("DTI_RISK_CAT", dtiRiskCat) \
    .withColumn("DELINQ_RISK_CAT", delinqRiskCat) \
    .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore)

# SAS: value risk_score format / RISK_SEGMENT IF/ELSE chain -> when().otherwise()
homeEquityRisk = homeEquityRisk.withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
).cache()

# SAS: LABEL statement -> metadata dictionary (no native column labels in Spark)
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("=" * 60)
print("Column Labels (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:16s} -> {label}")

print(f"\nRows in work.home_equity_risk: {homeEquityRisk.count()}")

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)
for colName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    showFrequency(homeEquityRisk, colName)

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#
# N per variable counts non-missing values, as in PROC MEANS; stddev is the
# sample standard deviation (SAS STD). Mean of BAD is the default rate.
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)
meansAggs = [count(lit(1)).alias("N_Obs")]
for varName in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(varName).alias(f"{varName}_N"),
        spark_round(mean(varName), 4).alias(f"{varName}_Mean"),
        spark_round(stddev(varName), 4).alias(f"{varName}_Std"),
    ]
homeEquityRisk.groupBy("RISK_SEGMENT").agg(*meansAggs) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#
# A 3-way TABLES request gives one DTI_RISK_CAT x BAD frequency table per
# LTV_RISK_CAT level -> groupBy(strata, row).pivot(BAD).count().
# Rows with a missing value in any table variable are excluded, as in SAS.
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ 3-way)")
print("=" * 60)
crossTab = homeEquityRisk \
    .filter(
        col("LTV_RISK_CAT").isNotNull() &
        col("DTI_RISK_CAT").isNotNull() &
        col("BAD").isNotNull()
    ) \
    .groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .count() \
    .na.fill(0, ["0", "1"]) \
    .withColumnRenamed("0", "BAD_0") \
    .withColumnRenamed("1", "BAD_1") \
    .withColumn("Total", col("BAD_0") + col("BAD_1"))

strata = [r["LTV_RISK_CAT"] for r in
          crossTab.select("LTV_RISK_CAT").distinct().orderBy("LTV_RISK_CAT").collect()]
for tableNum, stratum in enumerate(strata, start=1):
    print(f"\nTable {tableNum} of DTI_RISK_CAT by BAD, Controlling for LTV_RISK_CAT={stratum}")
    stratumTab = crossTab.filter(col("LTV_RISK_CAT") == stratum) \
        .drop("LTV_RISK_CAT") \
        .orderBy("DTI_RISK_CAT")
    totals = stratumTab.agg(
        lit("Total").alias("DTI_RISK_CAT"),
        spark_sum("BAD_0").alias("BAD_0"),
        spark_sum("BAD_1").alias("BAD_1"),
        spark_sum("Total").alias("Total"),
    )
    stratumTab.unionByName(totals).show(truncate=False)

# Clean up
homeEquityRisk.unpersist()
spark.stop()
