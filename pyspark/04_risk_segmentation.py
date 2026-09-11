"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, sum as spark_sum,
    round as spark_round
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from script 02)
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
df = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default"))
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull() &
        (col("LOAN") > 0) & (col("VALUE") > 0) &
        (col("LTV") > 0) & (col("LTV") < 5)
    )

# ------------------------------------------------------------------
# Step 1: Create risk bucket columns
# SAS equivalent:
#   proc format;
#       value ltv_risk
#           low  -< 0.60 = 'Low'
#           0.60 -< 0.80 = 'Medium'
#           0.80 - high   = 'High';
#       value dti_risk
#           low  -< 30  = 'Low'
#           30   -< 40  = 'Medium'
#           40   -< 50  = 'High'
#           50   - high  = 'Very High';
#   run;
#
# PySpark uses when/otherwise chains instead of PROC FORMAT value ranges.
# ------------------------------------------------------------------

# LTV Risk Category
dfRisk = df.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
)

# Debt-to-Income Risk Category
dfRisk = dfRisk.withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
)

# Delinquency Risk Category
dfRisk = dfRisk.withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
)

# ------------------------------------------------------------------
# Step 2: Create composite risk score (0-10 scale)
# SAS equivalent:
#   RISK_SCORE = 0;
#   /* LTV component (0-3 points) */
#   if LTV >= 0.80 then RISK_SCORE + 3;
#   else if LTV >= 0.60 then RISK_SCORE + 1.5;
#   /* DTI component (0-3 points) */
#   ...
# ------------------------------------------------------------------

# LTV component (0-3 points)
ltvScore = (
    when(col("LTV").isNull(), lit(0))
    .when(col("LTV") >= 0.80, lit(3))
    .when(col("LTV") >= 0.60, lit(1.5))
    .otherwise(lit(0))
)

# DTI component (0-3 points)
dtiScore = (
    when(col("DEBTINC").isNull(), lit(0))
    .when(col("DEBTINC") >= 50, lit(3))
    .when(col("DEBTINC") >= 40, lit(2))
    .when(col("DEBTINC") >= 30, lit(1))
    .otherwise(lit(0))
)

# Delinquency component (0-2 points)
delinqScore = (
    when(col("DELINQ").isNull(), lit(0))
    .when(col("DELINQ") >= 4, lit(2))
    .when(col("DELINQ") >= 2, lit(1.5))
    .when(col("DELINQ") == 1, lit(0.5))
    .otherwise(lit(0))
)

# Derogatory reports component (0-2 points)
derogScore = (
    when(col("DEROG").isNull(), lit(0))
    .when(col("DEROG") >= 3, lit(2))
    .when(col("DEROG") >= 1, lit(1))
    .otherwise(lit(0))
)

dfRisk = dfRisk.withColumn(
    "RISK_SCORE",
    ltvScore + dtiScore + delinqScore + derogScore
)

# Risk segment based on composite score
dfRisk = dfRisk.withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
).cache()
totalCount = dfRisk.count()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Distribution of Loans by Risk Segment")
print("(equivalent to PROC FREQ)")
print("=" * 60)

for segCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\n--- {segCol} ---")
    dfNonMissing = dfRisk.filter(col(segCol).isNotNull())
    nonMissingCount = dfNonMissing.count()
    dfNonMissing.groupBy(segCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(nonMissingCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(segCol) \
        .show(truncate=False)
    print(f"Frequency Missing = {totalCount - nonMissingCount}")

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment")
print("(equivalent to PROC MEANS with CLASS)")
print("=" * 60)

analysisVars = ["BAD", "LOAN", "LTV", "DEBTINC"]
segmentOrder = when(col("RISK_SEGMENT") == "Low Risk", 1) \
    .when(col("RISK_SEGMENT") == "Medium Risk", 2) \
    .when(col("RISK_SEGMENT") == "High Risk", 3) \
    .otherwise(4)

meansByVar = [
    dfRisk.groupBy("RISK_SEGMENT")
    .agg(
        count("*").alias("N_Obs"),
        lit(i).alias("VarOrder"),
        lit(varName).alias("Variable"),
        count(varName).alias("N"),
        spark_round(mean(varName), 4).alias("Mean"),
        spark_round(stddev(varName), 4).alias("Std")
    )
    for i, varName in enumerate(analysisVars)
]

reduce(lambda a, b: a.unionByName(b), meansByVar) \
    .withColumn("SegOrder", segmentOrder) \
    .orderBy("SegOrder", "VarOrder") \
    .select("RISK_SEGMENT", "N_Obs", "Variable", "N", "Mean", "Std") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("=" * 60)

dfCross = dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull() & col("BAD").isNotNull()
)
dfCross.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .agg(
        spark_sum(when(col("BAD") == 0, 1).otherwise(0)).alias("BAD_0"),
        spark_sum(when(col("BAD") == 1, 1).otherwise(0)).alias("BAD_1"),
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(20, truncate=False)
print(f"Frequency Missing = {totalCount - dfCross.count()}")

# Clean up
spark.stop()
