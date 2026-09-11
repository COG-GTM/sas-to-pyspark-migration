"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev,
    round as spark_round
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from script 02 so that df matches
# work.home_equity_final, including the LTV sanity filter 0 < LTV < 5)
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
)

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

totalCount = dfRisk.count()
for segCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\n--- {segCol} ---")
    dfRisk.groupBy(segCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(segCol) \
        .show(truncate=False)

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

dfRisk.groupBy("RISK_SEGMENT") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct"),
        spark_round(mean("LOAN"), 2).alias("Avg_LOAN"),
        spark_round(mean("LTV"), 4).alias("Avg_LTV"),
        spark_round(mean("DEBTINC"), 2).alias("Avg_DEBTINC")
    ) \
    .orderBy("RISK_SEGMENT") \
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

dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct")
    ) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(20, truncate=False)

# Clean up
spark.stop()
