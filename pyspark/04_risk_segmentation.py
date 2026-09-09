"""
PySpark Script: 04_risk_segmentation.py
Purpose: Build risk buckets, a composite risk score and segment-level reporting
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, round as sparkRound
)

spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Rebuild work.home_equity_final (see 02_data_cleaning.py)
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
    .filter((col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0))

# ------------------------------------------------------------------
# Step 1: Risk bucket definitions
# SAS equivalent:
#   proc format;
#       value ltv_risk low -< 0.60 = 'Low' ...;
#   run;
#
# PROC FORMAT value ranges become chained when()/otherwise() expressions;
# a null input yields null instead of matching a range.
# ------------------------------------------------------------------
ltvRiskCat = when(col("LTV").isNull(), lit(None).cast("string")) \
    .when(col("LTV") < 0.60, lit("Low")) \
    .when(col("LTV") < 0.80, lit("Medium")) \
    .otherwise(lit("High"))

dtiRiskCat = when(col("DEBTINC").isNull(), lit(None).cast("string")) \
    .when(col("DEBTINC") < 30, lit("Low")) \
    .when(col("DEBTINC") < 40, lit("Medium")) \
    .when(col("DEBTINC") < 50, lit("High")) \
    .otherwise(lit("Very High"))

delinqRiskCat = when(col("DELINQ").isNull(), lit(None).cast("string")) \
    .when(col("DELINQ") == 0, lit("None")) \
    .when(col("DELINQ") == 1, lit("Low")) \
    .when(col("DELINQ") <= 3, lit("Medium")) \
    .otherwise(lit("High"))

# ------------------------------------------------------------------
# Step 2: Composite risk score (0-10 scale)
# SAS equivalent: RISK_SCORE accumulated with IF/THEN blocks in a DATA step.
# Each SAS component becomes an expression; missing values contribute 0.
# ------------------------------------------------------------------
ltvScore = when(col("LTV").isNull(), lit(0.0)) \
    .when(col("LTV") >= 0.80, lit(3.0)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0.0))

dtiScore = when(col("DEBTINC").isNull(), lit(0.0)) \
    .when(col("DEBTINC") >= 50, lit(3.0)) \
    .when(col("DEBTINC") >= 40, lit(2.0)) \
    .when(col("DEBTINC") >= 30, lit(1.0)) \
    .otherwise(lit(0.0))

delinqScore = when(col("DELINQ").isNull(), lit(0.0)) \
    .when(col("DELINQ") >= 4, lit(2.0)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0.0))

derogScore = when(col("DEROG").isNull(), lit(0.0)) \
    .when(col("DEROG") >= 3, lit(2.0)) \
    .when(col("DEROG") >= 1, lit(1.0)) \
    .otherwise(lit(0.0))

dfRisk = dfFinal \
    .withColumn("LTV_RISK_CAT", ltvRiskCat) \
    .withColumn("DTI_RISK_CAT", dtiRiskCat) \
    .withColumn("DELINQ_RISK_CAT", delinqRiskCat) \
    .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore) \
    .withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk"))
    ).cache()

riskLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

totalRows = dfRisk.count()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 70)
print("Distribution of Loans by Risk Segment")
print("=" * 70)
for varName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\n{varName}")
    dfRisk.groupBy(varName).agg(
        count(lit(1)).alias("Frequency")
    ).withColumn(
        "Percent", sparkRound(col("Frequency") / lit(totalRows) * 100, 2)
    ).orderBy(varName).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 70)
print("Average Default Rate by Risk Segment")
print("=" * 70)
aggregations = []
for varName in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    aggregations += [
        count(col(varName)).alias(f"{varName}_N"),
        sparkRound(mean(col(varName)), 4).alias(f"{varName}_Mean"),
        sparkRound(stddev(col(varName)), 4).alias(f"{varName}_Std"),
    ]
dfRisk.groupBy("RISK_SEGMENT").agg(*aggregations).orderBy("RISK_SEGMENT").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 70)
print("Default Counts by LTV Risk and DTI Risk")
print("=" * 70)
dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT").pivot("BAD", [0, 1]).agg(
    count(lit(1))
).withColumnRenamed("0", "BAD_0").withColumnRenamed("1", "BAD_1") \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT").show(truncate=False)

spark.stop()
