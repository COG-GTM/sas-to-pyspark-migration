"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (SAS PROC FORMAT equivalents)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, round as spark_round
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Reproduce the cleaned dataset (work.home_equity_final)
# In SAS this script reads work.home_equity_final, produced by
# 02_data_cleaning.sas. We re-derive it here directly from the CSV
# so this script is self-contained and runnable on its own.
# SAS equivalent (from 02_data_cleaning.sas):
#   LTV = MORTDUE / VALUE (when VALUE > 0 and both non-missing);
#   LOAN_OUTCOME = 'Paid'/'Default' based on BAD;
#   keep LOAN, VALUE, BAD non-missing;
#   keep 0 < LTV < 5, LOAN > 0, VALUE > 0.
# ------------------------------------------------------------------
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

dfFinal = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    ) \
    .filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) &
        (col("VALUE") > 0)
    )

# ------------------------------------------------------------------
# Step 1: Define risk bucket "formats"
# SAS equivalent: proc format; value ltv_risk / dti_risk / delinq_risk /
#   risk_score ...; run;
# Note: PySpark has no native PROC FORMAT / value formats. The bucket
# boundaries are encoded directly in the CASE (when/otherwise) logic
# of Step 2 below, matching the SAS format definitions:
#   ltv_risk    : <0.60 Low | 0.60-<0.80 Medium | >=0.80 High
#   dti_risk    : <30 Low | 30-<40 Medium | 40-<50 High | >=50 Very High
#   delinq_risk : 0 None | 1 Low | 2-3 Medium | >=4 High
#   risk_score  : <3 Low Risk | 3-<5 Medium Risk | 5-<7 High Risk |
#                 >=7 Very High Risk
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       ... if/else assignments for the risk categories ...
#       RISK_SCORE = 0; ... additive components ...
#       ... RISK_SEGMENT assignment ...
#   run;
# ------------------------------------------------------------------
dfRisk = dfFinal \
    .withColumn(
        # LTV risk category (LTV_RISK_CAT $6)
        # SAS: if LTV<0.60 'Low'; else if <0.80 'Medium'; else 'High'
        "LTV_RISK_CAT",
        when(col("LTV").isNull(), lit(None).cast("string"))
        .when(col("LTV") < 0.60, lit("Low"))
        .when(col("LTV") < 0.80, lit("Medium"))
        .otherwise(lit("High"))
    ) \
    .withColumn(
        # Debt-to-Income risk category (DTI_RISK_CAT $9)
        # SAS: <30 'Low'; <40 'Medium'; <50 'High'; else 'Very High'
        "DTI_RISK_CAT",
        when(col("DEBTINC").isNull(), lit(None).cast("string"))
        .when(col("DEBTINC") < 30, lit("Low"))
        .when(col("DEBTINC") < 40, lit("Medium"))
        .when(col("DEBTINC") < 50, lit("High"))
        .otherwise(lit("Very High"))
    ) \
    .withColumn(
        # Delinquency risk category (DELINQ_RISK_CAT $6)
        # SAS: 0 'None'; 1 'Low'; <=3 'Medium'; else 'High'
        "DELINQ_RISK_CAT",
        when(col("DELINQ").isNull(), lit(None).cast("string"))
        .when(col("DELINQ") == 0, lit("None"))
        .when(col("DELINQ") == 1, lit("Low"))
        .when(col("DELINQ") <= 3, lit("Medium"))
        .otherwise(lit("High"))
    )

# Composite risk score (0-10 scale)
# SAS equivalent: RISK_SCORE = 0; then additive component blocks.
dfRisk = dfRisk.withColumn(
    "RISK_SCORE",
    lit(0.0)
    # LTV component (0-3 points)
    # SAS: if LTV>=0.80 +3; else if LTV>=0.60 +1.5
    + when(col("LTV").isNull(), lit(0.0))
    .when(col("LTV") >= 0.80, lit(3.0))
    .when(col("LTV") >= 0.60, lit(1.5))
    .otherwise(lit(0.0))
    # DTI component (0-3 points)
    # SAS: if DEBTINC>=50 +3; else if >=40 +2; else if >=30 +1
    + when(col("DEBTINC").isNull(), lit(0.0))
    .when(col("DEBTINC") >= 50, lit(3.0))
    .when(col("DEBTINC") >= 40, lit(2.0))
    .when(col("DEBTINC") >= 30, lit(1.0))
    .otherwise(lit(0.0))
    # Delinquency component (0-2 points)
    # SAS: if DELINQ>=4 +2; else if >=2 +1.5; else if =1 +0.5
    + when(col("DELINQ").isNull(), lit(0.0))
    .when(col("DELINQ") >= 4, lit(2.0))
    .when(col("DELINQ") >= 2, lit(1.5))
    .when(col("DELINQ") == 1, lit(0.5))
    .otherwise(lit(0.0))
    # Derogatory reports component (0-2 points)
    # SAS: if DEROG>=3 +2; else if >=1 +1
    + when(col("DEROG").isNull(), lit(0.0))
    .when(col("DEROG") >= 3, lit(2.0))
    .when(col("DEROG") >= 1, lit(1.0))
    .otherwise(lit(0.0))
)

# Risk score category (RISK_SEGMENT $14)
# SAS: <3 'Low Risk'; <5 'Medium Risk'; <7 'High Risk'; else 'Very High Risk'
dfRisk = dfRisk.withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

# SAS labels (no PySpark equivalent), documented here:
#   LTV_RISK_CAT    = "LTV Risk Category"
#   DTI_RISK_CAT    = "Debt-to-Income Risk Category"
#   DELINQ_RISK_CAT = "Delinquency Risk Category"
#   RISK_SCORE      = "Composite Risk Score (0-10)"
#   RISK_SEGMENT    = "Risk Segment"

# Cache since dfRisk is reused across the reporting steps below
dfRisk = dfRisk.cache()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)

totalCount = dfRisk.count()
for catCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\n--- {catCol} ---")
    dfRisk.groupBy(catCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(count("*") / lit(totalCount) * 100, 2).alias("Percent")
        ) \
        .orderBy(col("Frequency").desc()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)

dfRisk.groupBy("RISK_SEGMENT") \
    .agg(
        count("*").alias("N"),
        spark_round(mean("BAD"), 4).alias("Mean_BAD"),
        spark_round(stddev("BAD"), 4).alias("Std_BAD"),
        spark_round(mean("LOAN"), 2).alias("Mean_LOAN"),
        spark_round(stddev("LOAN"), 2).alias("Std_LOAN"),
        spark_round(mean("LTV"), 4).alias("Mean_LTV"),
        spark_round(stddev("LTV"), 4).alias("Std_LTV"),
        spark_round(mean("DEBTINC"), 2).alias("Mean_DEBTINC"),
        spark_round(stddev("DEBTINC"), 2).alias("Std_DEBTINC")
    ) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ)")
print("=" * 60)

dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD") \
    .agg(count("*").alias("Frequency")) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD") \
    .show(100, truncate=False)

# Clean up
spark.stop()
