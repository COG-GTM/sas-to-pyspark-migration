"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas

Self-contained: the upstream cleaning from sas/02_data_cleaning.sas
(work.home_equity_final) is replicated inline as helper functions.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev
)

# Resolve data path from the project root, not the current working directory
projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()


# ------------------------------------------------------------------
# Upstream helpers (replicates sas/02_data_cleaning.sas)
# ------------------------------------------------------------------
def deriveColumns(df):
    """SAS: DATA work.home_equity_clean -> LTV, LOAN_OUTCOME, propcase(CITY)."""
    return df \
        .withColumn(
            "LTV",
            # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE; else LTV = .;
            when(
                col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            # SAS: if BAD = 0 then 'Paid'; else if BAD = 1 then 'Default'; else '';
            when(col("BAD") == 0, lit("Paid"))
            .when(col("BAD") == 1, lit("Default"))
            .otherwise(lit(""))
        ) \
        .withColumn("CITY", initcap(col("CITY")))  # SAS: propcase(CITY) -> initcap


def flagMissing(df):
    """SAS: ARRAY num_vars / num_flags + DO loop -> Python loop over columns."""
    for c in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
        df = df.withColumn(f"{c}_MISS", when(col(c).isNull(), 1).otherwise(0))
    return df


def filterCritical(df):
    """SAS: if LOAN ne . and VALUE ne . and BAD ne .; (subsetting IF -> filter)."""
    return df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )


def removeOutliers(df):
    """SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;"""
    return df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )


# ------------------------------------------------------------------
# Step 0: Build work.home_equity_final
# SAS equivalent:
#   proc import ... out=work.home_equity;   (01_data_loading.sas)
#   data work.home_equity_final; ...        (02_data_cleaning.sas)
# ------------------------------------------------------------------
dfRaw = spark.read.csv(dataPath, header=True, inferSchema=True)  # SAS: PROC IMPORT -> spark.read.csv
dfFiltered = filterCritical(flagMissing(deriveColumns(dfRaw)))
dfFinal = removeOutliers(dfFiltered)

print("=" * 60)
print("Upstream row counts (02_data_cleaning)")
print("=" * 60)
print(f"  Raw (work.home_equity):            {dfRaw.count()}")
print(f"  Filtered (work.home_equity_filtered): {dfFiltered.count()}")
print(f"  Final (work.home_equity_final):    {dfFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low' 0.60 -< 0.80 = 'Medium' 0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low' 30 -< 40 = 'Medium' 40 -< 50 = 'High' 50 - high = 'Very High';
#       value delinq_risk 0 = 'None' 1 = 'Low' 2 - 3 = 'Medium' 4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk' 3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk' 7 - high = 'Very High Risk';
#   run;
#
# PySpark has no PROC FORMAT; each format becomes a reusable
# when().otherwise() expression. A missing input (.) yields null.
# ------------------------------------------------------------------
def ltvRisk(c):
    return when(c.isNull(), lit(None)) \
        .when(c < 0.60, lit("Low")) \
        .when(c < 0.80, lit("Medium")) \
        .otherwise(lit("High"))


def dtiRisk(c):
    return when(c.isNull(), lit(None)) \
        .when(c < 30, lit("Low")) \
        .when(c < 40, lit("Medium")) \
        .when(c < 50, lit("High")) \
        .otherwise(lit("Very High"))


def delinqRisk(c):
    return when(c.isNull(), lit(None)) \
        .when(c == 0, lit("None")) \
        .when(c == 1, lit("Low")) \
        .when(c <= 3, lit("Medium")) \
        .otherwise(lit("High"))


def riskSegment(c):
    return when(c < 3, lit("Low Risk")) \
        .when(c < 5, lit("Medium Risk")) \
        .when(c < 7, lit("High Risk")) \
        .otherwise(lit("Very High Risk"))


# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do; if LTV < 0.60 then LTV_RISK_CAT = 'Low'; ... end;
#       ...
#       RISK_SCORE = 0;
#       if LTV ne . then do; if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3; ... end;
#       ...
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk'; ...
#   run;
#
# The accumulating RISK_SCORE becomes a sum of four component
# expressions; a missing component contributes 0 (SAS: if X ne . then do).
# ------------------------------------------------------------------
ltvScore = when(col("LTV").isNull(), lit(0)) \
    .when(col("LTV") >= 0.80, lit(3)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0))                                   # LTV component (0-3)
dtiScore = when(col("DEBTINC").isNull(), lit(0)) \
    .when(col("DEBTINC") >= 50, lit(3)) \
    .when(col("DEBTINC") >= 40, lit(2)) \
    .when(col("DEBTINC") >= 30, lit(1)) \
    .otherwise(lit(0))                                   # DTI component (0-3)
delinqScore = when(col("DELINQ").isNull(), lit(0)) \
    .when(col("DELINQ") >= 4, lit(2)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0))                                   # Delinquency component (0-2)
derogScore = when(col("DEROG").isNull(), lit(0)) \
    .when(col("DEROG") >= 3, lit(2)) \
    .when(col("DEROG") >= 1, lit(1)) \
    .otherwise(lit(0))                                   # Derogatory component (0-2)

dfRisk = dfFinal \
    .withColumn("LTV_RISK_CAT", ltvRisk(col("LTV"))) \
    .withColumn("DTI_RISK_CAT", dtiRisk(col("DEBTINC"))) \
    .withColumn("DELINQ_RISK_CAT", delinqRisk(col("DELINQ"))) \
    .withColumn("RISK_SCORE", (ltvScore + dtiScore + delinqScore + derogScore).cast("double")) \
    .withColumn("RISK_SEGMENT", riskSegment(col("RISK_SCORE")))
dfRisk.cache()

# SAS: LABEL statement -> metadata dictionary (no native column labels in Spark)
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Variables (work.home_equity_risk)")
print("=" * 60)
for c, label in columnLabels.items():
    print(f"  {c:16s} -> {label}")
print(f"  Rows: {dfRisk.count()}")

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
total = dfRisk.count()
for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print("\n" + "=" * 60)
    print(f"Distribution of Loans by {c} (equivalent to PROC FREQ / nocum)")
    print("=" * 60)
    # SAS: PROC FREQ excludes missing levels from the table; report them separately
    nonMissing = dfRisk.filter(col(c).isNotNull())
    nNonMissing = nonMissing.count()
    nonMissing.groupBy(c).count() \
        .withColumn("Percent", (col("count") / lit(nNonMissing) * 100)) \
        .orderBy(c) \
        .show(truncate=False)
    print(f"  Frequency Missing = {total - nNonMissing}")

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)
aggExprs = []
for v in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    aggExprs += [
        count(v).alias(f"{v}_N"),
        mean(v).alias(f"{v}_Mean"),
        stddev(v).alias(f"{v}_Std"),
    ]
dfRisk.groupBy("RISK_SEGMENT").agg(count(lit(1)).alias("N_Obs"), *aggExprs) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#
# A three-way PROC FREQ table becomes groupBy + pivot on BAD; rows with
# missing LTV/DTI categories are excluded, as PROC FREQ does by default.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ 3-way)")
print("=" * 60)
dfRisk.filter(col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull()) \
    .groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "BAD_0") \
    .withColumnRenamed("1", "BAD_1") \
    .withColumn("Total", col("BAD_0") + col("BAD_1")) \
    .withColumn("Default_Rate", col("BAD_1") / col("Total")) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
dfRisk.unpersist()
spark.stop()
