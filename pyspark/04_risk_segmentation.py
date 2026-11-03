"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
  - Create a composite 0-10 risk score
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, round as spark_round,
    min as spark_min, max as spark_max
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load data and reproduce upstream cleaning (work.home_equity_final)
# SAS equivalent (from sas/02_data_cleaning.sas):
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered;
#       set work.home_equity_clean;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: SAS chains off work.home_equity_final from 02_data_cleaning.
# This script is self-contained, so the cleaning is reproduced inline.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)
print(f"Source rows: {df.count()}")

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
    "CITY",
    initcap(col("CITY"))
)

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)
print(f"Rows after null filter (LOAN, VALUE, BAD): {dfFiltered.count()}")

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)
print(f"Rows in final clean dataset: {dfFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low'   0.60 -< 0.80 = 'Medium'
#                         0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low'     30 -< 40 = 'Medium'
#                         40 -< 50 = 'High'     50 - high = 'Very High';
#       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'
#                         4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk'   3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk'    7 - high = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each format is expressed as a
# when() chain; "low -< x" maps to "< x" and "x - high" to otherwise().
# Missing inputs stay null, matching SAS's blank character values.
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
#       if LTV ne . then do; ... LTV_RISK_CAT = ...; end;
#       if DEBTINC ne . then do; ... DTI_RISK_CAT = ...; end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT = ...; end;
#
#       RISK_SCORE = 0;
#       /* LTV (0-3) */    >=0.80 +3, >=0.60 +1.5
#       /* DTI (0-3) */    >=50 +3, >=40 +2, >=30 +1
#       /* DELINQ (0-2) */ >=4 +2, >=2 +1.5, =1 +0.5
#       /* DEROG (0-2) */  >=3 +2, >=1 +1
#
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#   run;
#
# Note: each score component contributes 0 when its input is missing,
# mirroring the SAS "if X ne . then do" guards around each increment.
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
    .withColumn("LTV_RISK_CAT", ltvRisk(col("LTV"))) \
    .withColumn("DTI_RISK_CAT", dtiRisk(col("DEBTINC"))) \
    .withColumn("DELINQ_RISK_CAT", delinqRisk(col("DELINQ"))) \
    .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore) \
    .withColumn("RISK_SEGMENT", riskSegment(col("RISK_SCORE"))) \
    .cache()

# SAS equivalent: label statements (documented as metadata)
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Variables (SAS-style variable labels)")
print("=" * 60)
for name, label in columnLabels.items():
    print(f"  {name:16s} -> {label}")

print("\nComposite Risk Score range (expected 0-10):")
dfRisk.select(
    spark_min("RISK_SCORE").alias("MIN_RISK_SCORE"),
    spark_max("RISK_SCORE").alias("MAX_RISK_SCORE"),
).show()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#
# Note: like PROC FREQ, percentages exclude missing values and the
# missing count is reported separately ("Frequency Missing").
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment")
print("=" * 60)
for freqVar in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    nonMissing = dfRisk.filter(col(freqVar).isNotNull())
    totalNonMissing = nonMissing.count()
    nMissing = dfRisk.count() - totalNonMissing
    print(f"\n{freqVar} ({columnLabels[freqVar]})")
    nonMissing.groupBy(freqVar) \
        .agg(count("*").alias("Frequency")) \
        .withColumn("Percent", spark_round(col("Frequency") * 100.0 / totalNonMissing, 2)) \
        .orderBy(freqVar) \
        .show(truncate=False)
    print(f"Frequency Missing = {nMissing}")

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment")
print("=" * 60)
meansAggs = []
for meansVar in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(col(meansVar)).alias(f"{meansVar}_N"),
        spark_round(mean(col(meansVar)), 4).alias(f"{meansVar}_MEAN"),
        spark_round(stddev(col(meansVar)), 4).alias(f"{meansVar}_STD"),
    ]
dfRisk.groupBy("RISK_SEGMENT") \
    .agg(count("*").alias("N_OBS"), *meansAggs) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#
# Note: PROC FREQ produces one DTI_RISK_CAT x BAD table per LTV_RISK_CAT
# stratum and excludes rows missing any table variable. Here the strata
# are rows of a single groupBy/pivot on BAD (frequencies only).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("=" * 60)
dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull() & col("BAD").isNotNull()
).groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "BAD_0") \
    .withColumnRenamed("1", "BAD_1") \
    .withColumn("TOTAL", col("BAD_0") + col("BAD_1")) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
spark.stop()
