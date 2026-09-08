"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (SAS PROC FORMAT equivalents)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, mean, stddev

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load the input data and rebuild work.home_equity_final
# SAS equivalent:
#   set work.home_equity_final;   /* dataset created by 02_data_cleaning.sas */
#
# The SAS program reads the cleaned dataset produced by
# 02_data_cleaning.sas. Since each PySpark script is self-contained,
# the LTV derivation and the final filters from that program are
# re-applied here so the input matches work.home_equity_final.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

home_equity_final = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull()
            & col("MORTDUE").isNotNull()
            & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None))
    ) \
    .filter(
        col("LOAN").isNotNull()
        & col("VALUE").isNotNull()
        & col("BAD").isNotNull()
    ) \
    .filter(
        (col("LTV") > 0) & (col("LTV") < 5)
        & (col("LOAN") > 0)
        & (col("VALUE") > 0)
    )

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low'  0.60 -< 0.80 = 'Medium'
#                         0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low'  30 -< 40 = 'Medium'
#                         40 -< 50 = 'High'  50 - high = 'Very High';
#       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'
#                         4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk'  3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk'  7 - high = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each format is expressed as a
# reusable function returning a when()/otherwise() Column expression,
# which is then applied in the DATA step below. A missing input
# yields a missing (null) category, matching the SAS "if X ne ." guard.
# ------------------------------------------------------------------


def ltvRiskFormat(column):
    return when(column.isNull(), lit(None)) \
        .when(column < 0.60, lit("Low")) \
        .when(column < 0.80, lit("Medium")) \
        .otherwise(lit("High"))


def dtiRiskFormat(column):
    return when(column.isNull(), lit(None)) \
        .when(column < 30, lit("Low")) \
        .when(column < 40, lit("Medium")) \
        .when(column < 50, lit("High")) \
        .otherwise(lit("Very High"))


def delinqRiskFormat(column):
    return when(column.isNull(), lit(None)) \
        .when(column == 0, lit("None")) \
        .when(column == 1, lit("Low")) \
        .when(column <= 3, lit("Medium")) \
        .otherwise(lit("High"))


def riskScoreFormat(column):
    return when(column < 3, lit("Low Risk")) \
        .when(column < 5, lit("Medium Risk")) \
        .when(column < 7, lit("High Risk")) \
        .otherwise(lit("Very High Risk"))


riskFormats = {
    "ltv_risk": "low-<0.60='Low' 0.60-<0.80='Medium' 0.80-high='High'",
    "dti_risk": "low-<30='Low' 30-<40='Medium' 40-<50='High' 50-high='Very High'",
    "delinq_risk": "0='None' 1='Low' 2-3='Medium' 4-high='High'",
    "risk_score": "low-<3='Low Risk' 3-<5='Medium Risk' 5-<7='High Risk' "
                  "7-high='Very High Risk'",
}

print("=" * 60)
print("Risk Bucket Formats (equivalent to PROC FORMAT)")
print("=" * 60)
for formatName, definition in riskFormats.items():
    print(f"  {formatName:12s} -> {definition}")

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       length LTV_RISK_CAT $6 DTI_RISK_CAT $9 DELINQ_RISK_CAT $6;
#       if LTV ne . then do; ... LTV_RISK_CAT = 'Low'/'Medium'/'High'; end;
#       if DEBTINC ne . then do; ... DTI_RISK_CAT = ...; end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT = ...; end;
#       RISK_SCORE = 0;
#       if LTV ne . then do;      /* LTV component (0-3 points) */
#           if LTV >= 0.80 then RISK_SCORE + 3;
#           else if LTV >= 0.60 then RISK_SCORE + 1.5;
#       end;
#       if DEBTINC ne . then do;  /* DTI component (0-3 points) */
#           if DEBTINC >= 50 then RISK_SCORE + 3;
#           else if DEBTINC >= 40 then RISK_SCORE + 2;
#           else if DEBTINC >= 30 then RISK_SCORE + 1;
#       end;
#       if DELINQ ne . then do;   /* Delinquency component (0-2 points) */
#           if DELINQ >= 4 then RISK_SCORE + 2;
#           else if DELINQ >= 2 then RISK_SCORE + 1.5;
#           else if DELINQ = 1 then RISK_SCORE + 0.5;
#       end;
#       if DEROG ne . then do;    /* Derogatory component (0-2 points) */
#           if DEROG >= 3 then RISK_SCORE + 2;
#           else if DEROG >= 1 then RISK_SCORE + 1;
#       end;
#       length RISK_SEGMENT $14;
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#   run;
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

home_equity_risk = home_equity_final \
    .withColumn("LTV_RISK_CAT", ltvRiskFormat(col("LTV"))) \
    .withColumn("DTI_RISK_CAT", dtiRiskFormat(col("DEBTINC"))) \
    .withColumn("DELINQ_RISK_CAT", delinqRiskFormat(col("DELINQ"))) \
    .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore) \
    .withColumn("RISK_SEGMENT", riskScoreFormat(col("RISK_SCORE")))

# SAS equivalent:
#   label LTV_RISK_CAT = "LTV Risk Category"
#         DTI_RISK_CAT = "Debt-to-Income Risk Category"
#         DELINQ_RISK_CAT = "Delinquency Risk Category"
#         RISK_SCORE = "Composite Risk Score (0-10)"
#         RISK_SEGMENT = "Risk Segment";
#
# Note: PySpark does not have native column labels like SAS.
# We document them as metadata in a dictionary.
riskColumnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Column Labels (SAS-style variable labels)")
print("=" * 60)
for columnName, label in riskColumnLabels.items():
    print(f"  {columnName:16s} -> {label}")

print(f"\nRows in home_equity_risk: {home_equity_risk.count()}")
home_equity_risk.select(
    "BAD", "LOAN", "LTV", "DEBTINC", "DELINQ", "DEROG",
    "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT",
    "RISK_SCORE", "RISK_SEGMENT"
).show(10, truncate=False)

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from the table and from
# the percentage denominator, so null categories are filtered out.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment")
print("=" * 60)
freqVars = ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]
for freqVar in freqVars:
    nonMissing = home_equity_risk.filter(col(freqVar).isNotNull())
    totalCount = nonMissing.count()
    print(f"\nFrequency of {freqVar}")
    nonMissing.groupBy(freqVar) \
        .agg(count("*").alias("Frequency")) \
        .withColumn("Percent", col("Frequency") / lit(totalCount) * 100) \
        .orderBy(freqVar) \
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
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment")
print("=" * 60)
meansVars = ["BAD", "LOAN", "LTV", "DEBTINC"]
meansAggs = []
for meansVar in meansVars:
    meansAggs.extend([
        count(meansVar).alias(f"N_{meansVar}"),
        mean(meansVar).alias(f"Mean_{meansVar}"),
        stddev(meansVar).alias(f"Std_{meansVar}"),
    ])
home_equity_risk.groupBy("RISK_SEGMENT") \
    .agg(*meansAggs) \
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
# Note: A three-way PROC FREQ table produces one LTV_RISK_CAT x DTI_RISK_CAT
# cell per BAD value; with norow/nocol/nopercent only the counts remain.
# Represented here as a pivot of BAD across the LTV x DTI combinations.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("=" * 60)
home_equity_risk \
    .filter(col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull()) \
    .groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .agg(count("*")) \
    .withColumnRenamed("0", "BAD_0_Count") \
    .withColumnRenamed("1", "BAD_1_Count") \
    .na.fill(0, ["BAD_0_Count", "BAD_1_Count"]) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
spark.stop()
