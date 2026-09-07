"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
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
# Load input data and re-derive work.home_equity_final
# SAS equivalent:
#   set work.home_equity_final;
#
# Note: In SAS the WORK library persists between programs, so this
# program reads the output of 02_data_cleaning.sas. Each PySpark
# script starts from the raw CSV instead, so the prerequisite
# columns/filters from 02_data_cleaning.sas are re-derived here:
#   LTV = MORTDUE / VALUE when VALUE ne . and MORTDUE ne . and VALUE > 0
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfFinal = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() &
        col("MORTDUE").isNotNull() &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5)
).filter(
    col("LOAN") > 0
).filter(
    col("VALUE") > 0
)

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low'  0.60 -< 0.80 = 'Medium'  0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low'  30 -< 40 = 'Medium'  40 -< 50 = 'High'  50 - high = 'Very High';
#       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'  4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk'  3 -< 5 = 'Medium Risk'  5 -< 7 = 'High Risk'  7 - high = 'Very High Risk';
#   run;
#
# Mapping:
#   PROC FORMAT value ranges -> Python functions returning when() chains.
#   The formats are applied in Step 2 to produce the *_RISK_CAT columns.
# ------------------------------------------------------------------
def ltvRiskFormat(c):
    return when(c < 0.60, lit("Low")) \
        .when(c < 0.80, lit("Medium")) \
        .otherwise(lit("High"))


def dtiRiskFormat(c):
    return when(c < 30, lit("Low")) \
        .when(c < 40, lit("Medium")) \
        .when(c < 50, lit("High")) \
        .otherwise(lit("Very High"))


def delinqRiskFormat(c):
    return when(c == 0, lit("None")) \
        .when(c == 1, lit("Low")) \
        .when(c <= 3, lit("Medium")) \
        .otherwise(lit("High"))


def riskScoreFormat(c):
    return when(c < 3, lit("Low Risk")) \
        .when(c < 5, lit("Medium Risk")) \
        .when(c < 7, lit("High Risk")) \
        .otherwise(lit("Very High Risk"))


riskFormats = {
    "ltv_risk": "low-<0.60='Low', 0.60-<0.80='Medium', 0.80-high='High'",
    "dti_risk": "low-<30='Low', 30-<40='Medium', 40-<50='High', 50-high='Very High'",
    "delinq_risk": "0='None', 1='Low', 2-3='Medium', 4-high='High'",
    "risk_score": "low-<3='Low Risk', 3-<5='Medium Risk', 5-<7='High Risk', 7-high='Very High Risk'",
}

print("=" * 60)
print("Risk Bucket Formats (equivalent to PROC FORMAT)")
print("=" * 60)
for name, definition in riskFormats.items():
    print(f"  {name:12s} -> {definition}")

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       length LTV_RISK_CAT $6 DTI_RISK_CAT $9 DELINQ_RISK_CAT $6;
#       if LTV ne . then do; ... LTV_RISK_CAT ...; end;
#       if DEBTINC ne . then do; ... DTI_RISK_CAT ...; end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT ...; end;
#       RISK_SCORE = 0;
#       if LTV ne . then do;
#           if LTV >= 0.80 then RISK_SCORE + 3; else if LTV >= 0.60 then RISK_SCORE + 1.5; end;
#       if DEBTINC ne . then do;
#           if DEBTINC >= 50 then +3; else if >= 40 then +2; else if >= 30 then +1; end;
#       if DELINQ ne . then do;
#           if DELINQ >= 4 then +2; else if DELINQ >= 2 then +1.5; else if DELINQ = 1 then +0.5; end;
#       if DEROG ne . then do;
#           if DEROG >= 3 then +2; else if DEROG >= 1 then +1; end;
#       length RISK_SEGMENT $14;
#       if RISK_SCORE < 3 then 'Low Risk'; else if < 5 'Medium Risk';
#       else if < 7 'High Risk'; else 'Very High Risk';
#       label ...;
#   run;
#
# Mapping:
#   if X ne . then do ... end  -> when(X.isNull(), None/0) guard first
#   (unassigned char var stays blank/missing in SAS -> null here)
#   RISK_SCORE accumulation     -> sum of per-component when() expressions
#   label                       -> columnLabels metadata dict (printed)
# ------------------------------------------------------------------
ltvScore = when(col("LTV").isNull(), lit(0)) \
    .when(col("LTV") >= 0.80, lit(3)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0))

dtiScore = when(col("DEBTINC").isNull(), lit(0)) \
    .when(col("DEBTINC") >= 50, lit(3)) \
    .when(col("DEBTINC") >= 40, lit(2)) \
    .when(col("DEBTINC") >= 30, lit(1)) \
    .otherwise(lit(0))

delinqScore = when(col("DELINQ").isNull(), lit(0)) \
    .when(col("DELINQ") >= 4, lit(2)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0))

derogScore = when(col("DEROG").isNull(), lit(0)) \
    .when(col("DEROG") >= 3, lit(2)) \
    .when(col("DEROG") >= 1, lit(1)) \
    .otherwise(lit(0))

dfRisk = dfFinal.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None)).otherwise(ltvRiskFormat(col("LTV")))
).withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None)).otherwise(dtiRiskFormat(col("DEBTINC")))
).withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None)).otherwise(delinqRiskFormat(col("DELINQ")))
).withColumn(
    "RISK_SCORE",
    ltvScore + dtiScore + delinqScore + derogScore
).withColumn(
    "RISK_SEGMENT",
    riskScoreFormat(col("RISK_SCORE"))
)

columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Column Labels (SAS-style variable labels)")
print("=" * 60)
for c, label in columnLabels.items():
    print(f"  {c:16s} -> {label}")

print(f"\nNumber of rows in work.home_equity_risk: {dfRisk.count()}")

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#   title;
#
# Mapping:
#   PROC FREQ one-way tables -> groupBy().count() with a Percent column
#   (PROC FREQ excludes missing values from the table by default)
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)
for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    nonMissing = dfRisk.filter(col(c).isNotNull())
    total = nonMissing.count()
    print(f"\nFrequency table: {c}")
    nonMissing.groupBy(c).agg(count("*").alias("Frequency")) \
        .withColumn("Percent", (col("Frequency") / total) * 100) \
        .orderBy(c) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#   title;
#
# Mapping:
#   PROC MEANS with CLASS -> groupBy(class).agg(count, mean, stddev)
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)
meansAggs = []
for v in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(col(v)).alias(f"{v}_N"),
        mean(col(v)).alias(f"{v}_Mean"),
        stddev(col(v)).alias(f"{v}_Std"),
    ]
dfRisk.groupBy("RISK_SEGMENT").agg(*meansAggs) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#   title;
#
# Mapping:
#   three-way PROC FREQ -> groupBy(LTV_RISK_CAT, DTI_RISK_CAT).pivot(BAD).count()
#   (frequencies only, matching norow nocol nopercent)
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ crosstab)")
print("=" * 60)
dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull()
).groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "BAD_0") \
    .withColumnRenamed("1", "BAD_1") \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
spark.stop()
