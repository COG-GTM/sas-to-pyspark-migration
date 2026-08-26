"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    initcap,
    lit,
    mean,
    round as spark_round,
    stddev,
    when,
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Prepare the input dataset used by this standalone script
# SAS equivalent: set work.home_equity_final;
#
# work.home_equity_final is produced by sas/02_data_cleaning.sas.
# Reproduce its required transformations here so this script can run
# independently from the repository root.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfClean = df.withColumn(
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
).withColumn(
    "CITY",
    initcap(col("CITY"))
)

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) &
    (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Input Dataset: work.home_equity_final")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk
#           low  -< 0.60 = 'Low'
#           0.60 -< 0.80 = 'Medium'
#           0.80 - high   = 'High';
#       value dti_risk ...;
#       value delinq_risk ...;
#       value risk_score ...;
#   run;
#
# PySpark has no PROC FORMAT catalog. Document the formats as
# metadata and apply the same ordered ranges with when() in Step 2.
# ------------------------------------------------------------------
riskFormats = {
    "ltv_risk": [
        ("LTV < 0.60", "Low"),
        ("0.60 <= LTV < 0.80", "Medium"),
        ("LTV >= 0.80", "High"),
    ],
    "dti_risk": [
        ("DEBTINC < 30", "Low"),
        ("30 <= DEBTINC < 40", "Medium"),
        ("40 <= DEBTINC < 50", "High"),
        ("DEBTINC >= 50", "Very High"),
    ],
    "delinq_risk": [
        ("DELINQ = 0", "None"),
        ("DELINQ = 1", "Low"),
        ("2 <= DELINQ <= 3", "Medium"),
        ("DELINQ >= 4", "High"),
    ],
    "risk_score": [
        ("RISK_SCORE < 3", "Low Risk"),
        ("3 <= RISK_SCORE < 5", "Medium Risk"),
        ("5 <= RISK_SCORE < 7", "High Risk"),
        ("RISK_SCORE >= 7", "Very High Risk"),
    ],
}

print("\n" + "=" * 60)
print("Risk Formats (equivalent to PROC FORMAT)")
print("=" * 60)
for formatName, ranges in riskFormats.items():
    print(f"\n{formatName}")
    for condition, label in ranges:
        print(f"  {condition:28s} -> {label}")

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do;
#           if LTV < 0.60 then LTV_RISK_CAT = 'Low';
#           else if LTV < 0.80 then LTV_RISK_CAT = 'Medium';
#           else LTV_RISK_CAT = 'High';
#       end;
#       ...
#       RISK_SCORE = 0;
#       ...
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#   run;
# ------------------------------------------------------------------
ltvScore = (
    when(col("LTV").isNull(), lit(0.0))
    .when(col("LTV") >= 0.80, lit(3.0))
    .when(col("LTV") >= 0.60, lit(1.5))
    .otherwise(lit(0.0))
)

dtiScore = (
    when(col("DEBTINC").isNull(), lit(0.0))
    .when(col("DEBTINC") >= 50, lit(3.0))
    .when(col("DEBTINC") >= 40, lit(2.0))
    .when(col("DEBTINC") >= 30, lit(1.0))
    .otherwise(lit(0.0))
)

delinqScore = (
    when(col("DELINQ").isNull(), lit(0.0))
    .when(col("DELINQ") >= 4, lit(2.0))
    .when(col("DELINQ") >= 2, lit(1.5))
    .when(col("DELINQ") == 1, lit(0.5))
    .otherwise(lit(0.0))
)

derogScore = (
    when(col("DEROG").isNull(), lit(0.0))
    .when(col("DEROG") >= 3, lit(2.0))
    .when(col("DEROG") >= 1, lit(1.0))
    .otherwise(lit(0.0))
)

dfRisk = dfFinal.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
).withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
).withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
).withColumn(
    "RISK_SCORE",
    ltvScore + dtiScore + delinqScore + derogScore
).withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Column Labels (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:18s} -> {label}")

print("\nFirst 10 Risk-Segmented Loans")
dfRisk.select(
    "BAD",
    "LOAN",
    "LTV",
    "DEBTINC",
    "DELINQ",
    "DEROG",
    "LTV_RISK_CAT",
    "DTI_RISK_CAT",
    "DELINQ_RISK_CAT",
    "RISK_SCORE",
    "RISK_SEGMENT",
).show(10, truncate=False)

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT
#              DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Distribution of Loans by Risk Segment")
print("(equivalent to PROC FREQ)")
print("=" * 60)

for segmentCol in [
    "RISK_SEGMENT",
    "LTV_RISK_CAT",
    "DTI_RISK_CAT",
    "DELINQ_RISK_CAT",
]:
    nonMissingSegments = dfRisk.filter(col(segmentCol).isNotNull())
    totalCount = nonMissingSegments.count()

    print(f"\n--- {segmentCol} ---")
    nonMissingSegments.groupBy(segmentCol) \
        .agg(
            count("*").alias("Frequency"),
            spark_round(
                count("*") / lit(totalCount) * 100,
                2
            ).alias("Percent")
        ) \
        .orderBy(segmentCol) \
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
print("(equivalent to PROC MEANS with N, MEAN, and STD)")
print("=" * 60)

dfRisk.groupBy("RISK_SEGMENT") \
    .agg(
        count("BAD").alias("BAD_N"),
        spark_round(mean("BAD"), 4).alias("BAD_Mean"),
        spark_round(stddev("BAD"), 4).alias("BAD_Std"),
        count("LOAN").alias("LOAN_N"),
        spark_round(mean("LOAN"), 2).alias("LOAN_Mean"),
        spark_round(stddev("LOAN"), 2).alias("LOAN_Std"),
        count("LTV").alias("LTV_N"),
        spark_round(mean("LTV"), 4).alias("LTV_Mean"),
        spark_round(stddev("LTV"), 4).alias("LTV_Std"),
        count("DEBTINC").alias("DEBTINC_N"),
        spark_round(mean("DEBTINC"), 2).alias("DEBTINC_Mean"),
        spark_round(stddev("DEBTINC"), 2).alias("DEBTINC_Std"),
    ) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD
#              / norow nocol nopercent;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("(equivalent to three-way PROC FREQ)")
print("=" * 60)

dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() &
    col("DTI_RISK_CAT").isNotNull() &
    col("BAD").isNotNull()
).groupBy(
    "LTV_RISK_CAT",
    "DTI_RISK_CAT",
    "BAD",
).agg(
    count("*").alias("Frequency")
).orderBy(
    "LTV_RISK_CAT",
    "DTI_RISK_CAT",
    "BAD",
).show(100, truncate=False)

# Clean up
spark.stop()
