"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
    - Define risk buckets (PROC FORMAT equivalent)
    - Create composite risk scores
    - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, round as spark_round
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild the upstream input dataset (work.home_equity_final)
# SAS equivalent (sas/01_data_loading.sas + sas/02_data_cleaning.sas):
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#
#   data work.home_equity_imputed;   /* <COL>_MISS flags via ARRAY */
#   data work.home_equity_filtered;  /* LOAN, VALUE, BAD ne .     */
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: SAS chains off work.* datasets from earlier programs. This
# script is self-contained, so the required upstream steps are
# reproduced inline from the source CSV.
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
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY",
    initcap(col("CITY"))
)

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFinal = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Input Dataset (equivalent to work.home_equity_final)")
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
#       value dti_risk
#           low  -< 30  = 'Low'
#           30   -< 40  = 'Medium'
#           40   -< 50  = 'High'
#           50   - high  = 'Very High';
#       value delinq_risk
#           0         = 'None'
#           1         = 'Low'
#           2 - 3     = 'Medium'
#           4 - high  = 'High';
#       value risk_score
#           low  -< 3 = 'Low Risk'
#           3    -< 5 = 'Medium Risk'
#           5    -< 7 = 'High Risk'
#           7 - high  = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. The buckets are documented here
# and implemented as when().otherwise() chains in Step 2.
# ------------------------------------------------------------------
riskFormats = {
    "ltv_risk": [
        ("low  -< 0.60", "Low"),
        ("0.60 -< 0.80", "Medium"),
        ("0.80 -  high", "High"),
    ],
    "dti_risk": [
        ("low -< 30", "Low"),
        ("30  -< 40", "Medium"),
        ("40  -< 50", "High"),
        ("50  - high", "Very High"),
    ],
    "delinq_risk": [
        ("0", "None"),
        ("1", "Low"),
        ("2 - 3", "Medium"),
        ("4 - high", "High"),
    ],
    "risk_score": [
        ("low -< 3", "Low Risk"),
        ("3   -< 5", "Medium Risk"),
        ("5   -< 7", "High Risk"),
        ("7   - high", "Very High Risk"),
    ],
}

print("\n" + "=" * 60)
print("Risk Bucket Definitions (equivalent to PROC FORMAT)")
print("=" * 60)
for formatName, buckets in riskFormats.items():
    print(f"  {formatName}")
    for rangeText, bucketLabel in buckets:
        print(f"    {rangeText:12s} = '{bucketLabel}'")

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
#       if DEBTINC ne . then do; ... DTI_RISK_CAT ... end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT ... end;
#
#       RISK_SCORE = 0;
#       if LTV ne . then do;
#           if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#           else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#       end;
#       ... DEBTINC (0-3), DELINQ (0-2), DEROG (0-2) components ...
#
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#   run;
#
# Note: SAS leaves a category blank when its input is missing; here
# it is null. Missing inputs contribute 0 points to RISK_SCORE.
# ------------------------------------------------------------------
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
)

# Composite risk score (0-10 scale)
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

dfRisk = dfRisk.withColumn(
    "RISK_SCORE",
    ltvScore + dtiScore + delinqScore + derogScore
).withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

# SAS equivalent: label LTV_RISK_CAT = "LTV Risk Category" ...;
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

dfRisk.cache()

print("\n" + "=" * 60)
print("Risk Segment Variables (work.home_equity_risk)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:16s} -> {label}")
print(f"Number of rows: {dfRisk.count()}")
dfRisk.select(
    "BAD", "LTV", "DEBTINC", "DELINQ", "DEROG",
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
# Note: Like PROC FREQ, percentages exclude missing values and the
# missing count is reported separately.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment")
print("=" * 60)
for colName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    nonMissing = dfRisk.filter(col(colName).isNotNull())
    nonMissingCount = nonMissing.count()
    missingCount = dfRisk.count() - nonMissingCount

    print(f"\n{colName} ({columnLabels[colName]})")
    nonMissing.groupBy(colName) \
        .agg(count("*").alias("Frequency")) \
        .withColumn(
            "Percent",
            spark_round(col("Frequency") / lit(nonMissingCount) * 100, 2)
        ) \
        .orderBy(colName) \
        .show(truncate=False)
    if missingCount > 0:
        print(f"Frequency Missing = {missingCount}")

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#
# Note: mean(BAD) is the default rate. count(col) and stddev (sample
# standard deviation) ignore nulls, matching PROC MEANS N and STD.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment")
print("=" * 60)
meansVars = ["BAD", "LOAN", "LTV", "DEBTINC"]
aggExprs = [count("*").alias("N_Obs")]
for varName in meansVars:
    aggExprs.extend([
        count(varName).alias(f"{varName}_N"),
        spark_round(mean(varName), 4).alias(f"{varName}_Mean"),
        spark_round(stddev(varName), 4).alias(f"{varName}_Std"),
    ])

dfRisk.groupBy("RISK_SEGMENT") \
    .agg(*aggExprs) \
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
# Note: A three-way PROC FREQ table produces one DTI_RISK_CAT x BAD
# table per LTV_RISK_CAT level (frequencies only). Rows with missing
# values in any of the three variables are excluded, as in SAS.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("=" * 60)
dfCross = dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() &
    col("DTI_RISK_CAT").isNotNull() &
    col("BAD").isNotNull()
)

ltvLevels = [
    row["LTV_RISK_CAT"]
    for row in dfCross.select("LTV_RISK_CAT").distinct().orderBy("LTV_RISK_CAT").collect()
]
for tableNum, ltvLevel in enumerate(ltvLevels, start=1):
    print(f"\nTable {tableNum} of DTI_RISK_CAT by BAD")
    print(f"Controlling for LTV_RISK_CAT={ltvLevel}")
    dfCross.filter(col("LTV_RISK_CAT") == ltvLevel) \
        .groupBy("DTI_RISK_CAT") \
        .pivot("BAD", [0, 1]) \
        .count() \
        .na.fill(0) \
        .withColumn("Total", col("0") + col("1")) \
        .orderBy("DTI_RISK_CAT") \
        .show(truncate=False)

dfRisk.unpersist()

# Clean up
spark.stop()
