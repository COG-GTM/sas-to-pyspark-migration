"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas

Self-contained: loads data/home_equity.csv and reproduces the
sas/02_data_cleaning.sas transforms inline to build HOME_EQUITY_FINAL
(the SAS work.home_equity_final dataset this program reads).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, concat_ws
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load source data and reproduce 02_data_cleaning inline
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
#   data work.home_equity_clean;  ... LTV, LOAN_OUTCOME, propcase(CITY)
#   data work.home_equity_imputed; ... <VAR>_MISS flags via ARRAY
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

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

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Input: HOME_EQUITY_FINAL (02_data_cleaning reproduced inline)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low' 0.60 -< 0.80 = 'Medium'
#                         0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low' 30 -< 40 = 'Medium'
#                         40 -< 50 = 'High' 50 - high = 'Very High';
#       value delinq_risk 0 = 'None' 1 = 'Low' 2 - 3 = 'Medium'
#                         4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk' 3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk' 7 - high = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each value format is expressed as a
# when(...) chain below; a null input stays null (SAS blank category).
# ------------------------------------------------------------------
ltvRiskCat = when(col("LTV").isNull(), lit(None)) \
    .when(col("LTV") < 0.60, lit("Low")) \
    .when(col("LTV") < 0.80, lit("Medium")) \
    .otherwise(lit("High"))

dtiRiskCat = when(col("DEBTINC").isNull(), lit(None)) \
    .when(col("DEBTINC") < 30, lit("Low")) \
    .when(col("DEBTINC") < 40, lit("Medium")) \
    .when(col("DEBTINC") < 50, lit("High")) \
    .otherwise(lit("Very High"))

delinqRiskCat = when(col("DELINQ").isNull(), lit(None)) \
    .when(col("DELINQ") == 0, lit("None")) \
    .when(col("DELINQ") == 1, lit("Low")) \
    .when(col("DELINQ") <= 3, lit("Medium")) \
    .otherwise(lit("High"))

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do; ... LTV_RISK_CAT ... end;
#       if DEBTINC ne . then do; ... DTI_RISK_CAT ... end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT ... end;
#
#       RISK_SCORE = 0;
#       /* LTV (0-3), DTI (0-3), DELINQ (0-2), DEROG (0-2) points */
#       ...
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
    )

# SAS-style variable labels documented as metadata
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Variable Labels (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:16s} -> {label}")

dfRisk.cache()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#
# Note: PROC FREQ excludes missing values from the table and reports them
# as "Frequency Missing"; percentages are of the non-missing total.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)
for varName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    dfNonMissing = dfRisk.filter(col(varName).isNotNull())
    nonMissingTotal = dfNonMissing.count()
    missingCount = dfRisk.count() - nonMissingTotal
    print(f"\n{varName}")
    dfNonMissing.groupBy(varName) \
        .agg(count("*").alias("Frequency")) \
        .withColumn("Percent", col("Frequency") * 100.0 / lit(nonMissingTotal)) \
        .orderBy(varName) \
        .show(truncate=False)
    print(f"Frequency Missing = {missingCount}")

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
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)
meansAggs = []
for varName in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(col(varName)).alias(f"{varName}_N"),
        mean(col(varName)).alias(f"{varName}_MEAN"),
        stddev(col(varName)).alias(f"{varName}_STD"),
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
# Note: The 3-way SAS table (one DTI x BAD layer per LTV category) is
# flattened to one row per LTV/DTI combination with BAD as columns
# (counts only, matching norow nocol nopercent). Rows with a missing value
# in any table variable are excluded, as in SAS.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ)")
print("=" * 60)
dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() &
    col("DTI_RISK_CAT").isNotNull() &
    col("BAD").isNotNull()
).withColumn(
    "LTV_X_DTI",
    concat_ws(" x ", col("LTV_RISK_CAT"), col("DTI_RISK_CAT"))
).crosstab("LTV_X_DTI", "BAD") \
    .withColumnRenamed("LTV_X_DTI_BAD", "LTV_RISK_CAT x DTI_RISK_CAT") \
    .orderBy("LTV_RISK_CAT x DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
dfRisk.unpersist()
spark.stop()
