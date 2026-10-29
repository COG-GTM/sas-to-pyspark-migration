"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas

This script is self-contained: it loads data/home_equity.csv and reproduces
the upstream work.home_equity_final transforms from sas/02_data_cleaning.sas
before applying the risk segmentation logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, round as spark_round, sum as spark_sum
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load source data and reproduce upstream cleaning
# SAS equivalent (sas/01_data_loading.sas, sas/02_data_cleaning.sas):
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
#
#   data work.home_equity_imputed;   /* array missing-value flags */
#       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#       array num_flags{8} LOAN_MISS ... NINQ_MISS;
#       do i = 1 to 8; ... end;
#   run;
#
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
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
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY", initcap(col("CITY"))
)

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFinal = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
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
# Note: PySpark has no PROC FORMAT. Each value format is expressed as a
# reusable when/otherwise chain. Missing inputs map to null, matching the
# SAS DATA step (character variables are left blank when the source is .).
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
#       if LTV ne . then do;
#           if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#           else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#       end;
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

# SAS labels documented as metadata (PySpark has no native column labels)
columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("=" * 60)
print("Risk Segment Variables (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:16s} -> {label}")
print(f"Number of rows in work.home_equity_risk: {dfRisk.count()}")

dfRisk.cache()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#
# Note: Like PROC FREQ, percentages exclude missing values, and the
# missing frequency is reported separately.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ / nocum)")
print("=" * 60)
for freqVar in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    dfNonMissing = dfRisk.filter(col(freqVar).isNotNull())
    nonMissingTotal = dfNonMissing.count()
    missingCount = dfRisk.count() - nonMissingTotal

    print(f"\n{freqVar} ({columnLabels[freqVar]})")
    dfNonMissing.groupBy(freqVar).agg(
        count(lit(1)).alias("Frequency")
    ).withColumn(
        "Percent",
        spark_round(col("Frequency") / lit(nonMissingTotal) * 100, 2)
    ).orderBy(freqVar).show(truncate=False)
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
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS / CLASS)")
print("=" * 60)
meansAggs = [count(lit(1)).alias("N_Obs")]
for varName in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(varName).alias(f"N_{varName}"),
        spark_round(mean(varName), 4).alias(f"Mean_{varName}"),
        spark_round(stddev(varName), 4).alias(f"Std_{varName}"),
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
#
# Note: A three-way PROC FREQ table produces one DTI_RISK_CAT x BAD
# frequency table per LTV_RISK_CAT level. Rows with a missing value in
# any of the three variables are excluded, as in SAS.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ A*B*C)")
print("=" * 60)
dfCross = dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() &
    col("DTI_RISK_CAT").isNotNull() &
    col("BAD").isNotNull()
)
ltvLevels = sorted(
    row["LTV_RISK_CAT"]
    for row in dfCross.select("LTV_RISK_CAT").distinct().collect()
)
for ltvLevel in ltvLevels:
    print(f"\nTable of DTI_RISK_CAT by BAD -- Controlling for LTV_RISK_CAT={ltvLevel}")
    dfCross.filter(col("LTV_RISK_CAT") == ltvLevel) \
        .groupBy("DTI_RISK_CAT") \
        .pivot("BAD", [0, 1]) \
        .count() \
        .na.fill(0, ["0", "1"]) \
        .withColumnRenamed("0", "BAD_0") \
        .withColumnRenamed("1", "BAD_1") \
        .withColumn("Total", col("BAD_0") + col("BAD_1")) \
        .orderBy("DTI_RISK_CAT") \
        .show(truncate=False)

# Overall default rate per LTV x DTI cell (summary of the crosstab above)
print("\nDefault rate (mean of BAD) by LTV_RISK_CAT x DTI_RISK_CAT")
dfCross.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT").agg(
    count(lit(1)).alias("N"),
    spark_sum("BAD").alias("Defaults"),
    spark_round(mean("BAD"), 4).alias("Default_Rate")
).orderBy("LTV_RISK_CAT", "DTI_RISK_CAT").show(truncate=False)

# Clean up
spark.stop()
