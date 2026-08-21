"""
PySpark Script: 04_risk_segmentation.py
Purpose: Segment the HOME_EQUITY loan portfolio by risk
         (risk buckets, composite risk score, default rates by segment)
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, round as sparkRound
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# Load the source dataset
# SAS equivalent: set work.home_equity;
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 0: Recreate the cleaned dataset from 02_data_cleaning
#         so this script is self-contained
# SAS equivalent: set work.home_equity_final;
#   (LTV = MORTDUE / VALUE, LOAN_OUTCOME from BAD, critical-field and
#    range filters applied in 02_data_cleaning.sas)
# ------------------------------------------------------------------
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
# Step 1 & 2: Risk bucket categories
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60='Low'  0.60 -< 0.80='Medium'
#                         0.80 - high='High';
#       value dti_risk    low -< 30='Low' 30 -< 40='Medium'
#                         40 -< 50='High' 50 - high='Very High';
#       value delinq_risk 0='None' 1='Low' 2-3='Medium' 4-high='High';
#   run;
#
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do;
#           if LTV < 0.60 then LTV_RISK_CAT = 'Low';
#           else if LTV < 0.80 then LTV_RISK_CAT = 'Medium';
#           else LTV_RISK_CAT = 'High';
#       end;
#       ... same pattern for DEBTINC and DELINQ ...
#
# PROC FORMAT value ranges / IF-THEN chains become when().otherwise()
# chains; the SAS "ne ." guard becomes an isNull() branch yielding null.
# ------------------------------------------------------------------
dfRisk = dfFinal \
    .withColumn(
        "LTV_RISK_CAT",
        when(col("LTV").isNull(), lit(None))
        .when(col("LTV") < 0.60, lit("Low"))
        .when(col("LTV") < 0.80, lit("Medium"))
        .otherwise(lit("High"))
    ) \
    .withColumn(
        "DTI_RISK_CAT",
        when(col("DEBTINC").isNull(), lit(None))
        .when(col("DEBTINC") < 30, lit("Low"))
        .when(col("DEBTINC") < 40, lit("Medium"))
        .when(col("DEBTINC") < 50, lit("High"))
        .otherwise(lit("Very High"))
    ) \
    .withColumn(
        "DELINQ_RISK_CAT",
        when(col("DELINQ").isNull(), lit(None))
        .when(col("DELINQ") == 0, lit("None"))
        .when(col("DELINQ") == 1, lit("Low"))
        .when(col("DELINQ") <= 3, lit("Medium"))
        .otherwise(lit("High"))
    )

# ------------------------------------------------------------------
# Composite risk score (0-10 scale)
# SAS equivalent:
#   RISK_SCORE = 0;
#   if LTV ne . then do;
#       if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#       else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#   end;
#   ... DTI (0-3), DELINQ (0-2), DEROG (0-2) components ...
#
# Each SAS component becomes a when() expression; a null component
# contributes 0, matching the SAS "ne ." guard.
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

# ------------------------------------------------------------------
# Risk segment label
# SAS equivalent:
#   value risk_score low -< 3='Low Risk' 3 -< 5='Medium Risk'
#                    5 -< 7='High Risk'  7 - high='Very High Risk';
#   if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk'; else if ...
# ------------------------------------------------------------------
dfRisk = dfRisk \
    .withColumn(
        "RISK_SCORE",
        ltvScore + dtiScore + delinqScore + derogScore
    ) \
    .withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk"))
    )

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT
#              / nocum;
#   run;
#
# PROC FREQ -> groupBy().count() with a percent column.
# ------------------------------------------------------------------
totalRows = dfRisk.count()

print("=" * 60)
print("Distribution of Loans by Risk Segment")
print("(equivalent to PROC FREQ)")
print("=" * 60)

for freqCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT",
                "DELINQ_RISK_CAT"]:
    dfRisk.groupBy(freqCol) \
        .count() \
        .withColumn(
            "Percent",
            sparkRound(col("count") * 100.0 / totalRows, 2)
        ) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#
# PROC MEANS with CLASS -> groupBy().agg().
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment")
print("(equivalent to PROC MEANS with CLASS)")
print("=" * 60)

aggExprs = []
for statCol in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    aggExprs += [
        count(col(statCol)).alias(f"N_{statCol}"),
        sparkRound(mean(col(statCol)), 4).alias(f"Mean_{statCol}"),
        sparkRound(stddev(col(statCol)), 4).alias(f"Std_{statCol}"),
    ]

dfRisk.groupBy("RISK_SEGMENT").agg(*aggExprs) \
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
# Multi-way PROC FREQ -> groupBy().pivot().count() style cross-tab.
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("(equivalent to multi-way PROC FREQ)")
print("=" * 60)

dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD") \
    .count() \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Preview the segmented dataset
# SAS equivalent: proc print data=work.home_equity_risk(obs=10);
# ------------------------------------------------------------------
print("=" * 60)
print("First 10 Segmented Observations")
print("=" * 60)
dfRisk.select(
    "BAD", "LOAN", "LTV", "DEBTINC", "DELINQ", "DEROG",
    "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT",
    "RISK_SCORE", "RISK_SEGMENT"
).show(10, truncate=False)

# Clean up
spark.stop()
