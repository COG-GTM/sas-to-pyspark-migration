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
# Step 0: Load source data and rebuild work.home_equity_final
# SAS equivalent:
#   04_risk_segmentation.sas reads work.home_equity_final, produced by
#   01_data_loading.sas and 02_data_cleaning.sas. PySpark scripts are
#   self-contained, so the required 02 cleaning logic is reproduced here:
#
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_imputed;  /* <COL>_MISS flags via ARRAY */
#   data work.home_equity_filtered; if LOAN ne . and VALUE ne . and BAD ne .;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
#   run;
# ------------------------------------------------------------------
dataPath = "data/home_equity.csv"
df = spark.read.csv(dataPath, header=True, inferSchema=True)
print(f"Number of rows (source): {df.count()}")

dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() &
            col("MORTDUE").isNotNull() &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

missFlagColumns = [
    "LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"
]
for colName in missFlagColumns:
    dfClean = dfClean.withColumn(
        f"{colName}_MISS",
        when(col(colName).isNull(), lit(1)).otherwise(lit(0))
    )

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)
print(f"Number of rows (non-null LOAN/VALUE/BAD): {dfFiltered.count()}")

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)
print(f"Number of rows (work.home_equity_final): {dfFinal.count()}")

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
# Note: PySpark has no PROC FORMAT. Each value format becomes a
# when/otherwise chain applied in Step 2.
# ------------------------------------------------------------------

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
#       ... DTI_RISK_CAT, DELINQ_RISK_CAT ...
#       RISK_SCORE = 0;
#       if LTV ne . then do;
#           if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#           else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#       end;
#       ... DTI (0-3), DELINQ (0-2), DEROG (0-2) components ...
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#   run;
#
# Note: a missing input leaves its category null (SAS blank) and adds
# 0 points to RISK_SCORE.
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
    ) \
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
    ) \
    .cache()

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

print(f"\nNumber of rows (work.home_equity_risk): {dfRisk.count()}")
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
# Note: PROC FREQ excludes missing levels from the table and percent
# denominator and reports them as "Frequency Missing".
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)
for colName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    nonMissing = dfRisk.filter(col(colName).isNotNull())
    nonMissingCount = nonMissing.count()
    print(f"\n{colName} ({columnLabels[colName]})")
    nonMissing.groupBy(colName).agg(count(lit(1)).alias("Frequency")) \
        .withColumn(
            "Percent",
            spark_round(col("Frequency") / lit(nonMissingCount) * 100, 2)
        ) \
        .orderBy(colName) \
        .show(truncate=False)
    print(f"Frequency Missing = {dfRisk.count() - nonMissingCount}")

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
for colName in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    meansAggs += [
        count(col(colName)).alias(f"N_{colName}"),
        spark_round(mean(colName), 4).alias(f"Mean_{colName}"),
        spark_round(stddev(colName), 4).alias(f"Std_{colName}"),
    ]
dfRisk.groupBy("RISK_SEGMENT").agg(
    count(lit(1)).alias("N_Obs"), *meansAggs
).orderBy("RISK_SEGMENT").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#
# Note: a three-way table gives a DTI_RISK_CAT x BAD table per LTV_RISK_CAT
# level; here it is one frequency grid with BAD pivoted into columns.
# Rows with a missing category are excluded, as in PROC FREQ.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ)")
print("=" * 60)
dfRisk.filter(
    col("LTV_RISK_CAT").isNotNull() & col("DTI_RISK_CAT").isNotNull()
).groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .pivot("BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "BAD_0") \
    .withColumnRenamed("1", "BAD_1") \
    .withColumn("Total", col("BAD_0") + col("BAD_1")) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
    .show(truncate=False)

# Clean up
spark.stop()
