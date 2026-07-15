"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
Equivalent SAS Program: sas/04_risk_segmentation.sas
- Define risk buckets (SAS PROC FORMAT value maps documented in comments)
- Create composite risk scores
- Analyze default rates by risk segment
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, round as spark_round
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# Load data
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# ------------------------------------------------------------------
# Prerequisite: reproduce the cleaned work.home_equity_final dataset
# inline (originally produced by 02_data_cleaning.sas).
# SAS equivalent:
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#   run;
#   data work.home_equity_final;
#       set work.home_equity_clean;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
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
    )

dfFinal = dfFinal.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull() &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 1: Risk bucket "formats"
# PySpark has no PROC FORMAT; the SAS value maps are documented here and
# applied inline via when(...) chains in Step 2.
# SAS equivalent:
#   proc format;
#       value ltv_risk
#           low  -< 0.60 = 'Low'
#           0.60 -< 0.80 = 'Medium'
#           0.80 - high  = 'High';
#       value dti_risk
#           low  -< 30   = 'Low'
#           30   -< 40   = 'Medium'
#           40   -< 50   = 'High'
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
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       length LTV_RISK_CAT $6 DTI_RISK_CAT $9 DELINQ_RISK_CAT $6;
#       if LTV ne . then do; ... end;              /* LTV_RISK_CAT     */
#       if DEBTINC ne . then do; ... end;          /* DTI_RISK_CAT     */
#       if DELINQ ne . then do; ... end;           /* DELINQ_RISK_CAT  */
#       RISK_SCORE = 0; ... /* four weighted components */
#       length RISK_SEGMENT $14; ...               /* RISK_SEGMENT     */
#       label LTV_RISK_CAT    = "LTV Risk Category"
#             DTI_RISK_CAT    = "Debt-to-Income Risk Category"
#             DELINQ_RISK_CAT = "Delinquency Risk Category"
#             RISK_SCORE      = "Composite Risk Score (0-10)"
#             RISK_SEGMENT    = "Risk Segment";
#   run;
# ------------------------------------------------------------------

# LTV Risk Category (null LTV -> null category)
dfRisk = dfFinal.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
)

# Debt-to-Income Risk Category (null DEBTINC -> null category)
dfRisk = dfRisk.withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
)

# Delinquency Risk Category (null DELINQ -> null category)
dfRisk = dfRisk.withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
)

# Composite risk score (0-10 scale) built from four weighted components.
# Nulls contribute 0 to each component (SAS: "if <var> ne . then do ...").
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
)

# Risk segment label from the composite score
dfRisk = dfRisk.withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

# Cache: reused by the reporting steps below
dfRisk = dfRisk.cache()

# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
totalRows = dfRisk.count()

print("=" * 60)
print("Distribution of Loans by Risk Segment")
print("(equivalent to PROC FREQ / nocum)")
print("=" * 60)
for freqCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\nFrequency of {freqCol}:")
    dfRisk.groupBy(freqCol) \
        .agg(count(lit(1)).alias("Frequency")) \
        .withColumn(
            "Percent",
            spark_round(col("Frequency") / lit(totalRows) * 100, 2)
        ) \
        .orderBy(freqCol) \
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
print("(equivalent to PROC MEANS n mean std, CLASS RISK_SEGMENT)")
print("=" * 60)
aggExprs = []
for statCol in ["BAD", "LOAN", "LTV", "DEBTINC"]:
    aggExprs.append(count(col(statCol)).alias(f"{statCol}_N"))
    aggExprs.append(spark_round(mean(col(statCol)), 4).alias(f"{statCol}_Mean"))
    aggExprs.append(spark_round(stddev(col(statCol)), 4).alias(f"{statCol}_Std"))

dfRisk.groupBy("RISK_SEGMENT") \
    .agg(*aggExprs) \
    .orderBy("RISK_SEGMENT") \
    .show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("(equivalent to PROC FREQ crosstab LTV_RISK_CAT * DTI_RISK_CAT * BAD)")
print("=" * 60)
dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD") \
    .agg(count(lit(1)).alias("Frequency")) \
    .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD") \
    .show(100, truncate=False)

# Clean up
spark.stop()
