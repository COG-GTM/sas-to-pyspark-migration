"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT -> when/otherwise)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, mean, stddev, round

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load source data
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#
# The SAS program reads work.home_equity_final (the cleaned dataset).
# Here we load the raw CSV and derive LTV the same way the cleaning
# step does (LTV = MORTDUE / VALUE when VALUE > 0), so this script
# runs standalone.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# LTV (Loan-to-Value) derived column carried over from data cleaning.
# SAS equivalent:
#   if VALUE > 0 and MORTDUE ne . then LTV = MORTDUE / VALUE;
df = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
)

# ------------------------------------------------------------------
# Step 1 & 2: Risk bucket categories (PROC FORMAT + DATA step)
# SAS equivalent (PROC FORMAT value ltv_risk + DATA step IF/THEN):
#   if LTV ne . then do;
#       if LTV < 0.60 then LTV_RISK_CAT = 'Low';
#       else if LTV < 0.80 then LTV_RISK_CAT = 'Medium';
#       else LTV_RISK_CAT = 'High';
#   end;
# SAS leaves LTV_RISK_CAT missing when LTV is missing.
# ------------------------------------------------------------------
df = df.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
)

# Debt-to-Income risk category
# SAS equivalent:
#   if DEBTINC ne . then do;
#       if DEBTINC < 30 then DTI_RISK_CAT = 'Low';
#       else if DEBTINC < 40 then DTI_RISK_CAT = 'Medium';
#       else if DEBTINC < 50 then DTI_RISK_CAT = 'High';
#       else DTI_RISK_CAT = 'Very High';
#   end;
df = df.withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
)

# Delinquency risk category
# SAS equivalent:
#   if DELINQ ne . then do;
#       if DELINQ = 0 then DELINQ_RISK_CAT = 'None';
#       else if DELINQ = 1 then DELINQ_RISK_CAT = 'Low';
#       else if DELINQ <= 3 then DELINQ_RISK_CAT = 'Medium';
#       else DELINQ_RISK_CAT = 'High';
#   end;
df = df.withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
)

# ------------------------------------------------------------------
# Composite risk score (0-10 scale)
# SAS equivalent: RISK_SCORE built additively from four components,
# each guarded by a `ne .` (non-missing) check so missing inputs add 0.
# ------------------------------------------------------------------

# LTV component (0-3 points)
# SAS equivalent:
#   if LTV ne . then do;
#       if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#       else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#   end;
ltvScore = when(col("LTV").isNull(), lit(0)) \
    .when(col("LTV") >= 0.80, lit(3)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0))

# DTI component (0-3 points)
# SAS equivalent:
#   if DEBTINC ne . then do;
#       if DEBTINC >= 50 then RISK_SCORE = RISK_SCORE + 3;
#       else if DEBTINC >= 40 then RISK_SCORE = RISK_SCORE + 2;
#       else if DEBTINC >= 30 then RISK_SCORE = RISK_SCORE + 1;
#   end;
dtiScore = when(col("DEBTINC").isNull(), lit(0)) \
    .when(col("DEBTINC") >= 50, lit(3)) \
    .when(col("DEBTINC") >= 40, lit(2)) \
    .when(col("DEBTINC") >= 30, lit(1)) \
    .otherwise(lit(0))

# Delinquency component (0-2 points)
# SAS equivalent:
#   if DELINQ ne . then do;
#       if DELINQ >= 4 then RISK_SCORE = RISK_SCORE + 2;
#       else if DELINQ >= 2 then RISK_SCORE = RISK_SCORE + 1.5;
#       else if DELINQ = 1 then RISK_SCORE = RISK_SCORE + 0.5;
#   end;
delinqScore = when(col("DELINQ").isNull(), lit(0)) \
    .when(col("DELINQ") >= 4, lit(2)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0))

# Derogatory reports component (0-2 points)
# SAS equivalent:
#   if DEROG ne . then do;
#       if DEROG >= 3 then RISK_SCORE = RISK_SCORE + 2;
#       else if DEROG >= 1 then RISK_SCORE = RISK_SCORE + 1;
#   end;
derogScore = when(col("DEROG").isNull(), lit(0)) \
    .when(col("DEROG") >= 3, lit(2)) \
    .when(col("DEROG") >= 1, lit(1)) \
    .otherwise(lit(0))

df = df.withColumn(
    "RISK_SCORE",
    ltvScore + dtiScore + delinqScore + derogScore
)

# Risk score category (segment label)
# SAS equivalent:
#   if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#   else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#   else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#   else RISK_SEGMENT = 'Very High Risk';
df = df.withColumn(
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
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Distribution of Loans by Risk Segment")
print("=" * 60)
for column in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    print(f"\nFrequency of {column}:")
    df.groupBy(column).count().orderBy(col("count").desc()).show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Average default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment")
print("=" * 60)
df.groupBy("RISK_SEGMENT").agg(
    count("BAD").alias("N"),
    round(mean("BAD"), 4).alias("Mean_BAD"),
    round(stddev("BAD"), 4).alias("Std_BAD"),
    round(mean("LOAN"), 2).alias("Mean_LOAN"),
    round(mean("LTV"), 4).alias("Mean_LTV"),
    round(mean("DEBTINC"), 2).alias("Mean_DEBTINC"),
).orderBy("RISK_SEGMENT").show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk")
print("=" * 60)
df.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT").agg(
    count("BAD").alias("N"),
    round(mean("BAD"), 4).alias("Default_Rate"),
).orderBy("LTV_RISK_CAT", "DTI_RISK_CAT").show(truncate=False)

# Clean up
spark.stop()
