"""
PySpark Script: 04_risk_segmentation.py
Purpose: Segment the HOME_EQUITY loan portfolio by credit risk
Equivalent SAS Program: sas/04_risk_segmentation.sas

Re-migrated from scratch against the SAS program as the source of truth.
The SAS job uses PROC FORMAT value ranges + a DATA step of additive
sub-scores; here those map to when/otherwise chains and column arithmetic.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, mean, stddev
from pyspark.sql.functions import round as spark_round

# Self-contained SparkSession (no shared helper module).
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Load & prepare the input.
# The SAS program reads work.home_equity_final, which carries LTV derived
# upstream (MORTDUE / VALUE). We recompute it here so the script is
# runnable standalone from the raw CSV.
# ------------------------------------------------------------------
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

df = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE"),
    ),
).filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    & (col("LOAN") > 0) & (col("VALUE") > 0)
)

# ------------------------------------------------------------------
# Step 1: Risk bucket categories.
# SAS equivalent (PROC FORMAT value ranges applied in the DATA step):
#   value ltv_risk    low -< 0.60='Low' 0.60 -< 0.80='Medium' 0.80-high='High';
#   value dti_risk    low -< 30='Low' 30 -< 40='Medium' 40 -< 50='High' 50-high='Very High';
#   value delinq_risk 0='None' 1='Low' 2-3='Medium' 4-high='High';
# SAS leaves the category missing when the source variable is missing, so
# each chain guards nulls first (returning NULL) before bucketing.
# ------------------------------------------------------------------
LTV_RISK_CAT = (
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
)

DTI_RISK_CAT = (
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
)

DELINQ_RISK_CAT = (
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
)

# ------------------------------------------------------------------
# Step 2: Additive composite risk score (0-10 scale).
# SAS equivalent: RISK_SCORE starts at 0 and each factor conditionally
# adds its sub-score; a missing factor contributes 0. Each sub-score below
# reproduces one SAS "if/else if" block, and they are summed with column
# arithmetic. Thresholds and weights are copied verbatim from the SAS DATA
# step.
# ------------------------------------------------------------------

# LTV component (0-3 points)
ltv_score = (
    when(col("LTV").isNull(), lit(0.0))
    .when(col("LTV") >= 0.80, lit(3.0))
    .when(col("LTV") >= 0.60, lit(1.5))
    .otherwise(lit(0.0))
)

# Debt-to-income component (0-3 points)
dti_score = (
    when(col("DEBTINC").isNull(), lit(0.0))
    .when(col("DEBTINC") >= 50, lit(3.0))
    .when(col("DEBTINC") >= 40, lit(2.0))
    .when(col("DEBTINC") >= 30, lit(1.0))
    .otherwise(lit(0.0))
)

# Delinquency component (0-2 points)
delinq_score = (
    when(col("DELINQ").isNull(), lit(0.0))
    .when(col("DELINQ") >= 4, lit(2.0))
    .when(col("DELINQ") >= 2, lit(1.5))
    .when(col("DELINQ") == 1, lit(0.5))
    .otherwise(lit(0.0))
)

# Derogatory reports component (0-2 points)
derog_score = (
    when(col("DEROG").isNull(), lit(0.0))
    .when(col("DEROG") >= 3, lit(2.0))
    .when(col("DEROG") >= 1, lit(1.0))
    .otherwise(lit(0.0))
)

# ------------------------------------------------------------------
# Step 3: Segment label derived from the composite score.
# SAS equivalent (value risk_score):
#   low -< 3='Low Risk' 3 -< 5='Medium Risk' 5 -< 7='High Risk' 7-high='Very High Risk'
# ------------------------------------------------------------------
RISK_SEGMENT = (
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

df_risk = (
    df.withColumn("LTV_RISK_CAT", LTV_RISK_CAT)
    .withColumn("DTI_RISK_CAT", DTI_RISK_CAT)
    .withColumn("DELINQ_RISK_CAT", DELINQ_RISK_CAT)
    .withColumn("RISK_SCORE", ltv_score + dti_score + delinq_score + derog_score)
    .withColumn("RISK_SEGMENT", RISK_SEGMENT)
)

df_risk.cache()


def show_frequency(frame: DataFrame, column: str, total: int) -> None:
    """Frequency + percent for one column (SAS PROC FREQ, one-way table)."""
    print(f"\n--- {column} ---")
    frame.groupBy(column).agg(
        count("*").alias("Frequency"),
        spark_round(count("*") / lit(total) * 100, 2).alias("Percent"),
    ).orderBy(column).show(truncate=False)


# ------------------------------------------------------------------
# Step 4: Distribution across risk segments.
# SAS equivalent:
#   proc freq; tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
# ------------------------------------------------------------------
print("=" * 60)
print("Distribution of Loans by Risk Segment (PROC FREQ)")
print("=" * 60)

total_count = df_risk.count()
for seg_col in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    show_frequency(df_risk, seg_col, total_count)

# ------------------------------------------------------------------
# Step 5: Default rate and portfolio metrics by risk segment.
# SAS equivalent:
#   proc means n mean std; class RISK_SEGMENT; var BAD LOAN LTV DEBTINC;
# ------------------------------------------------------------------
print("=" * 60)
print("Average Default Rate by Risk Segment (PROC MEANS)")
print("=" * 60)

df_risk.groupBy("RISK_SEGMENT").agg(
    count("*").alias("N"),
    spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct"),
    spark_round(mean("LOAN"), 2).alias("Avg_LOAN"),
    spark_round(stddev("LOAN"), 2).alias("Std_LOAN"),
    spark_round(mean("LTV"), 4).alias("Avg_LTV"),
    spark_round(mean("DEBTINC"), 2).alias("Avg_DEBTINC"),
).orderBy("RISK_SEGMENT").show(truncate=False)

# ------------------------------------------------------------------
# Step 6: Cross-tabulation of default rate by LTV and DTI risk.
# SAS equivalent:
#   proc freq; tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
# ------------------------------------------------------------------
print("=" * 60)
print("Default Rates by LTV Risk and DTI Risk (PROC FREQ crosstab)")
print("=" * 60)

df_risk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT").agg(
    count("*").alias("N"),
    spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct"),
).orderBy("LTV_RISK_CAT", "DTI_RISK_CAT").show(20, truncate=False)

df_risk.unpersist()
spark.stop()
