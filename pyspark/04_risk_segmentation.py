"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Risk buckets (LTV, DTI, delinquency)  -> when().otherwise() chains
  - Composite risk score (0-10)
  - Default-rate analysis by segment

Migrated from: sas/04_risk_segmentation.sas

Run with: python pyspark/04_risk_segmentation.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, mean, stddev


def get_data_path():
    """Resolve data/home_equity.csv relative to the project root."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "home_equity.csv")


def load_clean(spark):
    """Load CSV and apply the cleaning transformations from script 02."""
    df = spark.read.csv(get_data_path(), header=True, inferSchema=True)

    df = df.withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ).otherwise(lit(None)),
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
    )

    df = df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    ).filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )
    return df


def add_risk_columns(df):
    """Replicate the SAS PROC FORMAT buckets and composite score logic."""
    # SAS: PROC FORMAT value ltv_risk / IF-THEN -> when().otherwise() chain.
    df = df.withColumn(
        "LTV_RISK_CAT",
        when(col("LTV").isNull(), lit(None))
        .when(col("LTV") < 0.60, lit("Low"))
        .when(col("LTV") < 0.80, lit("Medium"))
        .otherwise(lit("High")),
    ).withColumn(
        "DTI_RISK_CAT",
        when(col("DEBTINC").isNull(), lit(None))
        .when(col("DEBTINC") < 30, lit("Low"))
        .when(col("DEBTINC") < 40, lit("Medium"))
        .when(col("DEBTINC") < 50, lit("High"))
        .otherwise(lit("Very High")),
    ).withColumn(
        "DELINQ_RISK_CAT",
        when(col("DELINQ").isNull(), lit(None))
        .when(col("DELINQ") == 0, lit("None"))
        .when(col("DELINQ") == 1, lit("Low"))
        .when(col("DELINQ") <= 3, lit("Medium"))
        .otherwise(lit("High")),
    )

    # SAS: composite RISK_SCORE accumulated with IF-THEN; null contributes 0.
    ltv_score = (
        when(col("LTV").isNull(), lit(0))
        .when(col("LTV") >= 0.80, lit(3))
        .when(col("LTV") >= 0.60, lit(1.5))
        .otherwise(lit(0))
    )
    dti_score = (
        when(col("DEBTINC").isNull(), lit(0))
        .when(col("DEBTINC") >= 50, lit(3))
        .when(col("DEBTINC") >= 40, lit(2))
        .when(col("DEBTINC") >= 30, lit(1))
        .otherwise(lit(0))
    )
    delinq_score = (
        when(col("DELINQ").isNull(), lit(0))
        .when(col("DELINQ") >= 4, lit(2))
        .when(col("DELINQ") >= 2, lit(1.5))
        .when(col("DELINQ") == 1, lit(0.5))
        .otherwise(lit(0))
    )
    derog_score = (
        when(col("DEROG").isNull(), lit(0))
        .when(col("DEROG") >= 3, lit(2))
        .when(col("DEROG") >= 1, lit(1))
        .otherwise(lit(0))
    )

    df = df.withColumn(
        "RISK_SCORE", ltv_score + dti_score + delinq_score + derog_score
    ).withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk")),
    )
    return df


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_RiskSegmentation") \
        .master("local[*]") \
        .getOrCreate()

    df = add_risk_columns(load_clean(spark))

    # Step 3: Distribution across risk segments.
    # SAS: PROC FREQ tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT;
    for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        print(f"\nDistribution: {c}")
        df.groupBy(c).count().orderBy(col("count").desc()).show(truncate=False)

    # Step 4: Default rate by risk segment.
    # SAS: PROC MEANS class RISK_SEGMENT; var BAD LOAN LTV DEBTINC;
    print("\nAverage Default Rate by Risk Segment:")
    df.groupBy("RISK_SEGMENT").agg(
        count("BAD").alias("N"),
        mean("BAD").alias("Default_Rate"),
        stddev("BAD").alias("Std_BAD"),
        mean("LOAN").alias("Mean_LOAN"),
        mean("LTV").alias("Mean_LTV"),
        mean("DEBTINC").alias("Mean_DEBTINC"),
    ).orderBy("RISK_SEGMENT").show(truncate=False)

    # Step 5: Cross-tabulation of risk segments.
    # SAS: PROC FREQ tables LTV_RISK_CAT * DTI_RISK_CAT * BAD;
    print("\nCounts by LTV Risk, DTI Risk, and BAD:")
    df.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD").count() \
        .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT", "BAD").show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
