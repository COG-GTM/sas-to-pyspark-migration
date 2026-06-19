"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets via when().otherwise() chains (SAS PROC FORMAT)
  - Create a composite risk score (0-10)
  - Analyze default rates by risk segment

Migrated from: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, round as sround
)


def clean_home_equity(df):
    """Replicate the cleaning pipeline from 02_data_cleaning.py (work.home_equity_final)."""
    df = df.withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ).withColumn(
        "CITY", initcap(col("CITY"))
    )

    missCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
    for c in missCols:
        df = df.withColumn(
            f"{c}_MISS",
            when(col(c).isNull(), lit(1)).otherwise(lit(0))
        )

    df = df.filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    ).filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) &
        (col("VALUE") > 0)
    )
    return df


def add_risk_segments(df):
    """Create risk category columns, composite RISK_SCORE, and RISK_SEGMENT."""

    # Step 2: Risk category variables.
    # SAS equivalent: PROC FORMAT value ltv_risk / dti_risk / delinq_risk + DATA step assignment.
    df = df.withColumn(
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

    # Composite risk score (0-10 scale).
    # SAS equivalent: RISK_SCORE accumulation across LTV/DTI/DELINQ/DEROG components.
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

    df = df.withColumn(
        "RISK_SCORE",
        ltvScore + dtiScore + delinqScore + derogScore
    )

    # Map RISK_SCORE to RISK_SEGMENT.
    # SAS equivalent: PROC FORMAT value risk_score + DATA step assignment.
    df = df.withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk"))
    )

    return df


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_RiskSegmentation") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
    homeEquityFinal = clean_home_equity(df)
    homeEquityRisk = add_risk_segments(homeEquityFinal)

    # Step 3: Distribution of loans across risk segments.
    # SAS equivalent: PROC FREQ tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT.
    for c in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        print(f"\nDistribution by {c}:")
        homeEquityRisk.groupBy(c).count().orderBy(col("count").desc()).show()

    # Step 4: Default rate by risk segment.
    # SAS equivalent: PROC MEANS class RISK_SEGMENT; var BAD LOAN LTV DEBTINC.
    print("\nDefault Rate and Metrics by Risk Segment:")
    homeEquityRisk.groupBy("RISK_SEGMENT").agg(
        count("BAD").alias("N"),
        sround(mean("BAD"), 4).alias("Default_Rate"),
        sround(mean("LOAN"), 2).alias("Mean_LOAN"),
        sround(mean("LTV"), 4).alias("Mean_LTV"),
        sround(mean("DEBTINC"), 2).alias("Mean_DEBTINC"),
        sround(stddev("BAD"), 4).alias("Std_BAD"),
    ).orderBy("RISK_SEGMENT").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
