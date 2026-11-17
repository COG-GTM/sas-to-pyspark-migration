"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalents)
  - Create a composite risk score (0-10 scale)
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")


def load_data(spark: SparkSession) -> DataFrame:
    # SAS: PROC IMPORT dbms=csv -> spark.read.csv (empty fields become null)
    return spark.read.csv(DATA_PATH, header=True, inferSchema=True)


def build_final_dataset(df: DataFrame) -> DataFrame:
    """Inline replica of sas/02_data_cleaning.sas producing work.home_equity_final."""
    # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE; else LTV = .;
    df = df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    )
    # SAS: if LOAN ne . and VALUE ne . and BAD ne .; (subsetting IF -> filter)
    df = df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    )
    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0; (null comparisons drop the row, as in SAS)
    return df.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def add_risk_segments(df: DataFrame) -> DataFrame:
    """SAS DATA step creating work.home_equity_risk."""
    ltv, dti, delinq, derog = (F.col(c) for c in ("LTV", "DEBTINC", "DELINQ", "DEROG"))

    # SAS: PROC FORMAT value ltv_risk + IF/ELSE IF chain -> when().otherwise(); missing stays null
    ltv_risk_cat = (
        F.when(ltv.isNull(), F.lit(None))
        .when(ltv < 0.60, "Low")
        .when(ltv < 0.80, "Medium")
        .otherwise("High")
    )
    # SAS: PROC FORMAT value dti_risk
    dti_risk_cat = (
        F.when(dti.isNull(), F.lit(None))
        .when(dti < 30, "Low")
        .when(dti < 40, "Medium")
        .when(dti < 50, "High")
        .otherwise("Very High")
    )
    # SAS: PROC FORMAT value delinq_risk
    delinq_risk_cat = (
        F.when(delinq.isNull(), F.lit(None))
        .when(delinq == 0, "None")
        .when(delinq == 1, "Low")
        .when(delinq <= 3, "Medium")
        .otherwise("High")
    )

    # SAS: RISK_SCORE = 0; RISK_SCORE + n inside "if X ne . then do" -> sum of components,
    # where a missing component contributes 0
    ltv_points = (
        F.when(ltv.isNull(), 0.0).when(ltv >= 0.80, 3.0).when(ltv >= 0.60, 1.5).otherwise(0.0)
    )
    dti_points = (
        F.when(dti.isNull(), 0.0)
        .when(dti >= 50, 3.0)
        .when(dti >= 40, 2.0)
        .when(dti >= 30, 1.0)
        .otherwise(0.0)
    )
    delinq_points = (
        F.when(delinq.isNull(), 0.0)
        .when(delinq >= 4, 2.0)
        .when(delinq >= 2, 1.5)
        .when(delinq == 1, 0.5)
        .otherwise(0.0)
    )
    derog_points = (
        F.when(derog.isNull(), 0.0).when(derog >= 3, 2.0).when(derog >= 1, 1.0).otherwise(0.0)
    )

    df = (
        df.withColumn("LTV_RISK_CAT", ltv_risk_cat)
        .withColumn("DTI_RISK_CAT", dti_risk_cat)
        .withColumn("DELINQ_RISK_CAT", delinq_risk_cat)
        .withColumn("RISK_SCORE", ltv_points + dti_points + delinq_points + derog_points)
    )

    # SAS: PROC FORMAT value risk_score / RISK_SEGMENT IF/ELSE IF chain
    score = F.col("RISK_SCORE")
    return df.withColumn(
        "RISK_SEGMENT",
        F.when(score < 3, "Low Risk")
        .when(score < 5, "Medium Risk")
        .when(score < 7, "High Risk")
        .otherwise("Very High Risk"),
    )


# SAS: LABEL statement -> metadata dictionary (Spark has no native column labels)
COLUMN_LABELS = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}


def print_title(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def frequency_table(df: DataFrame, column: str) -> None:
    """SAS: PROC FREQ tables <col> / nocum -> groupBy().count() with percent of total."""
    total = df.count()
    (
        df.groupBy(column)
        .count()
        .withColumn("PERCENT", F.round(F.col("count") * 100.0 / total, 2))
        .orderBy(column)
        .show(truncate=False)
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("HomeEquity_RiskSegmentation")
        .master("local[*]")
        .getOrCreate()
    )

    df = build_final_dataset(load_data(spark))
    # SAS: data work.home_equity_risk; set work.home_equity_final; ... run;
    risk = add_risk_segments(df).cache()

    print_title("Column Labels (SAS-style variable labels)")
    for name, label in COLUMN_LABELS.items():
        print(f"  {name:16s} -> {label}")

    # Step 3 - SAS: PROC FREQ tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum
    print_title("Distribution of Loans by Risk Segment")
    for column in ("RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"):
        frequency_table(risk, column)

    # Step 4 - SAS: PROC MEANS n mean std; class RISK_SEGMENT; var BAD LOAN LTV DEBTINC
    # -> groupBy().agg(count, mean, stddev); PROC MEANS CLASS excludes missing class values
    print_title("Average Default Rate by Risk Segment")
    stats = []
    for var in ("BAD", "LOAN", "LTV", "DEBTINC"):
        stats += [
            F.count(var).alias(f"{var}_N"),
            F.round(F.mean(var), 4).alias(f"{var}_MEAN"),
            F.round(F.stddev(var), 4).alias(f"{var}_STD"),
        ]
    (
        risk.filter(F.col("RISK_SEGMENT").isNotNull())
        .groupBy("RISK_SEGMENT")
        .agg(F.count(F.lit(1)).alias("N_OBS"), *stats)
        .orderBy("RISK_SEGMENT")
        .show(truncate=False)
    )

    # Step 5 - SAS: PROC FREQ tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent
    # -> one LTV x DTI frequency table per BAD level via groupBy().pivot().count();
    # SAS drops rows with a missing value in any crosstab variable, so filter nulls first
    print_title("Default Rates by LTV Risk and DTI Risk")
    crosstab = risk.filter(
        F.col("LTV_RISK_CAT").isNotNull()
        & F.col("DTI_RISK_CAT").isNotNull()
        & F.col("BAD").isNotNull()
    )
    dti_levels = ["Low", "Medium", "High", "Very High"]
    for bad in (0, 1):
        print(f"\nBAD = {bad}")
        (
            crosstab.filter(F.col("BAD") == bad)
            .groupBy("LTV_RISK_CAT")
            .pivot("DTI_RISK_CAT", dti_levels)
            .count()
            .na.fill(0)
            .orderBy("LTV_RISK_CAT")
            .show(truncate=False)
        )

    risk.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
