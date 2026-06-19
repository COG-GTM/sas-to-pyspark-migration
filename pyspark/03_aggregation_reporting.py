"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
  - PROC FREQ   -> groupBy().count()
  - PROC MEANS  -> groupBy().agg()
  - PROC TABULATE -> groupBy().pivot().agg()
  - PROC SQL    -> spark.sql()

Migrated from: sas/03_aggregation_reporting.sas

Run with: python pyspark/03_aggregation_reporting.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, median, stddev, min as smin, max as smax,
)


def get_data_path():
    """Resolve data/home_equity.csv relative to the project root."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "home_equity.csv")


def load_clean(spark):
    """Load CSV and apply the key cleaning transformations from script 02."""
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
    )

    # Outlier removal (mirrors script 02 final filter).
    df = df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )
    return df


def summary_by_group(df, group_col, value_cols):
    """PROC MEANS with CLASS: n, mean, median, std, min, max per value column."""
    agg_exprs = []
    for c in value_cols:
        agg_exprs += [
            count(c).alias(f"N_{c}"),
            mean(c).alias(f"Mean_{c}"),
            median(c).alias(f"Median_{c}"),
            stddev(c).alias(f"Std_{c}"),
            smin(c).alias(f"Min_{c}"),
            smax(c).alias(f"Max_{c}"),
        ]
    return df.groupBy(group_col).agg(*agg_exprs)


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_AggregationReporting") \
        .master("local[*]") \
        .getOrCreate()

    df = load_clean(spark)

    # Step 1: Frequency tables.
    # SAS: PROC FREQ tables JOB REASON LOAN_OUTCOME REGION;
    # PySpark: groupBy(col).count().
    for c in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        print(f"\nFrequency table: {c}")
        df.groupBy(c).count().orderBy(col("count").desc()).show(truncate=False)

    # Step 2: Summary statistics by loan outcome.
    # SAS: PROC MEANS class LOAN_OUTCOME; var LOAN MORTDUE VALUE DEBTINC;
    print("\nSummary Statistics by Loan Outcome:")
    summary_by_group(df, "LOAN_OUTCOME", ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]).show(truncate=False)

    # Step 3: Cross-tabulation of default rates by JOB and REGION.
    # SAS: PROC TABULATE class JOB REGION; var BAD;
    # PySpark: groupBy().pivot().agg(count, mean).
    print("\nDefault counts by Job Category and Region (pivot):")
    df.groupBy("JOB").pivot("REGION").agg(count("BAD")).show(truncate=False)
    print("\nDefault rates (mean BAD) by Job Category and Region (pivot):")
    df.groupBy("JOB").pivot("REGION").agg(mean("BAD")).show(truncate=False)

    # Step 4: Top 10 states by average loan amount.
    # SAS: PROC SQL outobs=10 ... -> PySpark: temp view + spark.sql().
    df.createOrReplaceTempView("home_equity_final")
    print("\nTop 10 States by Average Loan Amount:")
    spark.sql("""
        SELECT STATE,
               COUNT(*)     AS num_loans,
               AVG(LOAN)    AS avg_loan,
               AVG(VALUE)   AS avg_property_value,
               AVG(BAD)     AS default_rate
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC
        LIMIT 10
    """).show(truncate=False)

    # Step 5: Additional reporting - by REASON and LOAN_OUTCOME.
    # SAS: PROC MEANS class REASON LOAN_OUTCOME; var LOAN LTV DEBTINC;
    print("\nLoan Amount Distribution by Reason and Outcome:")
    df.groupBy("REASON", "LOAN_OUTCOME").agg(
        count("LOAN").alias("N"),
        mean("LOAN").alias("Mean_LOAN"),
        stddev("LOAN").alias("Std_LOAN"),
        median("LOAN").alias("Median_LOAN"),
        mean("LTV").alias("Mean_LTV"),
        mean("DEBTINC").alias("Mean_DEBTINC"),
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
