"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
  - groupBy().count()           (SAS PROC FREQ)
  - groupBy().agg(...)          (SAS PROC MEANS)
  - groupBy().pivot().agg(...)  (SAS PROC TABULATE)
  - spark.sql(...)              (SAS PROC SQL)

Migrated from: sas/03_aggregation_reporting.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as smin, max as smax, round as sround
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


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_AggregationReporting") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
    homeEquityFinal = clean_home_equity(df)

    # Step 1: Frequency tables for categorical variables.
    # SAS equivalent: PROC FREQ tables JOB REASON LOAN_OUTCOME REGION.
    for c in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        print(f"\nFrequency Table: {c}")
        homeEquityFinal.groupBy(c).count().orderBy(col("count").desc()).show()

    # Step 2: Summary statistics by loan outcome.
    # SAS equivalent: PROC MEANS class LOAN_OUTCOME; var LOAN MORTDUE VALUE DEBTINC.
    print("\nSummary Statistics by Loan Outcome:")
    aggExprs = []
    for c in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
        aggExprs += [
            count(c).alias(f"N_{c}"),
            sround(mean(c), 2).alias(f"Mean_{c}"),
            sround(stddev(c), 2).alias(f"Std_{c}"),
            sround(smin(c), 2).alias(f"Min_{c}"),
            sround(smax(c), 2).alias(f"Max_{c}"),
        ]
    homeEquityFinal.groupBy("LOAN_OUTCOME").agg(*aggExprs).show(truncate=False)

    # Step 3: Cross-tabulation of default rates by JOB and REGION.
    # SAS equivalent: PROC TABULATE class JOB REGION; var BAD (n and mean).
    print("\nDefault Counts by Job Category and Region (pivot):")
    homeEquityFinal.groupBy("JOB").pivot("REGION").agg(count("BAD")).show(truncate=False)
    print("\nDefault Rates (mean BAD) by Job Category and Region (pivot):")
    homeEquityFinal.groupBy("JOB").pivot("REGION").agg(sround(mean("BAD"), 4)).show(truncate=False)

    # Step 4: Top 10 states by average loan amount.
    # SAS equivalent: PROC SQL group by STATE having count(*) >= 10 order by avg_loan desc.
    homeEquityFinal.createOrReplaceTempView("home_equity_final")
    topStates = spark.sql("""
        SELECT STATE,
               COUNT(*)        AS num_loans,
               AVG(LOAN)       AS avg_loan,
               AVG(VALUE)      AS avg_property_value,
               AVG(BAD)        AS default_rate
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC
        LIMIT 10
    """)
    print("\nTop 10 States by Average Loan Amount:")
    topStates.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
