"""
PySpark Script: 06_simple_ltv_bands.py
Purpose: Minimal end-to-end migration example (filter, derive, aggregate)
Equivalent SAS Program: sas/06_simple_ltv_bands.sas
"""

import csv
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, lit, when

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "output", "06_ltv_band_summary_pyspark.csv")


def build_ltv_bands(df: DataFrame) -> DataFrame:
    """
    SAS equivalent:
        data work.ltv_bands;
            set work.home_equity(keep=BAD LOAN MORTDUE VALUE REASON);
            if LOAN ne . and VALUE ne . and VALUE > 0;
            if MORTDUE ne . then LTV = MORTDUE / VALUE;
            if LTV = . then LTV_BAND = 'UNKNOWN';
            else if LTV < 0.8 then LTV_BAND = 'LOW';
            else if LTV < 1.0 then LTV_BAND = 'MEDIUM';
            else LTV_BAND = 'HIGH';
        run;
    """
    ltv = when(col("MORTDUE").isNotNull(), col("MORTDUE") / col("VALUE"))
    return (
        df.select("BAD", "LOAN", "MORTDUE", "VALUE", "REASON")
        .filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & (col("VALUE") > 0))
        .withColumn("LTV", ltv)
        .withColumn(
            "LTV_BAND",
            # SAS missing (.) is < 0.8, Spark NULL is not; check null first like the SAS code
            when(col("LTV").isNull(), lit("UNKNOWN"))
            .when(col("LTV") < 0.8, lit("LOW"))
            .when(col("LTV") < 1.0, lit("MEDIUM"))
            .otherwise(lit("HIGH")),
        )
    )


def summarize_ltv_bands(bands: DataFrame) -> DataFrame:
    """
    SAS equivalent:
        proc means data=work.ltv_bands noprint nway;
            class LTV_BAND;
            var LOAN BAD;
            output out=work.ltv_band_summary(drop=_TYPE_ _FREQ_)
                n(LOAN)=N_LOANS mean(LOAN)=AVG_LOAN mean(BAD)=DEFAULT_RATE;
        run;
        proc sort data=work.ltv_band_summary; by LTV_BAND; run;
    """
    return (
        bands.groupBy("LTV_BAND")
        .agg(
            count("LOAN").alias("N_LOANS"),
            avg("LOAN").alias("AVG_LOAN"),
            avg("BAD").alias("DEFAULT_RATE"),
        )
        .orderBy("LTV_BAND")
    )


def main() -> None:
    spark = SparkSession.builder \
        .appName("HomeEquity_SimpleLtvBands") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    bands = build_ltv_bands(df)
    summary = summarize_ltv_bands(bands)

    print("=== Loans and Default Rate by LTV Band ===")
    summary.show(truncate=False)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(summary.columns)
        writer.writerows(summary.collect())
    print(f"Wrote {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
