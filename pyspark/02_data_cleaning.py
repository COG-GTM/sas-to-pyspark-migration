"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    initcap,
    lit,
    max,
    min,
    percentile_approx,
    stddev,
    sum,
    when,
)


MISSING_FLAG_COLUMNS = [
    "LOAN",
    "MORTDUE",
    "VALUE",
    "YOJ",
    "DEROG",
    "DELINQ",
    "CLAGE",
    "NINQ",
]

OUTLIER_COLUMNS = [
    "LOAN",
    "MORTDUE",
    "VALUE",
    "DEBTINC",
    "LTV",
    "CLAGE",
    "DEROG",
    "DELINQ",
]

PERCENTILES = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]


def transform_home_equity(
    df: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    df_clean = (
        df.withColumn(
            "LTV",
            when(
                col("VALUE").isNotNull()
                & col("MORTDUE").isNotNull()
                & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE"),
            ),
        )
        .withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid")).when(
                col("BAD") == 1, lit("Default")
            ),
        )
        .withColumn("CITY", initcap(col("CITY")))
    )

    for column_name in MISSING_FLAG_COLUMNS:
        df_clean = df_clean.withColumn(
            f"{column_name}_MISS",
            when(col(column_name).isNull(), lit(1)).otherwise(lit(0)),
        )

    df_filtered = df_clean.filter(
        col("LOAN").isNotNull()
        & col("VALUE").isNotNull()
        & col("BAD").isNotNull()
    )

    df_final = df_filtered.filter(
        (col("LTV") > 0)
        & (col("LTV") < 5)
        & (col("LOAN") > 0)
        & (col("VALUE") > 0)
    )

    return df_clean, df_filtered, df_final


def show_summary_statistics(
    df: DataFrame,
    columns: List[str],
    include_percentiles: bool,
) -> None:
    expressions = []
    for column_name in columns:
        expressions.extend(
            [
                count(col(column_name)).alias(f"{column_name}_N"),
                sum(
                    when(col(column_name).isNull(), lit(1)).otherwise(lit(0))
                ).alias(f"{column_name}_NMISS"),
                avg(col(column_name)).alias(f"{column_name}_MEAN"),
                stddev(col(column_name)).alias(f"{column_name}_STD"),
                min(col(column_name)).alias(f"{column_name}_MIN"),
                max(col(column_name)).alias(f"{column_name}_MAX"),
            ]
        )
        if include_percentiles:
            expressions.append(
                percentile_approx(
                    col(column_name),
                    PERCENTILES,
                    10000,
                ).alias(f"{column_name}_PERCENTILES")
            )

    result = df.agg(*expressions).first()
    for column_name in columns:
        print(
            f"{column_name}: "
            f"N={result[f'{column_name}_N']}, "
            f"NMISS={result[f'{column_name}_NMISS']}, "
            f"MEAN={result[f'{column_name}_MEAN']}, "
            f"STD={result[f'{column_name}_STD']}, "
            f"MIN={result[f'{column_name}_MIN']}, "
            f"MAX={result[f'{column_name}_MAX']}"
        )
        if include_percentiles:
            values = result[f"{column_name}_PERCENTILES"]
            print(
                "  "
                + ", ".join(
                    f"P{int(percentile * 100)}={value}"
                    for percentile, value in zip(PERCENTILES, values)
                )
            )


def main() -> None:
    spark = (
        SparkSession.builder.appName("HomeEquity_DataCleaning")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.csv(
            "data/home_equity.csv",
            header=True,
            inferSchema=True,
        )
        _, df_filtered, df_final = transform_home_equity(df)

        print("=" * 60)
        print("Summary Statistics for Outlier Detection")
        print("=" * 60)
        show_summary_statistics(
            df_filtered,
            OUTLIER_COLUMNS,
            include_percentiles=True,
        )

        print("\n" + "=" * 60)
        print("Clean Dataset Summary")
        print("=" * 60)
        print(f"Original rows: {df.count()}")
        print(f"After filtering: {df_filtered.count()}")
        print(f"Final clean rows: {df_final.count()}")
        show_summary_statistics(
            df_final,
            ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
            include_percentiles=False,
        )

        df_final.select(
            "BAD",
            "LOAN",
            "MORTDUE",
            "VALUE",
            "LTV",
            "LOAN_OUTCOME",
            "DEBTINC",
            "JOB",
            "REASON",
        ).show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
