"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Flag missing values
  - Filter records and check for outliers

Migrated from: sas/02_data_cleaning.sas

Run with: python pyspark/02_data_cleaning.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap


def get_data_path():
    """Resolve data/home_equity.csv relative to the project root."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "home_equity.csv")


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_DataCleaning") \
        .master("local[*]") \
        .getOrCreate()

    # SAS: PROC IMPORT dbms=csv -> PySpark: spark.read.csv()
    df = spark.read.csv(get_data_path(), header=True, inferSchema=True)

    # Step 1: Derived columns.
    # SAS DATA step IF/THEN -> PySpark when().otherwise().
    # LTV = MORTDUE / VALUE only when both non-missing and VALUE > 0, else null.
    df = df.withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ).otherwise(lit(None)),
    )

    # SAS: if BAD=0 then 'Paid'; else if BAD=1 then 'Default'.
    df = df.withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default")),
    )

    # SAS: CITY = propcase(CITY) -> PySpark: initcap().
    df = df.withColumn("CITY", initcap(col("CITY")))

    # Step 2: Missing value flags.
    # SAS: ARRAY processing with a DO loop -> PySpark: loop over a column list
    # with withColumn, building a *_MISS flag for each numeric variable.
    miss_flag_cols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
    for c in miss_flag_cols:
        df = df.withColumn(
            f"{c}_MISS",
            when(col(c).isNull(), lit(1)).otherwise(lit(0)),
        )

    # Step 3: Filter out records with missing critical fields.
    # SAS: if LOAN ne . and VALUE ne . and BAD ne .;
    df = df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )

    # Step 4: Outlier detection.
    # SAS: PROC MEANS n nmiss mean std min p1 p5 p25 median p75 p95 p99 max
    # PySpark: .summary() for the standard moments/percentiles plus
    # .approxQuantile() for the extra tail percentiles (p1, p99).
    outlier_cols = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
    print("Summary Statistics for Outlier Detection:")
    df.select(outlier_cols).summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    ).show(truncate=False)

    print("Tail percentiles (p1, p99) via approxQuantile:")
    for c in outlier_cols:
        q = df.filter(col(c).isNotNull()).approxQuantile(c, [0.01, 0.99], 0.001)
        if q:
            print(f"  {c}: p1={q[0]:.4f}, p99={q[1]:.4f}")
        else:
            print(f"  {c}: no non-null values")

    # Step 5: Final clean dataset - remove extreme outliers.
    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
    df_final = df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )

    print("\nClean Dataset Summary:")
    df_final.select("LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC").summary(
        "count", "mean", "stddev", "min", "max"
    ).show(truncate=False)

    print(f"Final row count: {df_final.count()}")

    print("\nPreview of final clean dataset:")
    df_final.select(
        "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
