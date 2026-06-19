"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Handle missing values
  - Filter records and remove outliers

Migrated from: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap


def clean_home_equity(df):
    """Replicate the SAS cleaning pipeline, returning work.home_equity_final."""

    # Step 1: Create derived columns.
    # SAS equivalent: DATA step with IF/THEN/ELSE assignments.
    df = df.withColumn(
        # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE; else LTV = .
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ).withColumn(
        # SAS: if BAD = 0 then 'Paid'; else if BAD = 1 then 'Default'; else ''
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ).withColumn(
        # SAS: CITY = propcase(CITY);
        "CITY",
        initcap(col("CITY"))
    )

    # Step 2: Flag missing values for numeric variables.
    # SAS equivalent: array num_vars / num_flags with a DO loop creating *_MISS flags.
    missCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
    for c in missCols:
        df = df.withColumn(
            f"{c}_MISS",
            when(col(c).isNull(), lit(1)).otherwise(lit(0))
        )

    # Step 3: Filter out records with missing critical fields.
    # SAS equivalent: if LOAN ne . and VALUE ne . and BAD ne .;
    df = df.filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    )

    # Step 5: Remove extreme outliers to build the final clean dataset.
    # SAS equivalent: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
    df = df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) &
        (col("VALUE") > 0)
    )

    return df


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_DataCleaning") \
        .master("local[*]") \
        .getOrCreate()

    # SAS equivalent: PROC IMPORT dbms=csv.
    df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

    homeEquityFinal = clean_home_equity(df)

    # Step 4: Summary statistics for outlier detection / verification.
    # SAS equivalent: PROC MEANS on the cleaned dataset.
    print("Clean Dataset Summary (work.home_equity_final):")
    homeEquityFinal.select(
        "LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"
    ).describe().show()

    print(f"Final row count: {homeEquityFinal.count()}")

    # SAS equivalent: PROC PRINT data=work.home_equity_final(obs=10).
    homeEquityFinal.select(
        "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
        "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
