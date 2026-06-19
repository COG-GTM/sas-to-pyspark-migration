"""
PySpark Script: 01_data_loading.py
Purpose: Load and explore the HOME_EQUITY dataset
Dataset: data/home_equity.csv - Home equity loan data for risk analysis

Migrated from: sas/01_data_loading.sas

Run with: python pyspark/01_data_loading.py
"""

import os

from pyspark.sql import SparkSession


def get_data_path():
    """Resolve data/home_equity.csv relative to the project root."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "home_equity.csv")


def main():
    # SAS: a SAS session is implicit -> PySpark: build an explicit SparkSession.
    spark = SparkSession.builder \
        .appName("HomeEquity_DataLoading") \
        .master("local[*]") \
        .getOrCreate()

    # SAS: PROC IMPORT datafile=... dbms=csv guessingrows=5960
    # PySpark: spark.read.csv() with header + schema inference.
    data_path = get_data_path()
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # SAS: PROC DATASETS ... LABEL statements (display/metadata documentation).
    # PySpark: no native column labels -> capture them in a dictionary.
    column_labels = {
        "BAD": "Loan Status (1=Default, 0=Paid)",
        "LOAN": "Amount of Loan Request",
        "MORTDUE": "Amount Due on Existing Mortgage",
        "VALUE": "Value of Current Property",
        "REASON": "Loan Purpose (HomeImp or DebtCon)",
        "JOB": "Job Category",
        "YOJ": "Years at Present Job",
        "DEROG": "Number of Derogatory Reports",
        "DELINQ": "Number of Delinquent Credit Lines",
        "CLAGE": "Age of Oldest Credit Line (months)",
        "NINQ": "Number of Recent Credit Inquiries",
        "CLNO": "Number of Credit Lines",
        "DEBTINC": "Debt to Income Ratio",
        "APPDATE": "Loan Application Date",
        "CITY": "City",
        "STATE": "State",
        "DIVISION": "Census Division",
        "REGION": "Census Region",
    }

    print("Column Labels:")
    for col_name, label in column_labels.items():
        print(f"  {col_name}: {label}")

    # SAS: PROC CONTENTS -> PySpark: printSchema() for column names and types.
    print("\nHOME_EQUITY Dataset Metadata (schema):")
    df.printSchema()

    print(f"\nRow count: {df.count()}")
    print(f"Column count: {len(df.columns)}")

    # SAS: PROC PRINT data=work.home_equity(obs=20) -> PySpark: df.show(20).
    print("\nFirst 20 Observations of HOME_EQUITY:")
    df.show(20, truncate=False)

    # SAS: end of session -> PySpark: stop the SparkSession.
    spark.stop()


if __name__ == "__main__":
    main()
