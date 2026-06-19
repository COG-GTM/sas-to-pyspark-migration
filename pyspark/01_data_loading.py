"""
PySpark Script: 01_data_loading.py
Purpose: Load and explore the HOME_EQUITY dataset
Dataset: data/home_equity.csv - Home equity loan data for risk analysis

Migrated from: sas/01_data_loading.sas
"""

from pyspark.sql import SparkSession


def main():
    # SAS equivalent: SAS session is implicit; PySpark requires an explicit SparkSession.
    spark = SparkSession.builder \
        .appName("HomeEquity_DataLoading") \
        .master("local[*]") \
        .getOrCreate()

    # Step 1: Import CSV data into a Spark DataFrame.
    # SAS equivalent: PROC IMPORT dbms=csv (guessingrows -> inferSchema=True).
    df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

    # Step 2: Document variable labels as a metadata dictionary.
    # SAS equivalent: PROC DATASETS ... LABEL statements (PySpark has no native labels).
    columnLabels = {
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

    # Step 3: Display column labels for documentation/reporting.
    # SAS equivalent: labels are surfaced in PROC CONTENTS / PROC PRINT output.
    print("Column Labels:")
    for colName, label in columnLabels.items():
        print(f"  {colName}: {label}")

    # Step 4: Display dataset metadata (column names, types).
    # SAS equivalent: PROC CONTENTS.
    print("\nHOME_EQUITY Dataset Metadata (schema):")
    df.printSchema()

    rowCount = df.count()
    print(f"\nRow count: {rowCount}")
    print(f"Column count: {len(df.columns)}")

    # Step 5: Preview the first 20 observations.
    # SAS equivalent: PROC PRINT data=work.home_equity(obs=20).
    print("\nFirst 20 Observations of HOME_EQUITY:")
    df.show(20, truncate=False)

    # SAS equivalent: end of SAS session.
    spark.stop()


if __name__ == "__main__":
    main()
