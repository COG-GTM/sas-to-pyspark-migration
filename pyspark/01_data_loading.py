"""
PySpark Script: 01_data_loading.py
Purpose: Load and explore the HOME_EQUITY dataset
Equivalent SAS Program: sas/01_data_loading.sas

Faithful PySpark re-migration of the SAS data-loading program:
  - PROC IMPORT  -> spark.read.csv(header=True, inferSchema=True)
  - variable labels / formats (PROC DATASETS) -> documented as DataFrame
    column metadata, since Spark has no native display labels/formats
  - PROC CONTENTS -> schema inspection (printSchema, column list, dtypes)
  - PROC PRINT (obs=20) -> bounded preview via show(n)
"""

from pyspark.sql import SparkSession

# SAS variable labels applied by the PROC DATASETS ... LABEL step.
# Spark has no native "label" concept, so we attach them as column
# metadata (and print them) purely for documentation/reporting parity.
COLUMN_LABELS = {
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

# SAS display formats applied by the PROC DATASETS ... FORMAT step.
# These control display only (not stored values); recorded as metadata
# because Spark's DataFrame API has no equivalent display-format layer.
COLUMN_FORMATS = {
    "LOAN": "dollar12.",
    "MORTDUE": "dollar12.",
    "VALUE": "dollar12.",
    "APPDATE": "date9.",
    "DEBTINC": "8.1",
    "CLAGE": "comma8.1",
}

DATA_PATH = "data/home_equity.csv"


def build_spark_session():
    """Start a self-contained local SparkSession (SAS: begin a SAS session)."""
    return (
        SparkSession.builder
        .appName("HomeEquity_DataLoading")
        .master("local[*]")
        .getOrCreate()
    )


def load_home_equity(spark):
    """
    Step 1: Import the CSV into a DataFrame.

    SAS equivalent:
        proc import datafile="/data/home_equity.csv"
            dbms=csv out=work.home_equity replace;
            guessingrows=5960;
        run;

    guessingrows=5960 (scan every row to guess types) maps to
    inferSchema=True, which scans the data to infer column types.
    """
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    # Attach SAS labels/formats as column metadata so downstream readers
    # retain the documentation from the PROC DATASETS steps.
    for name in df.columns:
        metadata = {}
        if name in COLUMN_LABELS:
            metadata["label"] = COLUMN_LABELS[name]
        if name in COLUMN_FORMATS:
            metadata["sas_format"] = COLUMN_FORMATS[name]
        if metadata:
            df = df.withMetadata(name, metadata)
    return df


def print_labels():
    """Print the SAS variable labels (documentation of the LABEL step)."""
    print("=" * 60)
    print("Column Labels (SAS-style variable labels)")
    print("=" * 60)
    for name, label in COLUMN_LABELS.items():
        fmt = COLUMN_FORMATS.get(name, "")
        suffix = f"  [format: {fmt}]" if fmt else ""
        print(f"  {name:12s} -> {label}{suffix}")


def describe_contents(df):
    """
    Step 2: Describe dataset metadata.

    SAS equivalent:
        proc contents data=work.home_equity;
        run;

    PROC CONTENTS reports column names, types, count and attributes;
    reproduced here via schema inspection.
    """
    print("\n" + "=" * 60)
    print("Dataset Schema (equivalent to PROC CONTENTS)")
    print("=" * 60)
    df.printSchema()

    print(f"\nNumber of observations: {df.count()}")
    print(f"Number of variables: {len(df.columns)}")

    print("\nVariables (position, name, type):")
    for position, (name, dtype) in enumerate(df.dtypes, start=1):
        print(f"  {position:>2d}  {name:12s} {dtype}")


def preview(df, n=20):
    """
    Step 3: Preview the first n observations.

    SAS equivalent:
        proc print data=work.home_equity(obs=20);
        run;
    """
    print("\n" + "=" * 60)
    print(f"First {n} Observations (equivalent to PROC PRINT obs={n})")
    print("=" * 60)
    df.show(n, truncate=False)


def main():
    spark = build_spark_session()
    try:
        df = load_home_equity(spark)
        print_labels()
        describe_contents(df)
        preview(df, 20)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
