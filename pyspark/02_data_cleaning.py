"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
         - Create derived columns (LTV, LOAN_OUTCOME)
         - Flag missing values
         - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join, col, concat, count, format_string, initcap, lit,
    max as spark_max, mean, min as spark_min, percentile, split, stddev,
    substring, sum as spark_sum, transform, upper, when
)

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "home_equity.csv"
)

# SAS: array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
MISSING_FLAG_VARS = [
    "LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"
]

# SAS: label LTV = "Loan to Value Ratio" LOAN_OUTCOME = "Loan Outcome";
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}

# SAS propcase() capitalises after blank, /, -, (, . and tab.
# Spark initcap() only splits on spaces, so the remaining delimiters
# are handled by splitting, upper-casing each first letter and re-joining.
PROPCASE_DELIMITERS = ["/", "-", "(", ".", "\t"]


def propcase(column):
    """SAS propcase() equivalent built from native Spark functions."""
    result = initcap(column)
    for delim in PROPCASE_DELIMITERS:
        result = array_join(
            transform(
                split(result, "\\" + delim if delim != "\t" else "\t"),
                lambda part: concat(upper(substring(part, 1, 1)),
                                    substring(part, 2, 1 << 30))
            ),
            delim
        )
    return result


def derive_columns(df: DataFrame) -> DataFrame:
    """Step 1: LTV, LOAN_OUTCOME and proper-cased CITY."""
    return df \
        .withColumn(
            "LTV",
            when(
                col("VALUE").isNotNull() &
                col("MORTDUE").isNotNull() &
                (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid"))
            .when(col("BAD") == 1, lit("Default"))
            # SAS blank character value '' is missing -> null in Spark
            .otherwise(lit(None).cast("string"))
        ) \
        .withColumn("CITY", propcase(col("CITY")))


def flag_missing(df: DataFrame) -> DataFrame:
    """Step 2: <VAR>_MISS = 1 if VAR is missing else 0 (SAS array loop)."""
    return df.withColumns({
        f"{c}_MISS": when(col(c).isNull(), lit(1)).otherwise(lit(0))
        for c in MISSING_FLAG_VARS
    })


def filter_critical(df: DataFrame) -> DataFrame:
    """Step 3: keep rows where LOAN, VALUE and BAD are non-missing."""
    return df.filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    )


def remove_outliers(df: DataFrame) -> DataFrame:
    """Step 5: 0 < LTV < 5, LOAN > 0, VALUE > 0 (null LTV is dropped,
    as in SAS where a missing LTV fails LTV > 0)."""
    return df \
        .filter((col("LTV") > 0) & (col("LTV") < 5)) \
        .filter(col("LOAN") > 0) \
        .filter(col("VALUE") > 0)


def clean(df: DataFrame) -> dict:
    """Run the full cleaning pipeline and return each intermediate dataset
    keyed by its SAS WORK dataset name."""
    homeEquityClean = derive_columns(df)
    homeEquityImputed = flag_missing(homeEquityClean)
    homeEquityFiltered = filter_critical(homeEquityImputed)
    homeEquityFinal = remove_outliers(homeEquityFiltered)
    return {
        "home_equity_clean": homeEquityClean,
        "home_equity_imputed": homeEquityImputed,
        "home_equity_filtered": homeEquityFiltered,
        "home_equity_final": homeEquityFinal,
    }


def proc_means(df: DataFrame, variables, stats) -> DataFrame:
    """PROC MEANS equivalent: one output row per variable.

    Percentiles use Spark's exact percentile() (linear interpolation);
    SAS's default QNTLDEF=5 can differ slightly on small gaps.
    """
    statExprs = {
        "N": lambda c: count(col(c)),
        "NMiss": lambda c: spark_sum(when(col(c).isNull(), 1).otherwise(0)),
        "Mean": lambda c: mean(col(c)),
        "Std": lambda c: stddev(col(c)),
        "Min": lambda c: spark_min(col(c)),
        "P1": lambda c: percentile(col(c), lit(0.01)),
        "P5": lambda c: percentile(col(c), lit(0.05)),
        "P25": lambda c: percentile(col(c), lit(0.25)),
        "Median": lambda c: percentile(col(c), lit(0.50)),
        "P75": lambda c: percentile(col(c), lit(0.75)),
        "P95": lambda c: percentile(col(c), lit(0.95)),
        "P99": lambda c: percentile(col(c), lit(0.99)),
        "Max": lambda c: spark_max(col(c)),
    }
    perVariable = [
        df.agg(
            lit(c).alias("Variable"),
            *[statExprs[s](c).cast("long" if s in ("N", "NMiss") else "double")
              .alias(s) for s in stats]
        )
        for c in variables
    ]
    return reduce(DataFrame.unionByName, perVariable)


if __name__ == "__main__":
    # Initialize SparkSession
    # SAS equivalent: Starting a SAS session
    spark = SparkSession.builder \
        .appName("HomeEquity_DataCleaning") \
        .master("local[*]") \
        .getOrCreate()

    # Load the raw dataset (stages are self-contained)
    # SAS equivalent: work.home_equity created by 01_data_loading.sas
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    print(f"Input rows (work.home_equity): {df.count()}")

    # ------------------------------------------------------------------
    # Step 1: Create derived columns using DATA step
    # SAS equivalent:
    #   data work.home_equity_clean;
    #       length LOAN_OUTCOME $ 7;
    #       set work.home_equity;
    #       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
    #           LTV = MORTDUE / VALUE;
    #       else LTV = .;
    #       if BAD = 0 then LOAN_OUTCOME = 'Paid';
    #       else if BAD = 1 then LOAN_OUTCOME = 'Default';
    #       else LOAN_OUTCOME = '';
    #       CITY = propcase(CITY);
    #       label LTV = "Loan to Value Ratio" LOAN_OUTCOME = "Loan Outcome";
    #       format LTV percent8.2;
    #   run;
    #
    # Note: IF/THEN/ELSE -> when().otherwise(); labels are kept in the
    # columnLabels dictionary and percent8.2 is applied only when printing.
    # ------------------------------------------------------------------
    homeEquityClean = derive_columns(df)

    print("\n" + "=" * 60)
    print("Step 1: Derived columns (work.home_equity_clean)")
    print("=" * 60)
    for colName, label in columnLabels.items():
        print(f"  {colName:12s} -> {label}")
    homeEquityClean.select(
        "MORTDUE", "VALUE", "LTV", "BAD", "LOAN_OUTCOME", "CITY"
    ).show(5, truncate=False)

    # ------------------------------------------------------------------
    # Step 2: Handle missing values using array processing
    # SAS equivalent:
    #   data work.home_equity_imputed;
    #       set work.home_equity_clean;
    #       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
    #       array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
    #                          DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
    #       do i = 1 to 8;
    #           if num_vars{i} = . then num_flags{i} = 1;
    #           else num_flags{i} = 0;
    #       end;
    #       drop i;
    #   run;
    #
    # Note: The SAS ARRAY / DO loop becomes a Python loop over column
    # names that builds all flag columns in a single withColumns() call.
    # ------------------------------------------------------------------
    homeEquityImputed = flag_missing(homeEquityClean)

    print("\n" + "=" * 60)
    print("Step 2: Missing-value flags (work.home_equity_imputed)")
    print("=" * 60)
    homeEquityImputed.agg(*[
        spark_sum(f"{c}_MISS").alias(f"{c}_MISS") for c in MISSING_FLAG_VARS
    ]).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 3: Filter out records with missing critical fields
    # SAS equivalent:
    #   data work.home_equity_filtered;
    #       set work.home_equity_imputed;
    #       if LOAN ne . and VALUE ne . and BAD ne .;
    #   run;
    # ------------------------------------------------------------------
    homeEquityFiltered = filter_critical(homeEquityImputed).cache()

    print("\n" + "=" * 60)
    print("Step 3: Filtered on critical fields (work.home_equity_filtered)")
    print("=" * 60)
    print(f"Rows after filter: {homeEquityFiltered.count()}")

    # ------------------------------------------------------------------
    # Step 4: Check for outliers using PROC MEANS
    # SAS equivalent:
    #   title "Summary Statistics for Outlier Detection";
    #   proc means data=work.home_equity_filtered
    #       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
    #       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
    #   run;
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Summary Statistics for Outlier Detection (PROC MEANS)")
    print("=" * 60)
    proc_means(
        homeEquityFiltered,
        ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG",
         "DELINQ"],
        ["N", "NMiss", "Mean", "Std", "Min", "P1", "P5", "P25", "Median",
         "P75", "P95", "P99", "Max"]
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 5: Create the final clean dataset
    # SAS equivalent:
    #   data work.home_equity_final;
    #       set work.home_equity_filtered;
    #       if LTV > 0 and LTV < 5;
    #       if LOAN > 0;
    #       if VALUE > 0;
    #   run;
    #
    #   title "Clean Dataset Summary";
    #   proc means data=work.home_equity_final n nmiss mean std min max;
    #       var LOAN MORTDUE VALUE LTV DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    homeEquityFinal = remove_outliers(homeEquityFiltered).cache()

    print("\n" + "=" * 60)
    print("Clean Dataset Summary (work.home_equity_final)")
    print("=" * 60)
    print(f"Rows in final dataset: {homeEquityFinal.count()}")
    proc_means(
        homeEquityFinal,
        ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
        ["N", "NMiss", "Mean", "Std", "Min", "Max"]
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 6: Preview the final dataset
    # SAS equivalent:
    #   proc print data=work.home_equity_final(obs=10);
    #       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
    #   run;
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("First 10 Observations (equivalent to PROC PRINT obs=10)")
    print("=" * 60)
    homeEquityFinal.select(
        "BAD", "LOAN", "MORTDUE", "VALUE",
        # SAS format percent8.2 (display only)
        format_string("%.2f%%", col("LTV") * 100).alias("LTV"),
        "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
    ).show(10, truncate=False)

    # Clean up
    spark.stop()
