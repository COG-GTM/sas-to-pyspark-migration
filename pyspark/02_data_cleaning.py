"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

NUM_VARS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

# SAS propcase() default delimiters: blank, slash, hyphen, open paren, period, tab
PROPCASE_DELIMS = " /-(.\t"


def propcase(colName):
    """SAS: propcase(x) -> capitalise the first letter of every delimited word.

    initcap() only splits on whitespace, so "winston-salem" would become
    "Winston-salem"; SAS gives "Winston-Salem".
    """
    lowered = F.lower(F.col(colName))
    delims = F.array(*[F.lit(c) for c in PROPCASE_DELIMS])
    chars = F.split(lowered, "")
    return F.when(lowered.isNull(), F.lit(None)).otherwise(
        F.array_join(
            F.transform(
                chars,
                lambda c, i: F.when(
                    (i == 0) | F.array_contains(delims, F.element_at(chars, i)),
                    F.upper(c),
                ).otherwise(c),
            ),
            "",
        )
    )


def print_title(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def proc_means(df, varCols, stats, classCols=()):
    """SAS: PROC MEANS <stats>; CLASS ...; VAR ...; -> groupBy().agg() per variable.

    Percentiles use percentile_approx (accuracy 10000, exact at this data size);
    SAS's default QNTLDEF=5 may average two neighbouring values instead.
    Rows with a missing CLASS value are excluded, as in SAS (no MISSING option).
    """
    statExprs = {
        "N": lambda c: F.count(c),
        "NMISS": lambda c: F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)),
        "MEAN": lambda c: F.mean(c),
        "STD": lambda c: F.stddev_samp(c),
        "MIN": lambda c: F.min(c),
        "MAX": lambda c: F.max(c),
        "MEDIAN": lambda c: F.percentile_approx(c, 0.5, 10000),
    }
    for pct in (1, 5, 25, 75, 95, 99):
        statExprs[f"P{pct}"] = lambda c, q=pct / 100: F.percentile_approx(c, q, 10000)

    source = df
    for classCol in classCols:
        source = source.filter(F.col(classCol).isNotNull())

    result = None
    for varIdx, varCol in enumerate(varCols):
        part = source.groupBy(*classCols).agg(
            *[(statExprs[s](varCol) if s in ("N", "NMISS")
               else F.round(statExprs[s](varCol).cast("double"), 4)).alias(s) for s in stats]
        ).withColumn("Variable", F.lit(varCol)).withColumn("_varIdx", F.lit(varIdx))
        result = part if result is None else result.unionByName(part)
    return result.orderBy(*classCols, "_varIdx").select(*classCols, "Variable", *stats)


def load_home_equity(spark):
    # SAS: PROC IMPORT dbms=csv guessingrows=max -> spark.read.csv(inferSchema=True)
    # Empty CSV fields are read as null, the PySpark equivalent of SAS missing (.)
    return spark.read.csv(DATA_PATH, header=True, inferSchema=True)


def main():
    # SAS: session start -> SparkSession
    spark = SparkSession.builder \
        .appName("HomeEquity_DataCleaning") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = load_home_equity(spark)

    # ------------------------------------------------------------------
    # Step 1: Create derived columns
    # SAS equivalent:
    #   data work.home_equity_clean;
    #       length LOAN_OUTCOME $ 7;
    #       set work.home_equity;
    #       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
    #       else LTV = .;
    #       if BAD = 0 then LOAN_OUTCOME = 'Paid';
    #       else if BAD = 1 then LOAN_OUTCOME = 'Default';
    #       else LOAN_OUTCOME = '';
    #       CITY = propcase(CITY);
    #       label LTV = "Loan to Value Ratio" LOAN_OUTCOME = "Loan Outcome";
    #   run;
    # ------------------------------------------------------------------
    homeEquityClean = df.select(
        *[c for c in df.columns if c != "CITY"],
        # SAS: CITY = propcase(CITY) -> propcase() helper (array transform)
        propcase("CITY").alias("CITY"),
        # SAS: IF/THEN/ELSE LTV = . -> when() without otherwise() yields null
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ).alias("LTV", metadata={"comment": "Loan to Value Ratio"}),  # SAS: LABEL -> column metadata
        # SAS: blank character value '' is missing -> null
        F.when(F.col("BAD") == 0, F.lit("Paid"))
        .when(F.col("BAD") == 1, F.lit("Default"))
        .alias("LOAN_OUTCOME", metadata={"comment": "Loan Outcome"}),
    ).select(*df.columns, "LTV", "LOAN_OUTCOME")

    # ------------------------------------------------------------------
    # Step 2: Flag missing values using array processing
    # SAS equivalent:
    #   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
    #   array num_flags{8} LOAN_MISS ... NINQ_MISS;
    #   do i = 1 to 8;
    #       if num_vars{i} = . then num_flags{i} = 1; else num_flags{i} = 0;
    #   end;
    # ------------------------------------------------------------------
    # SAS: ARRAY + DO loop -> Python loop over a column list with withColumn
    homeEquityImputed = homeEquityClean
    for varName in NUM_VARS:
        homeEquityImputed = homeEquityImputed.withColumn(
            f"{varName}_MISS",
            F.when(F.col(varName).isNull(), 1).otherwise(0),
        )

    # ------------------------------------------------------------------
    # Step 3: Filter out records with missing critical fields
    # SAS equivalent:
    #   data work.home_equity_filtered;
    #       set work.home_equity_imputed;
    #       if LOAN ne . and VALUE ne . and BAD ne .;
    #   run;
    # ------------------------------------------------------------------
    # SAS: subsetting IF with "ne ." -> filter(isNotNull())
    homeEquityFiltered = homeEquityImputed.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    ).cache()

    # ------------------------------------------------------------------
    # Step 4: Check for outliers
    # SAS equivalent:
    #   proc means data=work.home_equity_filtered
    #       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
    #       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
    #   run;
    # ------------------------------------------------------------------
    print_title("Summary Statistics for Outlier Detection")
    proc_means(
        homeEquityFiltered,
        ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"],
        ["N", "NMISS", "MEAN", "STD", "MIN", "P1", "P5", "P25", "MEDIAN",
         "P75", "P95", "P99", "MAX"],
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
    # ------------------------------------------------------------------
    # SAS missing compares lower than any number, so "LTV > 0" drops missing LTV;
    # in Spark a null comparison is null and filter() drops it the same way.
    homeEquityFinal = homeEquityFiltered.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5)
        & (F.col("LOAN") > 0)
        & (F.col("VALUE") > 0)
    ).cache()

    print_title("Row Counts by Step")
    print(f"  work.home_equity          : {df.count()}")
    print(f"  work.home_equity_filtered : {homeEquityFiltered.count()}")
    print(f"  work.home_equity_final    : {homeEquityFinal.count()}")

    # SAS: proc means data=work.home_equity_final n nmiss mean std min max;
    #          var LOAN MORTDUE VALUE LTV DEBTINC;
    print_title("Clean Dataset Summary")
    proc_means(
        homeEquityFinal,
        ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
        ["N", "NMISS", "MEAN", "STD", "MIN", "MAX"],
    ).show(truncate=False)

    # SAS: proc print data=work.home_equity_final(obs=10); var ...; -> select().show(10)
    print_title("First 10 Observations of work.home_equity_final")
    homeEquityFinal.select(
        "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
        "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON",
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
