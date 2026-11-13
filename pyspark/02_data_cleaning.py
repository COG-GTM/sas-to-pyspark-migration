"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Flag missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_PATH = "data/home_equity.csv"

IMPUTE_VARS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
OUTLIER_VARS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
SUMMARY_VARS = ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]
PRINT_VARS = ["BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"]

# Order matches the PROC MEANS statistic keywords.
PERCENTILES = [("P1", 0.01), ("P5", 0.05), ("P25", 0.25), ("Median", 0.50),
               ("P75", 0.75), ("P95", 0.95), ("P99", 0.99)]

# SAS PROPCASE default delimiters (blank, slash, hyphen, open paren, period, tab).
PROPCASE_DELIMS = r"([-/(.\t])"


def propcase(c):
    """SAS PROPCASE(): initcap() only splits on spaces, so temporarily add a
    space after each SAS delimiter (e.g. 'winston-salem' -> 'Winston-Salem')."""
    spaced = F.regexp_replace(F.lower(c), PROPCASE_DELIMS, "$1 ")
    return F.regexp_replace(F.initcap(spaced), PROPCASE_DELIMS + " ", "$1")


def load_data(spark: SparkSession, path: str = DATA_PATH) -> DataFrame:
    """Recreate work.home_equity (01_data_loading) so this stage is self-contained."""
    return spark.read.csv(path, header=True, inferSchema=True)


def derive_columns(df: DataFrame) -> DataFrame:
    """Step 1: LTV, LOAN_OUTCOME and proper-cased CITY."""
    return (
        df.withColumn(
            "LTV",
            F.when(
                F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
                F.col("MORTDUE") / F.col("VALUE"),
            ),
        )
        .withColumn(
            "LOAN_OUTCOME",
            F.when(F.col("BAD") == 0, "Paid").when(F.col("BAD") == 1, "Default"),
        )
        .withColumn("CITY", propcase(F.col("CITY")))
        .withMetadata("LTV", {"label": "Loan to Value Ratio", "format": "percent8.2"})
        .withMetadata("LOAN_OUTCOME", {"label": "Loan Outcome"})
    )


def flag_missing(df: DataFrame) -> DataFrame:
    """Step 2: one <VAR>_MISS 0/1 indicator per numeric variable."""
    return df.withColumns({
        f"{v}_MISS": F.when(F.col(v).isNull(), 1).otherwise(0) for v in IMPUTE_VARS
    })


def filter_critical(df: DataFrame) -> DataFrame:
    """Step 3: keep rows where LOAN, VALUE and BAD are non-missing."""
    return df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    )


def remove_outliers(df: DataFrame) -> DataFrame:
    """Step 5: LTV in (0, 5), LOAN > 0, VALUE > 0.
    Null comparisons evaluate to null and are dropped, matching SAS where a
    missing value fails every subsetting IF."""
    return df.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def _sas_percentile(c: str, p: float):
    """SAS default percentile definition (PCTLDEF=5): with n non-missing sorted
    values and j = n*p, take the mean of x[j] and x[j+1] when j is an integer,
    otherwise x[ceil(j)]. Computed exactly over the collected sorted values."""
    arr = F.sort_array(F.collect_list(F.col(c)))
    n = F.size(arr)
    np_ = n * F.lit(p)
    j = F.floor(np_).cast("int")
    ceil_j = F.ceil(np_).cast("int")
    return F.when(n == 0, F.lit(None)).when(
        np_ == j,
        (F.element_at(arr, F.greatest(j, F.lit(1))) + F.element_at(arr, F.least(j + 1, n))) / 2,
    ).otherwise(F.element_at(arr, ceil_j))


def proc_means(df: DataFrame, variables, with_percentiles: bool) -> DataFrame:
    """PROC MEANS: one output row per analysis variable."""
    spark = df.sparkSession
    exprs = []
    for v in variables:
        exprs += [
            F.count(v).alias(f"{v}__N"),
            F.count(F.when(F.col(v).isNull(), 1)).alias(f"{v}__NMiss"),
            F.mean(v).alias(f"{v}__Mean"),
            F.stddev_samp(v).alias(f"{v}__StdDev"),
            F.min(v).cast("double").alias(f"{v}__Min"),
            F.max(v).cast("double").alias(f"{v}__Max"),
        ]
        if with_percentiles:
            exprs += [_sas_percentile(v, p).cast("double").alias(f"{v}__{name}")
                      for name, p in PERCENTILES]
    row = df.agg(*exprs).first().asDict()

    stats = ["N", "NMiss", "Mean", "StdDev", "Min"]
    if with_percentiles:
        stats += [name for name, _ in PERCENTILES]
    stats += ["Max"]
    data = [
        [v] + [
            int(row[f"{v}__{s}"]) if s in ("N", "NMiss")
            else (float(row[f"{v}__{s}"]) if row[f"{v}__{s}"] is not None else None)
            for s in stats
        ]
        for v in variables
    ]
    schema = "Variable string, " + ", ".join(
        f"{s} {'long' if s in ('N', 'NMiss') else 'double'}" for s in stats
    )
    return spark.createDataFrame(data, schema)


def run(spark: SparkSession, path: str = DATA_PATH, verbose: bool = True) -> dict:
    """Run every step; returns the intermediate datasets keyed by SAS name."""

    # ------------------------------------------------------------------
    # Step 0: Load work.home_equity (output of 01_data_loading)
    # SAS equivalent:
    #   proc import datafile="/data/home_equity.csv" dbms=csv
    #       out=work.home_equity replace;
    #   run;
    # ------------------------------------------------------------------
    df = load_data(spark, path)

    # ------------------------------------------------------------------
    # Step 1: Create derived columns
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
    # Note: SAS labels/formats are stored as Spark column metadata.
    # A blank SAS character value maps to null.
    # ------------------------------------------------------------------
    dfClean = derive_columns(df)

    # ------------------------------------------------------------------
    # Step 2: Flag missing values (array processing)
    # SAS equivalent:
    #   data work.home_equity_imputed;
    #       set work.home_equity_clean;
    #       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
    #       array num_flags{8} LOAN_MISS ... NINQ_MISS;
    #       do i = 1 to 8;
    #           if num_vars{i} = . then num_flags{i} = 1;
    #           else num_flags{i} = 0;
    #       end;
    #       drop i;
    #   run;
    #
    # Note: the DO loop over arrays becomes a Python loop that builds
    # column expressions; Spark evaluates them in a single projection.
    # ------------------------------------------------------------------
    dfImputed = flag_missing(dfClean)

    # ------------------------------------------------------------------
    # Step 3: Filter out records with missing critical fields
    # SAS equivalent:
    #   data work.home_equity_filtered;
    #       set work.home_equity_imputed;
    #       if LOAN ne . and VALUE ne . and BAD ne .;
    #   run;
    # ------------------------------------------------------------------
    dfFiltered = filter_critical(dfImputed).cache()

    # ------------------------------------------------------------------
    # Step 4: Outlier detection summary statistics
    # SAS equivalent:
    #   proc means data=work.home_equity_filtered
    #       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
    #       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
    #   run;
    #
    # Note: percentiles use the SAS default definition (PCTLDEF=5),
    # computed exactly rather than with approx percentile_approx().
    # ------------------------------------------------------------------
    outlierStats = proc_means(dfFiltered, OUTLIER_VARS, with_percentiles=True)

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
    #   proc means data=work.home_equity_final n nmiss mean std min max;
    #       var LOAN MORTDUE VALUE LTV DEBTINC;
    #   run;
    #
    #   proc print data=work.home_equity_final(obs=10);
    #       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
    #   run;
    # ------------------------------------------------------------------
    dfFinal = remove_outliers(dfFiltered).cache()
    finalStats = proc_means(dfFinal, SUMMARY_VARS, with_percentiles=False)

    if verbose:
        print("=" * 60)
        print("Row counts by step")
        print("=" * 60)
        print(f"  work.home_equity          : {df.count()}")
        print(f"  work.home_equity_filtered : {dfFiltered.count()}")
        print(f"  work.home_equity_final    : {dfFinal.count()}")

        print("\n" + "=" * 60)
        print("Summary Statistics for Outlier Detection (PROC MEANS)")
        print("=" * 60)
        outlierStats.show(truncate=False)

        print("=" * 60)
        print("Clean Dataset Summary (PROC MEANS)")
        print("=" * 60)
        finalStats.show(truncate=False)

        print("=" * 60)
        print("First 10 Observations of HOME_EQUITY_FINAL (PROC PRINT obs=10)")
        print("=" * 60)
        # Apply format percent8.2 to LTV for display only.
        dfFinal.select(*PRINT_VARS) \
            .withColumn("LTV", F.format_string("%.2f%%", F.col("LTV") * 100)) \
            .show(10, truncate=False)

    return {
        "home_equity": df,
        "home_equity_clean": dfClean,
        "home_equity_imputed": dfImputed,
        "home_equity_filtered": dfFiltered,
        "home_equity_final": dfFinal,
        "outlier_stats": outlierStats,
        "final_stats": finalStats,
    }


if __name__ == "__main__":
    # SAS equivalent: Starting a SAS session
    spark = SparkSession.builder \
        .appName("HomeEquity_DataCleaning") \
        .master("local[*]") \
        .getOrCreate()
    run(spark)
    spark.stop()
