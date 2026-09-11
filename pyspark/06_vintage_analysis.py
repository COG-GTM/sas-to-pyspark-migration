"""
PySpark Script: 06_vintage_analysis.py
Purpose: Monthly cohort / vintage report - cohort derivation, running
         exposure with first/last flags, cohort summary, and a
         delinquency-threshold sweep
Equivalent SAS Program: sas/06_vintage_analysis.sas

The transformations are exposed as functions so the test suite can import
and validate each stage; running the module as a script executes the full
report (equivalent to submitting the SAS program).
"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, mean, when, lit, upper, coalesce,
    row_number, monotonically_increasing_id, format_string, format_number
)

# ------------------------------------------------------------------
# SAS equivalent:
#   %let thresholds = 1 2 3;
# A space-delimited macro variable scanned with %scan/countw becomes a
# plain Python list.
# ------------------------------------------------------------------
THRESHOLDS = [1, 2, 3]

DATA_PATH = "data/home_equity.csv"


def derive_cohorts(df: DataFrame) -> DataFrame:
    """
    SAS equivalent (data work.he2):
        length COHORT $7 REASON_CLEAN $7;
        if missing(REASON) then REASON_CLEAN = 'UNKNOWN';
        else REASON_CLEAN = upcase(REASON);
        if YOJ = . then YOJ = 0;
        if YOJ < 2 then COHORT = 'V0-2';
        else if YOJ < 5 then COHORT = 'V2-5';
        else if YOJ < 10 then COHORT = 'V5-10';
        else COHORT = 'V10+';
        LTV = MORTDUE / VALUE;
        if VALUE in (., 0) then LTV = .;
    """
    return (
        df
        # missing(REASON) / upcase() -> when(isNull) / upper()
        .withColumn(
            "REASON_CLEAN",
            when(col("REASON").isNull(), lit("UNKNOWN")).otherwise(upper(col("REASON")))
        )
        # "if YOJ = . then YOJ = 0" -> coalesce (na.fill would also work)
        .withColumn("YOJ", coalesce(col("YOJ"), lit(0.0)))
        # IF / ELSE IF ladder -> chained when().otherwise(); order matters
        .withColumn(
            "COHORT",
            when(col("YOJ") < 2, lit("V0-2"))
            .when(col("YOJ") < 5, lit("V2-5"))
            .when(col("YOJ") < 10, lit("V5-10"))
            .otherwise(lit("V10+"))
        )
        # SAS division by missing or zero yields missing (.). Spark: x / null
        # -> null, but x / 0 -> Infinity (or null in ANSI mode), so guard
        # VALUE explicitly to reproduce "if VALUE in (., 0) then LTV = .".
        .withColumn(
            "LTV",
            when(col("VALUE").isNull() | (col("VALUE") == 0), lit(None))
            .otherwise(col("MORTDUE") / col("VALUE"))
        )
    )


def running_exposure(dfHe2: DataFrame) -> DataFrame:
    """
    SAS equivalent (proc sort + data work.he3):
        proc sort data=work.he2; by COHORT LOAN; run;
        data work.he3;
            set work.he2;
            by COHORT;
            retain CUM_LOAN 0 SEQ 0;
            if first.COHORT then do; CUM_LOAN = 0; SEQ = 0; end;
            CUM_LOAN + LOAN;
            SEQ + 1;
            IS_FIRST = first.COHORT;
            IS_LAST  = last.COHORT;
        run;

    Returns every row (one per input row) with the RETAIN'd running total
    and the first./last. flags. `cohort_last_rows` applies the
    "if last.COHORT then output;" filter.
    """
    # PROC SORT is stable (EQUALS is the default), so ties on LOAN keep
    # their input order. Capture input order as a tiebreaker so the
    # per-row running total is deterministic, like the sequential DATA step.
    dfOrdered = dfHe2.withColumn("_ROW_ID", monotonically_increasing_id())

    # BY COHORT + sequential processing -> Window.partitionBy("COHORT")
    # ordered the same way PROC SORT ordered the data.
    byCohort = Window.partitionBy("COHORT").orderBy("LOAN", "_ROW_ID")

    # RETAIN + "CUM_LOAN + LOAN" (sum statement) -> running sum over the
    # window. rowsBetween (not rangeBetween) so tied LOAN values accumulate
    # one row at a time, exactly like the DATA step.
    runningWindow = byCohort.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Whole-partition window for the last.COHORT comparison.
    cohortWindow = Window.partitionBy("COHORT")

    return (
        dfOrdered
        .withColumn("CUM_LOAN", spark_sum("LOAN").over(runningWindow))
        # "SEQ + 1" per row, reset on first.COHORT -> row_number()
        .withColumn("SEQ", row_number().over(byCohort))
        # first.COHORT -> row_number() == 1 ; SAS stores flags as 1/0
        .withColumn("IS_FIRST", (col("SEQ") == 1).cast("int"))
        # last.COHORT -> row_number() == count over the whole partition
        .withColumn("IS_LAST", (col("SEQ") == count("*").over(cohortWindow)).cast("int"))
        .drop("_ROW_ID")
    )


def cohort_last_rows(dfHe2: DataFrame) -> DataFrame:
    """
    SAS equivalent:
        if last.COHORT then output;
    Only the last row per cohort is emitted, carrying the cohort total.
    """
    return running_exposure(dfHe2).filter(col("IS_LAST") == 1)


def cohort_summary(dfHe2: DataFrame) -> DataFrame:
    """
    SAS equivalent (proc sql -> work.cohort_summary):
        select COHORT,
               count(*)  as N_LOANS,
               sum(BAD)  as N_BAD,
               calculated N_BAD / calculated N_LOANS as DEFAULT_RATE format=percent8.2,
               mean(LOAN)    as AVG_LOAN format=dollar12.,
               mean(LTV)     as AVG_LTV  format=8.3,
               mean(DEBTINC) as AVG_DTI  format=8.2
        from work.he2 group by COHORT order by COHORT;
    """
    return (
        dfHe2.groupBy("COHORT")
        .agg(
            count("*").alias("N_LOANS"),
            spark_sum("BAD").alias("N_BAD"),
            # mean() ignores nulls in both SAS and Spark
            mean("LOAN").alias("AVG_LOAN"),
            mean("LTV").alias("AVG_LTV"),
            mean("DEBTINC").alias("AVG_DTI"),
        )
        # "calculated N_BAD / calculated N_LOANS" -> reference the aliases
        # in a follow-up withColumn
        .withColumn("DEFAULT_RATE", col("N_BAD") / col("N_LOANS"))
        .select("COHORT", "N_LOANS", "N_BAD", "DEFAULT_RATE", "AVG_LOAN", "AVG_LTV", "AVG_DTI")
        # ORDER BY COHORT: lexical order, so 'V10+' sorts before 'V2-5' as in SAS
        .orderBy("COHORT")
    )


def delinq_over_threshold(dfHe2: DataFrame, threshold: int) -> DataFrame:
    """
    SAS equivalent (one iteration of %delinq_sweep):
        create table work.delinq_ge_&thr as
        select COHORT,
               &thr                             as THRESHOLD,
               sum(DELINQ >= &thr)              as N_OVER,
               count(*)                         as N,
               calculated N_OVER / calculated N as PCT_OVER format=percent8.2
        from work.he2
        where not missing(DELINQ)
        group by COHORT;
    """
    return (
        dfHe2.filter(col("DELINQ").isNotNull())
        .groupBy("COHORT")
        .agg(
            # "&thr as THRESHOLD" -> constant column via lit()
            lit(threshold).alias("THRESHOLD"),
            # SAS sums a boolean (1/0); Spark needs an explicit cast
            spark_sum((col("DELINQ") >= threshold).cast("int")).alias("N_OVER"),
            count("*").alias("N"),
        )
        .withColumn("PCT_OVER", col("N_OVER") / col("N"))
    )


def delinq_sweep(dfHe2: DataFrame, thresholds=THRESHOLDS) -> dict:
    """
    SAS equivalent:
        %macro delinq_sweep;
            %do i = 1 %to %sysfunc(countw(&thresholds));
                %let thr = %scan(&thresholds, &i);
                ... create table work.delinq_ge_&thr ...
            %end;
        %mend;
        %delinq_sweep;

    The %DO loop becomes a Python for-loop; the generated table names
    (delinq_ge_1, delinq_ge_2, ...) become dictionary keys.
    """
    return {
        f"delinq_ge_{thr}": delinq_over_threshold(dfHe2, thr)
        for thr in thresholds
    }


def stack_sweep(sweepTables: dict) -> DataFrame:
    """
    SAS equivalent:
        data work.delinq_all;
            set work.delinq_ge_1 work.delinq_ge_2 work.delinq_ge_3;
        run;
    SET with multiple datasets -> unionByName across the loop outputs.
    """
    return reduce(DataFrame.unionByName, sweepTables.values()).orderBy("THRESHOLD", "COHORT")


def main() -> None:
    spark = SparkSession.builder \
        .appName("HomeEquity_VintageAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    # ------------------------------------------------------------------
    # SAS equivalent:
    #   proc import datafile="/data/home_equity.csv" dbms=csv out=work.he replace;
    #       guessingrows=max;
    #   run;
    # guessingrows=max -> inferSchema=True (Spark scans the file for types)
    # ------------------------------------------------------------------
    dfHe = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    dfHe2 = derive_cohorts(dfHe)
    # he2 feeds three downstream steps; cache it instead of recomputing
    dfHe2.cache()

    dfHe3 = cohort_last_rows(dfHe2)
    dfSummary = cohort_summary(dfHe2)
    sweepTables = delinq_sweep(dfHe2)
    dfDelinqAll = stack_sweep(sweepTables)

    print("=" * 60)
    print("work.he3 - last row per cohort with RETAIN'd running total")
    print("=" * 60)
    dfHe3.select(
        "COHORT", "LOAN", "CUM_LOAN", "SEQ", "IS_FIRST", "IS_LAST", "REASON_CLEAN", "LTV"
    ).orderBy("COHORT").show(truncate=False)

    # ------------------------------------------------------------------
    # SAS equivalent:
    #   proc print data=work.cohort_summary noobs label; run;
    # SAS FORMATs (percent8.2, dollar12., 8.3, 8.2) are display-only, so the
    # stored columns stay numeric and formatting is applied at print time.
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Cohort Summary (equivalent to PROC PRINT of work.cohort_summary)")
    print("=" * 60)
    dfSummary.select(
        "COHORT", "N_LOANS", "N_BAD",
        format_string("%.2f%%", col("DEFAULT_RATE") * 100).alias("DEFAULT_RATE"),   # percent8.2
        format_string("$%s", format_number(col("AVG_LOAN"), 0)).alias("AVG_LOAN"),  # dollar12.
        format_string("%.3f", col("AVG_LTV")).alias("AVG_LTV"),                    # 8.3
        format_string("%.2f", col("AVG_DTI")).alias("AVG_DTI"),                    # 8.2
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # SAS equivalent:
    #   proc print data=work.delinq_all noobs; run;
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Delinquency Threshold Sweep (equivalent to PROC PRINT of work.delinq_all)")
    print("=" * 60)
    dfDelinqAll.select(
        "COHORT", "THRESHOLD", "N_OVER", "N",
        format_string("%.2f%%", col("PCT_OVER") * 100).alias("PCT_OVER"),  # percent8.2
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
