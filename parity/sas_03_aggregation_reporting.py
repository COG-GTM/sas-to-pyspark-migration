"""
PySpark conversion of sas/03_aggregation_reporting.sas that preserves SAS
DATA step and PROC semantics exactly (as opposed to the idiomatic rewrite in
pyspark/03_aggregation_reporting.py).

The SAS program reads ``work.home_equity_final``, which is produced by the
DATA step chain in sas/02_data_cleaning.sas, so that chain is reproduced here
first (``build_home_equity_final``). Each PROC step is exposed as a function
returning a DataFrame with a stable column layout so it can be diffed against
the expected SAS output in tests/expected_sas_output/.

SAS semantics that are deliberately preserved:

* Missing-value comparisons. In SAS a numeric missing (``.``) is *smaller than
  any number*, so ``if LTV < 5`` keeps missing rows while ``if LTV > 0`` drops
  them. PySpark's three-valued NULL logic drops NULLs in *both* cases, so the
  helpers ``sas_gt`` / ``sas_lt`` encode the SAS rule explicitly.
* ``PROPCASE`` treats blank, ``/``, ``-``, ``(``, ``.`` and tab as word
  delimiters; Spark's ``initcap`` only splits on whitespace.
* ``PROC FREQ`` excludes missing values from the table and from the percent
  denominator; levels are ordered by internal value (``ORDER=INTERNAL``).
* ``PROC MEANS`` with ``CLASS`` drops observations where any CLASS variable is
  missing, prints only the full CLASS crossing, computes ``N`` per analysis
  variable (non-missing count), the sample standard deviation, and the
  ``PCTLDEF=5`` median (which for p=0.5 is the conventional median).
* ``PROC TABULATE`` drops observations where *any* CLASS variable is missing
  before computing every cell, including the ``ALL`` totals.
* ``PROC SQL``: ``mean()`` ignores missing values (as does Spark ``AVG``),
  ``outobs=10`` limits the *result rows* (``LIMIT 10``), ``HAVING`` is
  evaluated after grouping. ``GROUP BY`` keeps a missing group in SAS, as in
  Spark. Display formats (``dollar12.``, ``percent8.2``) only affect
  rendering and are not applied to the data.
"""

import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DATA_PATH = os.path.join("data", "home_equity.csv")

# PROC IMPORT with guessingrows=5960 infers numeric for every column that only
# contains numbers, and character for the rest.
HOME_EQUITY_SCHEMA = StructType([
    StructField("BAD", DoubleType()),
    StructField("LOAN", DoubleType()),
    StructField("MORTDUE", DoubleType()),
    StructField("VALUE", DoubleType()),
    StructField("REASON", StringType()),
    StructField("JOB", StringType()),
    StructField("YOJ", DoubleType()),
    StructField("DEROG", DoubleType()),
    StructField("DELINQ", DoubleType()),
    StructField("CLAGE", DoubleType()),
    StructField("NINQ", DoubleType()),
    StructField("CLNO", DoubleType()),
    StructField("DEBTINC", DoubleType()),
    StructField("APPDATE", DoubleType()),
    StructField("CITY", StringType()),
    StructField("STATE", StringType()),
    StructField("DIVISION", StringType()),
    StructField("REGION", StringType()),
])

MISS_FLAG_VARS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

PROPCASE_DELIMITERS = " /-(.\t"


# ---------------------------------------------------------------------------
# SAS semantic helpers
# ---------------------------------------------------------------------------
def sas_gt(c, value):
    """SAS ``c > value``: missing is less than every number, so missing -> false."""
    return c.isNotNull() & (c > value)


def sas_lt(c, value):
    """SAS ``c < value``: missing is less than every number, so missing -> true."""
    return c.isNull() | (c < value)


def sas_propcase(text):
    if text is None:
        return None
    out = []
    capitalize_next = True
    for ch in text.lower():
        if capitalize_next and ch.isalpha():
            out.append(ch.upper())
            capitalize_next = False
        else:
            out.append(ch)
        if ch in PROPCASE_DELIMITERS:
            capitalize_next = True
    return "".join(out)


propcase_udf = F.udf(sas_propcase, StringType())


def sas_std(c):
    """PROC MEANS STD: sample standard deviation, missing when N < 2."""
    return F.when(F.count(c) >= 2, F.stddev_samp(c))


def sas_median(c):
    """PROC MEANS MEDIAN with the default PCTLDEF=5 (exact, not approximate)."""
    return F.percentile(c, F.lit(0.5))


# ---------------------------------------------------------------------------
# sas/01 + sas/02: build work.home_equity_final
# ---------------------------------------------------------------------------
def load_home_equity(spark, path=DATA_PATH):
    """proc import datafile=... dbms=csv out=work.home_equity;"""
    return spark.read.csv(path, header=True, schema=HOME_EQUITY_SCHEMA, encoding="UTF-8")


def build_home_equity_final(df):
    """Reproduce the DATA step chain of sas/02_data_cleaning.sas."""
    # data work.home_equity_clean;
    clean = (
        df
        # if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE; else LTV = .;
        .withColumn(
            "LTV",
            F.when(
                F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & sas_gt(F.col("VALUE"), 0),
                F.col("MORTDUE") / F.col("VALUE"),
            ),
        )
        # length LOAN_OUTCOME $ 7; blank character value == missing == NULL here
        .withColumn(
            "LOAN_OUTCOME",
            F.when(F.col("BAD") == 0, F.lit("Paid")).when(F.col("BAD") == 1, F.lit("Default")),
        )
        # CITY = propcase(CITY);
        .withColumn("CITY", propcase_udf(F.col("CITY")))
    )

    # data work.home_equity_imputed;  array num_vars{8} ...; do i = 1 to 8; ...
    imputed = clean
    for var in MISS_FLAG_VARS:
        imputed = imputed.withColumn(
            f"{var}_MISS", F.when(F.col(var).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))
        )

    # data work.home_equity_filtered;  if LOAN ne . and VALUE ne . and BAD ne .;
    filtered = imputed.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    )

    # data work.home_equity_final;
    #   if LTV > 0 and LTV < 5;  if LOAN > 0;  if VALUE > 0;
    final = filtered.filter(
        sas_gt(F.col("LTV"), 0) & sas_lt(F.col("LTV"), 5)
        & sas_gt(F.col("LOAN"), 0)
        & sas_gt(F.col("VALUE"), 0)
    )
    return final


# ---------------------------------------------------------------------------
# sas/03 steps
# ---------------------------------------------------------------------------
def step1_freq_tables(final, variables=("JOB", "REASON", "LOAN_OUTCOME", "REGION")):
    """proc freq; tables JOB REASON LOAN_OUTCOME REGION / nocum;"""
    result = None
    for var in variables:
        nonmissing = final.filter(F.col(var).isNotNull())
        table = (
            nonmissing.groupBy(F.col(var).alias("VALUE"))
            .agg(F.count(F.lit(1)).alias("FREQUENCY"))
            .withColumn("VARIABLE", F.lit(var))
            .withColumn("PERCENT", F.col("FREQUENCY") * 100.0 / F.sum("FREQUENCY").over(Window.partitionBy()))
            .select("VARIABLE", "VALUE", "FREQUENCY", "PERCENT")
        )
        result = table if result is None else result.unionByName(table)
    return result.orderBy(F.array_position(F.array(*[F.lit(v) for v in variables]), F.col("VARIABLE")), "VALUE")


def _proc_means(final, class_vars, analysis_vars, stats):
    """Generic PROC MEANS with CLASS: one output row per class level x analysis variable."""
    class_cols = [F.col(c) for c in class_vars]
    nonmissing_class = final
    for c in class_vars:
        nonmissing_class = nonmissing_class.filter(F.col(c).isNotNull())

    stat_exprs = {
        "N": lambda c: F.count(c).cast("double"),
        "MEAN": F.mean,
        "MEDIAN": sas_median,
        "STD": sas_std,
        "MIN": F.min,
        "MAX": F.max,
    }

    result = None
    for var in analysis_vars:
        c = F.col(var)
        aggs = [F.count(F.lit(1)).cast("double").alias("N_OBS")]
        aggs += [stat_exprs[s](c).alias(s) for s in stats]
        table = (
            nonmissing_class.groupBy(*class_cols)
            .agg(*aggs)
            .withColumn("VARIABLE", F.lit(var))
            .select(*class_vars, "N_OBS", "VARIABLE", *stats)
        )
        result = table if result is None else result.unionByName(table)
    return result.orderBy(
        *class_vars, F.array_position(F.array(*[F.lit(v) for v in analysis_vars]), F.col("VARIABLE"))
    )


def step2_means_by_outcome(final):
    """proc means n mean median std min max; class LOAN_OUTCOME; var LOAN MORTDUE VALUE DEBTINC;"""
    return _proc_means(
        final,
        class_vars=["LOAN_OUTCOME"],
        analysis_vars=["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
        stats=["N", "MEAN", "MEDIAN", "STD", "MIN", "MAX"],
    )


def step3_tabulate_default_rates(final):
    """
    proc tabulate; class JOB REGION; var BAD;
        table JOB all='Total', REGION * BAD * (n mean) all='Total' * BAD * (n mean);

    Returned in long form: one row per (JOB, REGION) cell including 'Total' margins.
    """
    nonmissing_class = final.filter(F.col("JOB").isNotNull() & F.col("REGION").isNotNull())
    return (
        nonmissing_class.cube("JOB", "REGION")
        .agg(F.count("BAD").cast("double").alias("N"), F.mean("BAD").alias("MEAN_BAD"))
        .withColumn("JOB", F.coalesce(F.col("JOB"), F.lit("Total")))
        .withColumn("REGION", F.coalesce(F.col("REGION"), F.lit("Total")))
        .select("JOB", "REGION", "N", "MEAN_BAD")
        .orderBy(F.col("JOB") == "Total", "JOB", F.col("REGION") == "Total", "REGION")
    )


def step4_sql_top_states(spark, final):
    """
    proc sql outobs=10;
        select STATE, count(*) as num_loans, mean(LOAN) as avg_loan,
               mean(VALUE) as avg_property_value, mean(BAD) as default_rate
        from work.home_equity_final
        group by STATE
        having count(*) >= 10
        order by avg_loan desc;
    quit;

    SAS leaves the order of ties unspecified; STATE is added as a deterministic
    tie-breaker so the result is reproducible.
    """
    final.createOrReplaceTempView("home_equity_final")
    return spark.sql(
        """
        SELECT STATE,
               CAST(COUNT(*) AS DOUBLE) AS num_loans,
               AVG(LOAN)                AS avg_loan,
               AVG(VALUE)               AS avg_property_value,
               AVG(BAD)                 AS default_rate
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC, STATE
        LIMIT 10
        """
    )


def step5_means_by_reason_outcome(final):
    """proc means n mean std median; class REASON LOAN_OUTCOME; var LOAN LTV DEBTINC;"""
    return _proc_means(
        final,
        class_vars=["REASON", "LOAN_OUTCOME"],
        analysis_vars=["LOAN", "LTV", "DEBTINC"],
        stats=["N", "MEAN", "STD", "MEDIAN"],
    )


STEPS = {
    "01_freq_tables": lambda spark, final: step1_freq_tables(final),
    "02_means_by_outcome": lambda spark, final: step2_means_by_outcome(final),
    "03_tabulate_default_rates": lambda spark, final: step3_tabulate_default_rates(final),
    "04_sql_top_states": step4_sql_top_states,
    "05_means_by_reason_outcome": lambda spark, final: step5_means_by_reason_outcome(final),
}


def run_all(spark, data_path=DATA_PATH):
    """Return {step_name: DataFrame} for every step of the SAS program."""
    final = build_home_equity_final(load_home_equity(spark, data_path)).cache()
    return {name: fn(spark, final) for name, fn in STEPS.items()}


def get_spark(app_name="SAS_03_AggregationReporting_Parity"):
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    out_dir = argv[0] if argv else None
    spark = get_spark()
    try:
        for name, df in run_all(spark).items():
            print("=" * 72)
            print(name)
            print("=" * 72)
            df.show(200, truncate=False)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
                df.toPandas().to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
