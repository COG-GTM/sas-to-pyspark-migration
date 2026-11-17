"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas

Self-contained: rebuilds work.home_equity_final (01 load + 02 cleaning) inline.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")


def banner(title):
    # SAS: TITLE statement -> printed header
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def load_data(spark):
    # SAS: PROC IMPORT dbms=csv -> spark.read.csv (empty fields become null)
    return spark.read.csv(DATA_PATH, header=True, inferSchema=True)


def build_final_dataset(df):
    """Replicates 02_data_cleaning.sas up to work.home_equity_final."""
    # SAS: IF VALUE ne . and MORTDUE ne . and VALUE > 0 THEN LTV = ... -> when().otherwise(null)
    df = df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    )
    # SAS: IF BAD=0 'Paid' ELSE IF BAD=1 'Default' ELSE '' -> when().otherwise("")
    df = df.withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid"))
        .when(F.col("BAD") == 1, F.lit("Default"))
        .otherwise(F.lit("")),
    )
    # SAS: subsetting IF LOAN ne . and VALUE ne . and BAD ne . -> filter(isNotNull)
    df = df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    )
    # SAS: IF LTV > 0 and LTV < 5; IF LOAN > 0; IF VALUE > 0; -> filter (null LTV drops out)
    return df.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def proc_freq(df, var):
    """SAS: PROC FREQ TABLES var / NOCUM -> groupBy().count() with percent."""
    # SAS: missing levels are excluded from the table and reported as "Frequency Missing"
    nonMissing = df.filter(F.col(var).isNotNull() & (F.col(var) != ""))
    total = nonMissing.count()
    missing = df.count() - total
    freq = (
        nonMissing.groupBy(var)
        .count()
        .withColumnRenamed("count", "Frequency")
        .withColumn("Percent", F.round(F.col("Frequency") * 100.0 / total, 2))
        .orderBy(var)
    )
    print(f"\nTable of {var}")
    freq.show(truncate=False)
    if missing:
        print(f"Frequency Missing = {missing}")


def proc_means(df, classVars, analysisVars, stats):
    """SAS: PROC MEANS CLASS ... VAR ... -> groupBy().agg()."""
    # SAS: CLASS drops observations with a missing class value (no MISSING option)
    for c in classVars:
        df = df.filter(F.col(c).isNotNull() & (F.col(c) != ""))

    statExprs = {
        # SAS: N -> count (non-missing values only)
        "n": lambda v: F.count(v).alias(f"{v}_N"),
        "mean": lambda v: F.round(F.mean(v), 4).alias(f"{v}_Mean"),
        # SAS: MEDIAN -> percentile_approx(col, 0.5) (works on Spark < 3.4, unlike F.median)
        "median": lambda v: F.round(F.percentile_approx(v, 0.5), 4).alias(f"{v}_Median"),
        # SAS: STD (sample std dev) -> stddev_samp
        "std": lambda v: F.round(F.stddev_samp(v), 4).alias(f"{v}_StdDev"),
        "min": lambda v: F.min(v).alias(f"{v}_Min"),
        "max": lambda v: F.max(v).alias(f"{v}_Max"),
    }
    statLabels = {"n": "N", "mean": "Mean", "median": "Median", "std": "StdDev", "min": "Min", "max": "Max"}
    aggs = [F.count(F.lit(1)).alias("N_Obs")]
    statCols = {}
    for v in analysisVars:
        aggs.extend(statExprs[s](v) for s in stats)
        statCols[v] = [f"{v}_{statLabels[s]}" for s in stats]

    result = df.groupBy(*classVars).agg(*aggs).orderBy(*classVars)
    # One block per analysis variable mirrors the SAS listing
    for v in analysisVars:
        print(f"\nAnalysis Variable : {v}")
        result.select(*classVars, "N_Obs", *statCols[v]).show(truncate=False)
    return result


def proc_tabulate_default_rates(df):
    """SAS: PROC TABULATE JOB all, REGION*BAD*(n mean) all*BAD*(n mean) -> pivot."""
    # SAS: TABULATE drops observations with any missing CLASS variable
    tab = df.filter(F.col("JOB").isNotNull() & F.col("REGION").isNotNull())
    regions = [r["REGION"] for r in tab.select("REGION").distinct().orderBy("REGION").collect()]

    def cells(grouped):
        # SAS: REGION * BAD * (n mean*f=percent8.2) -> pivot(REGION).agg(count, mean)
        return grouped.pivot("REGION", regions).agg(
            F.count("BAD").alias("N"),
            F.round(F.mean("BAD") * 100, 2).alias("Default_Pct"),
        )

    def totals(grouped):
        # SAS: all='Total' * BAD * (n mean) -> overall column per row
        return grouped.agg(
            F.count("BAD").alias("Total_N"),
            F.round(F.mean("BAD") * 100, 2).alias("Total_Default_Pct"),
        )

    byJob = cells(tab.groupBy("JOB")).join(totals(tab.groupBy("JOB")), "JOB").orderBy("JOB")
    # SAS: row dimension "all='Total'" -> grand-total row appended via unionByName
    totalRow = (
        cells(tab.withColumn("JOB", F.lit("Total")).groupBy("JOB"))
        .join(totals(tab.withColumn("JOB", F.lit("Total")).groupBy("JOB")), "JOB")
    )
    result = byJob.unionByName(totalRow, allowMissingColumns=True)
    result.show(truncate=False)
    return result


def top_states_sql(spark, df):
    """SAS: PROC SQL OUTOBS=10 ... GROUP BY STATE HAVING ... ORDER BY -> spark.sql()."""
    # SAS: work.home_equity_final -> createOrReplaceTempView
    df.createOrReplaceTempView("home_equity_final")
    # SAS: mean() -> AVG(); OUTOBS=10 -> LIMIT 10; dollar12./percent8.2 formats -> ROUND
    result = spark.sql("""
        SELECT STATE,
               COUNT(*)                    AS num_loans,
               ROUND(AVG(LOAN), 2)         AS avg_loan,
               ROUND(AVG(VALUE), 2)        AS avg_property_value,
               ROUND(AVG(BAD) * 100, 2)    AS default_rate_pct
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC
        LIMIT 10
    """)
    result.show(truncate=False)
    return result


def main():
    # SAS: starting a SAS session -> SparkSession
    spark = (
        SparkSession.builder
        .appName("HomeEquity_AggregationReporting")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # SAS: work.home_equity_final from 02_data_cleaning.sas
    df = build_final_dataset(load_data(spark)).cache()

    # Step 1: SAS: PROC FREQ TABLES JOB REASON LOAN_OUTCOME REGION / NOCUM
    banner("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
    for var in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        proc_freq(df, var)

    # Step 2: SAS: PROC MEANS N MEAN MEDIAN STD MIN MAX; CLASS LOAN_OUTCOME
    banner("Summary Statistics by Loan Outcome")
    proc_means(
        df, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
        ["n", "mean", "median", "std", "min", "max"],
    )

    # Step 3: SAS: PROC TABULATE default rates by JOB and REGION
    banner("Default Rates by Job Category and Region")
    proc_tabulate_default_rates(df)

    # Step 4: SAS: PROC SQL top 10 states by average loan amount
    banner("Top 10 States by Average Loan Amount")
    top_states_sql(spark, df)

    # Step 5: SAS: PROC MEANS N MEAN STD MEDIAN; CLASS REASON LOAN_OUTCOME
    banner("Loan Amount Distribution by Reason and Outcome")
    proc_means(
        df, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"],
        ["n", "mean", "std", "median"],
    )

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
