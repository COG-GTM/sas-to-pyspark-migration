"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
  - PROC FREQ for categorical distributions
  - PROC MEANS for numeric summaries by group
  - PROC TABULATE for cross-tabulations
  - PROC SQL for ad hoc queries
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

DATA_PATH = "data/home_equity.csv"


def buildHomeEquityFinal(spark, path=DATA_PATH):
    """Recreate work.home_equity_final (output of 01 + 02) from the raw CSV.

    SAS equivalent (sas/01_data_loading.sas, sas/02_data_cleaning.sas):
      proc import datafile="/data/home_equity.csv" out=work.home_equity ...;
      data work.home_equity_clean;   LTV, LOAN_OUTCOME, CITY=propcase(CITY)
      data work.home_equity_filtered; if LOAN ne . and VALUE ne . and BAD ne .;
      data work.home_equity_final;
          if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
      run;

    Only the steps that affect work.home_equity_final are recreated; the
    *_MISS flags from 02 are not used by this program.
    """
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull()
            & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    ).withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid"))
        .when(F.col("BAD") == 1, F.lit("Default")),
    ).withColumn("CITY", F.initcap(F.col("CITY")))

    df = df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull()
        & F.col("BAD").isNotNull()
    )

    # NULL comparisons evaluate to NULL (falsy) in Spark, matching SAS
    # subsetting IF where missing LTV fails "LTV > 0".
    return df.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5)
        & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def frequencyTable(df: DataFrame, column: str):
    """One-way frequency table (PROC FREQ ... / nocum).

    Returns (DataFrame[column, Frequency, Percent], frequencyMissing).
    Like SAS, missing values are excluded from the table and the percent
    denominator, and reported separately as "Frequency Missing".
    """
    nonMissing = df.filter(F.col(column).isNotNull())
    total = nonMissing.count()
    frequencyMissing = df.count() - total
    table = (
        nonMissing.groupBy(column)
        .agg(F.count(F.lit(1)).alias("Frequency"))
        .withColumn("Percent", F.round(F.col("Frequency") * 100.0 / total, 2))
        .orderBy(column)
    )
    return table, frequencyMissing


def classSummary(df: DataFrame, classVars, analysisVars, stats):
    """PROC MEANS with a CLASS statement.

    Rows with a missing CLASS value are dropped (SAS default without the
    MISSING option). Median uses Spark's exact percentile, which matches
    SAS's default QNTLDEF=5 at the 50th percentile (mean of the two middle
    values for an even N). Std is the sample standard deviation (VARDEF=DF).
    """
    statFns = {
        "N": lambda c: F.count(c),
        "Mean": lambda c: F.avg(c),
        "Median": lambda c: F.percentile(c, 0.5),
        "Std": lambda c: F.stddev_samp(c),
        "Min": lambda c: F.min(c),
        "Max": lambda c: F.max(c),
    }
    filtered = df
    for c in classVars:
        filtered = filtered.filter(F.col(c).isNotNull())

    aggs = [F.count(F.lit(1)).alias("N_Obs")]
    for var in analysisVars:
        for stat in stats:
            aggs.append(statFns[stat](F.col(var)).alias(f"{var}_{stat}"))
    return filtered.groupBy(*classVars).agg(*aggs).orderBy(*classVars)


def defaultRateTabulate(df: DataFrame) -> DataFrame:
    """PROC TABULATE of BAD (N and mean) by JOB rows x REGION columns.

    Rows with missing JOB or REGION are excluded (SAS default for CLASS
    variables). Adds a 'Total' row (all='Total') and Total columns.
    """
    base = df.filter(F.col("JOB").isNotNull() & F.col("REGION").isNotNull())
    regions = [r[0] for r in base.select("REGION").distinct().orderBy("REGION").collect()]

    def pivoted(frame, rowLabel):
        cells = frame.groupBy(rowLabel).pivot("REGION", regions).agg(
            F.count("BAD").alias("N"), F.avg("BAD").alias("Mean")
        )
        totals = frame.groupBy(rowLabel).agg(
            F.count("BAD").alias("Total_N"), F.avg("BAD").alias("Total_Mean")
        )
        return cells.join(totals, on=rowLabel)

    byJob = pivoted(base, "JOB").orderBy("JOB")
    allRow = pivoted(base.withColumn("JOB", F.lit("Total")), "JOB")
    return byJob.unionByName(allRow)


def topStatesByAvgLoan(spark, df: DataFrame) -> DataFrame:
    """PROC SQL outobs=10 top states by average loan amount."""
    df.createOrReplaceTempView("home_equity_final")
    return spark.sql("""
        SELECT STATE,
               COUNT(*)   AS num_loans,
               AVG(LOAN)  AS avg_loan,
               AVG(VALUE) AS avg_property_value,
               AVG(BAD)   AS default_rate
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC
        LIMIT 10
    """)


def dollar(c):
    """SAS dollar12. display format."""
    return F.concat(F.lit("$"), F.format_number(c, 0))


def percent(c):
    """SAS percent8.2 display format."""
    return F.concat(F.format_number(c * 100, 2), F.lit("%"))


def banner(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    # Initialize SparkSession
    # SAS equivalent: Starting a SAS session
    spark = SparkSession.builder \
        .appName("HomeEquity_AggregationReporting") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # work.home_equity_final from programs 01-02
    df = buildHomeEquityFinal(spark).cache()
    print(f"work.home_equity_final rows: {df.count()}")

    # ------------------------------------------------------------------
    # Step 1: Frequency tables for categorical variables
    # SAS equivalent:
    #   proc freq data=work.home_equity_final;
    #       tables JOB REASON LOAN_OUTCOME REGION / nocum;
    #   run;
    # ------------------------------------------------------------------
    banner("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
    for column in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        table, frequencyMissing = frequencyTable(df, column)
        table.show(truncate=False)
        if frequencyMissing:
            print(f"Frequency Missing = {frequencyMissing}")

    # ------------------------------------------------------------------
    # Step 2: Summary statistics by loan outcome
    # SAS equivalent:
    #   proc means data=work.home_equity_final n mean median std min max;
    #       class LOAN_OUTCOME;
    #       var LOAN MORTDUE VALUE DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    banner("Summary Statistics by Loan Outcome")
    outcomeStats = classSummary(
        df, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
        ["N", "Mean", "Median", "Std", "Min", "Max"],
    )
    # Display in SAS's long layout: one row per class level x variable
    for var in ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]:
        outcomeStats.select(
            "LOAN_OUTCOME", "N_Obs", F.lit(var).alias("Variable"),
            *[F.round(F.col(f"{var}_{s}"), 4).alias(s)
              for s in ["N", "Mean", "Median", "Std", "Min", "Max"]],
        ).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 3: Cross-tabulation of default rates by JOB and REGION
    # SAS equivalent:
    #   proc tabulate data=work.home_equity_final;
    #       class JOB REGION;
    #       var BAD;
    #       table JOB all='Total',
    #             REGION * BAD * (n mean*f=percent8.2)
    #             all='Total' * BAD * (n mean*f=percent8.2);
    #   run;
    # ------------------------------------------------------------------
    banner("Default Rates by Job Category and Region")
    tab = defaultRateTabulate(df)
    tab.select(*[
        percent(F.col(c)).alias(c) if c.endswith("_Mean") else F.col(c)
        for c in tab.columns
    ]).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 4: Top 10 states by average loan amount using PROC SQL
    # SAS equivalent:
    #   proc sql outobs=10;
    #       select STATE, count(*) as num_loans,
    #              mean(LOAN) as avg_loan format=dollar12.,
    #              mean(VALUE) as avg_property_value format=dollar12.,
    #              mean(BAD) as default_rate format=percent8.2
    #       from work.home_equity_final
    #       group by STATE
    #       having count(*) >= 10
    #       order by avg_loan desc;
    #   quit;
    # ------------------------------------------------------------------
    banner("Top 10 States by Average Loan Amount")
    topStatesByAvgLoan(spark, df).select(
        "STATE", "num_loans",
        dollar(F.col("avg_loan")).alias("avg_loan"),
        dollar(F.col("avg_property_value")).alias("avg_property_value"),
        percent(F.col("default_rate")).alias("default_rate"),
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 5: Loan distribution by reason and outcome
    # SAS equivalent:
    #   proc means data=work.home_equity_final n mean std median;
    #       class REASON LOAN_OUTCOME;
    #       var LOAN LTV DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    banner("Loan Amount Distribution by Reason and Outcome")
    reasonStats = classSummary(
        df, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"],
        ["N", "Mean", "Std", "Median"],
    )
    for var in ["LOAN", "LTV", "DEBTINC"]:
        reasonStats.select(
            "REASON", "LOAN_OUTCOME", "N_Obs", F.lit(var).alias("Variable"),
            *[F.round(F.col(f"{var}_{s}"), 4).alias(s)
              for s in ["N", "Mean", "Std", "Median"]],
        ).show(truncate=False)

    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()
