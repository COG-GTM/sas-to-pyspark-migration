"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
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




def clean_home_equity(df):
    """Rebuild work.home_equity_final from sas/02_data_cleaning.sas (steps 1-3, 5)."""
    # SAS: DATA step IF/THEN -> when(); CITY = propcase(CITY) -> propcase()
    clean = df.withColumn("CITY", propcase("CITY")) \
        .withColumn(
            "LTV",
            F.when(
                F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
                F.col("MORTDUE") / F.col("VALUE"),
            ),
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            F.when(F.col("BAD") == 0, F.lit("Paid")).when(F.col("BAD") == 1, F.lit("Default")),
        )
    # SAS: ARRAY num_vars / num_flags + DO loop -> loop over NUM_VARS
    for varName in NUM_VARS:
        clean = clean.withColumn(
            f"{varName}_MISS", F.when(F.col(varName).isNull(), 1).otherwise(0)
        )
    # SAS: if LOAN ne . and VALUE ne . and BAD ne .; if LTV > 0 and LTV < 5;
    #      if LOAN > 0; if VALUE > 0;  -> filter()
    return clean.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
        & (F.col("LTV") > 0) & (F.col("LTV") < 5)
        & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def proc_freq(df, colName):
    """SAS: PROC FREQ; TABLES col / nocum; -> groupBy().count() with percent.

    Like SAS, missing values are excluded from the table and percentages and
    reported as "Frequency Missing".
    """
    nonMissing = df.filter(F.col(colName).isNotNull())
    total = nonMissing.count()
    print(f"\n--- {colName} ---")
    nonMissing.groupBy(colName) \
        .agg(F.count("*").alias("Frequency")) \
        .withColumn("Percent", F.round(F.col("Frequency") / F.lit(total) * 100, 2)) \
        .orderBy(colName) \
        .show(truncate=False)
    missing = df.count() - total
    if missing:
        print(f"Frequency Missing = {missing}")


def proc_tabulate_default_rate(df, rowCol, colCol):
    """SAS: PROC TABULATE; CLASS row col; VAR BAD;
         TABLE row all='Total', col*BAD*(n mean*f=percent8.2) all='Total'*BAD*(n mean);
    -> groupBy().pivot().agg() plus 'Total' row/column via a second aggregation.
    """
    # PROC TABULATE drops observations with a missing CLASS value
    source = df.filter(F.col(rowCol).isNotNull() & F.col(colCol).isNotNull())
    colLevels = sorted(r[0] for r in source.select(colCol).distinct().collect())

    def cells(grouped):
        # SAS: n -> count(BAD); mean*f=percent8.2 -> mean(BAD) * 100 rounded to 2
        pivoted = grouped.pivot(colCol, colLevels).agg(
            F.count("BAD").alias("N"),
            F.round(F.mean("BAD") * 100, 2).alias("Default_Pct"),
        )
        return pivoted

    byRow = cells(source.groupBy(rowCol)) \
        .join(
            source.groupBy(rowCol).agg(
                F.count("BAD").alias("Total_N"),
                F.round(F.mean("BAD") * 100, 2).alias("Total_Default_Pct"),
            ),
            on=rowCol,
        )
    # SAS: all='Total' row
    totalRow = cells(source.withColumn(rowCol, F.lit("Total")).groupBy(rowCol)) \
        .join(
            source.withColumn(rowCol, F.lit("Total")).groupBy(rowCol).agg(
                F.count("BAD").alias("Total_N"),
                F.round(F.mean("BAD") * 100, 2).alias("Total_Default_Pct"),
            ),
            on=rowCol,
        )
    return byRow.orderBy(rowCol).unionByName(totalRow)


def main():
    # SAS: session start -> SparkSession
    spark = SparkSession.builder \
        .appName("HomeEquity_AggregationReporting") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # SAS: work.home_equity_final (built by 02_data_cleaning.sas) -> rebuilt inline
    homeEquityFinal = clean_home_equity(load_home_equity(spark)).cache()
    print(f"work.home_equity_final rows: {homeEquityFinal.count()}")

    # ------------------------------------------------------------------
    # Step 1: Frequency tables for categorical variables
    # SAS equivalent:
    #   proc freq data=work.home_equity_final;
    #       tables JOB REASON LOAN_OUTCOME REGION / nocum;
    #   run;
    # ------------------------------------------------------------------
    print_title("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
    for catCol in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        proc_freq(homeEquityFinal, catCol)

    # ------------------------------------------------------------------
    # Step 2: Summary statistics by loan outcome
    # SAS equivalent:
    #   proc means data=work.home_equity_final n mean median std min max;
    #       class LOAN_OUTCOME;
    #       var LOAN MORTDUE VALUE DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    print_title("Summary Statistics by Loan Outcome")
    proc_means(
        homeEquityFinal,
        ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
        ["N", "MEAN", "MEDIAN", "STD", "MIN", "MAX"],
        classCols=["LOAN_OUTCOME"],
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
    print_title("Default Rates by Job Category and Region")
    proc_tabulate_default_rate(homeEquityFinal, "JOB", "REGION").show(truncate=False)

    # ------------------------------------------------------------------
    # Step 4: Top 10 states by average loan amount
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
    # SAS: PROC SQL -> temp view + spark.sql(); outobs=10 -> LIMIT 10
    # SAS: format dollar12. / percent8.2 -> format_string() for display only;
    #      ORDER BY uses the unformatted numeric average, as in SAS.
    homeEquityFinal.createOrReplaceTempView("home_equity_final")
    print_title("Top 10 States by Average Loan Amount")
    spark.sql("""
        SELECT STATE,
               COUNT(*)                                     AS num_loans,
               format_string('$%,.0f', AVG(LOAN))           AS avg_loan,
               format_string('$%,.0f', AVG(VALUE))          AS avg_property_value,
               format_string('%.2f%%', AVG(BAD) * 100)      AS default_rate
        FROM home_equity_final
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY AVG(LOAN) DESC
        LIMIT 10
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 5: Loan distribution by reason and outcome
    # SAS equivalent:
    #   proc means data=work.home_equity_final n mean std median;
    #       class REASON LOAN_OUTCOME;
    #       var LOAN LTV DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    print_title("Loan Amount Distribution by Reason and Outcome")
    proc_means(
        homeEquityFinal,
        ["LOAN", "LTV", "DEBTINC"],
        ["N", "MEAN", "STD", "MEDIAN"],
        classCols=["REASON", "LOAN_OUTCOME"],
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
