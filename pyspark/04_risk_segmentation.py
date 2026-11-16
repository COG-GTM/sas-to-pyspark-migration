"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
Equivalent SAS Program: sas/04_risk_segmentation.sas
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


# SAS: PROC FORMAT value ranges -> ordered (exclusive upper bound, label) lists.
# "low -< 0.60 = 'Low'" means x < 0.60; the final label covers "x - high".
LTV_RISK_FMT = ([(0.60, "Low"), (0.80, "Medium")], "High")
DTI_RISK_FMT = ([(30, "Low"), (40, "Medium"), (50, "High")], "Very High")
RISK_SCORE_FMT = ([(3, "Low Risk"), (5, "Medium Risk"), (7, "High Risk")], "Very High Risk")

# Composite score points: (minimum value, points), checked highest first.
LTV_POINTS = [(0.80, 3.0), (0.60, 1.5)]
DTI_POINTS = [(50, 3.0), (40, 2.0), (30, 1.0)]
DELINQ_POINTS = [(4, 2.0), (2, 1.5), (1, 0.5)]
DEROG_POINTS = [(3, 2.0), (1, 1.0)]


def apply_range_format(colName, fmt):
    """SAS: put(x, fmt.) for a 'low -< a, a -< b, ... b - high' format -> when() chain.

    A missing value stays missing (the DATA step only assigns inside IF x ne .).
    """
    ranges, highLabel = fmt
    expr = F.when(F.col(colName).isNull(), F.lit(None))
    for upper, label in ranges:
        expr = expr.when(F.col(colName) < upper, F.lit(label))
    return expr.otherwise(F.lit(highLabel))


def score_points(colName, points):
    """SAS: if x ne . then do; if x >= a then RISK_SCORE + p; else if ... end;
    -> when() chain; a missing component contributes 0 points."""
    expr = F.when(F.col(colName).isNull(), F.lit(0.0))
    for minimum, pts in points:
        expr = expr.when(F.col(colName) >= minimum, F.lit(pts))
    return expr.otherwise(F.lit(0.0))


def add_risk_segments(df):
    """SAS: data work.home_equity_risk (sas/04_risk_segmentation.sas step 2)."""
    return df \
        .withColumn("LTV_RISK_CAT", apply_range_format("LTV", LTV_RISK_FMT)) \
        .withColumn("DTI_RISK_CAT", apply_range_format("DEBTINC", DTI_RISK_FMT)) \
        .withColumn(
            # SAS: value delinq_risk 0='None' 1='Low' 2-3='Medium' 4-high='High'
            "DELINQ_RISK_CAT",
            F.when(F.col("DELINQ").isNull(), F.lit(None))
            .when(F.col("DELINQ") == 0, F.lit("None"))
            .when(F.col("DELINQ") == 1, F.lit("Low"))
            .when(F.col("DELINQ") <= 3, F.lit("Medium"))
            .otherwise(F.lit("High")),
        ) \
        .withColumn(
            # SAS: RISK_SCORE = 0; RISK_SCORE = RISK_SCORE + ... -> sum of components
            "RISK_SCORE",
            score_points("LTV", LTV_POINTS)
            + score_points("DEBTINC", DTI_POINTS)
            + score_points("DELINQ", DELINQ_POINTS)
            + score_points("DEROG", DEROG_POINTS),
        ) \
        .withColumn("RISK_SEGMENT", apply_range_format("RISK_SCORE", RISK_SCORE_FMT))


def main():
    # SAS: session start -> SparkSession
    spark = SparkSession.builder \
        .appName("HomeEquity_RiskSegmentation") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # SAS: work.home_equity_final (built by 02_data_cleaning.sas) -> rebuilt inline
    homeEquityFinal = clean_home_equity(load_home_equity(spark))

    # ------------------------------------------------------------------
    # Step 1: Define risk bucket formats
    # SAS equivalent:
    #   proc format;
    #       value ltv_risk    low -< 0.60 = 'Low'  0.60 -< 0.80 = 'Medium'  0.80 - high = 'High';
    #       value dti_risk    low -< 30 = 'Low'  30 -< 40 = 'Medium'  40 -< 50 = 'High'
    #                         50 - high = 'Very High';
    #       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'  4 - high = 'High';
    #       value risk_score  low -< 3 = 'Low Risk'  3 -< 5 = 'Medium Risk'
    #                         5 -< 7 = 'High Risk'  7 - high = 'Very High Risk';
    #   run;
    # -> LTV_RISK_FMT / DTI_RISK_FMT / RISK_SCORE_FMT constants above
    #
    # Step 2: Create risk segment variables
    # SAS equivalent:
    #   data work.home_equity_risk;
    #       set work.home_equity_final;
    #       if LTV ne . then do; ... LTV_RISK_CAT ... end;
    #       if DEBTINC ne . then do; ... DTI_RISK_CAT ... end;
    #       if DELINQ ne . then do; ... DELINQ_RISK_CAT ... end;
    #       RISK_SCORE = 0;
    #       /* LTV 0-3, DTI 0-3, DELINQ 0-2, DEROG 0-2 points */
    #       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk'; ...
    #   run;
    # ------------------------------------------------------------------
    homeEquityRisk = add_risk_segments(homeEquityFinal).cache()
    print(f"work.home_equity_risk rows: {homeEquityRisk.count()}")

    # ------------------------------------------------------------------
    # Step 3: Distribution across risk segments
    # SAS equivalent:
    #   proc freq data=work.home_equity_risk;
    #       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
    #   run;
    # ------------------------------------------------------------------
    print_title("Distribution of Loans by Risk Segment")
    for segCol in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        proc_freq(homeEquityRisk, segCol)

    # ------------------------------------------------------------------
    # Step 4: Default rate by risk segment
    # SAS equivalent:
    #   proc means data=work.home_equity_risk n mean std;
    #       class RISK_SEGMENT;
    #       var BAD LOAN LTV DEBTINC;
    #   run;
    # ------------------------------------------------------------------
    print_title("Average Default Rate by Risk Segment")
    proc_means(
        homeEquityRisk,
        ["BAD", "LOAN", "LTV", "DEBTINC"],
        ["N", "MEAN", "STD"],
        classCols=["RISK_SEGMENT"],
    ).show(50, truncate=False)

    # ------------------------------------------------------------------
    # Step 5: Cross-tabulation of risk segments
    # SAS equivalent:
    #   proc freq data=work.home_equity_risk;
    #       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
    #   run;
    # ------------------------------------------------------------------
    # SAS: 3-way TABLES a*b*c -> one b x c table per level of a
    #      -> groupBy(a, b).pivot(c).count(); norow nocol nopercent = counts only.
    #      Rows with any missing table variable are excluded, as in PROC FREQ.
    print_title("Default Rates by LTV Risk and DTI Risk")
    crossSource = homeEquityRisk.filter(
        F.col("LTV_RISK_CAT").isNotNull()
        & F.col("DTI_RISK_CAT").isNotNull()
        & F.col("BAD").isNotNull()
    )
    crossTab = crossSource.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
        .pivot("BAD", [0, 1]) \
        .count() \
        .na.fill(0, subset=["0", "1"]) \
        .withColumnRenamed("0", "BAD_0") \
        .withColumnRenamed("1", "BAD_1") \
        .withColumn("Total", F.col("BAD_0") + F.col("BAD_1"))
    ltvLevels = sorted(r[0] for r in crossTab.select("LTV_RISK_CAT").distinct().collect())
    for tableNo, ltvLevel in enumerate(ltvLevels, start=1):
        print(f"\nTable {tableNo} of DTI_RISK_CAT by BAD, controlling for LTV_RISK_CAT={ltvLevel}")
        stratum = crossTab.filter(F.col("LTV_RISK_CAT") == ltvLevel) \
            .drop("LTV_RISK_CAT") \
            .orderBy("DTI_RISK_CAT")
        # SAS: PROC FREQ prints a Total row under each stratum's table
        totalRow = stratum.agg(
            F.lit("Total").alias("DTI_RISK_CAT"),
            *[F.sum(c).alias(c) for c in ("BAD_0", "BAD_1", "Total")],
        )
        stratum.unionByName(totalRow).show(truncate=False)
    missing = homeEquityRisk.count() - crossSource.count()
    print(f"Frequency Missing = {missing}")

    spark.stop()


if __name__ == "__main__":
    main()
