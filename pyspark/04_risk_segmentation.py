"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalent)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas

Run from the repository root:
    python pyspark/04_risk_segmentation.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

DATA_PATH = "data/home_equity.csv"

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk    low -< 0.60 = 'Low'   0.60 -< 0.80 = 'Medium'
#                         0.80 - high = 'High';
#       value dti_risk    low -< 30 = 'Low'  30 -< 40 = 'Medium'
#                         40 -< 50 = 'High'  50 - high = 'Very High';
#       value delinq_risk 0 = 'None'  1 = 'Low'  2 - 3 = 'Medium'
#                         4 - high = 'High';
#       value risk_score  low -< 3 = 'Low Risk'  3 -< 5 = 'Medium Risk'
#                         5 -< 7 = 'High Risk'   7 - high = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each format is expressed as an ordered
# list of (upper_bound_exclusive, label) pairs; the last pair (bound None)
# is the open-ended "- high" range. apply_format() turns a spec into a
# when/otherwise chain. Missing input stays missing (null), as in SAS.
# ------------------------------------------------------------------
LTV_RISK_FMT = [(0.60, "Low"), (0.80, "Medium"), (None, "High")]
DTI_RISK_FMT = [(30, "Low"), (40, "Medium"), (50, "High"), (None, "Very High")]
RISK_SCORE_FMT = [
    (3, "Low Risk"), (5, "Medium Risk"), (7, "High Risk"), (None, "Very High Risk"),
]


def apply_format(c: Column, spec) -> Column:
    """Map a numeric column onto range labels (SAS `value` format)."""
    expr = None
    for upper, label in spec:
        if upper is None:
            expr = expr.otherwise(F.lit(label))
        else:
            cond = c < upper
            expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return F.when(c.isNotNull(), expr)


def delinq_risk(c: Column) -> Column:
    """SAS `delinq_risk` format: 0=None, 1=Low, 2-3=Medium, 4+=High."""
    return (
        F.when(c.isNull(), F.lit(None).cast("string"))
        .when(c == 0, F.lit("None"))
        .when(c == 1, F.lit("Low"))
        .when(c <= 3, F.lit("Medium"))
        .otherwise(F.lit("High"))
    )


def load_home_equity_final(spark: SparkSession, path: str = DATA_PATH) -> DataFrame:
    """Recreate work.home_equity_final (upstream 01 + 02 steps used here).

    SAS equivalent (01_data_loading.sas / 02_data_cleaning.sas):
        proc import datafile=... out=work.home_equity; run;
        data work.home_equity_clean;  LTV = MORTDUE / VALUE; ... run;
        data work.home_equity_filtered;
            if LOAN ne . and VALUE ne . and BAD ne .; run;
        data work.home_equity_final;
            if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0; run;

    Only the columns program 04 depends on are derived (LTV, LOAN_OUTCOME).
    """
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    ).withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid")).when(F.col("BAD") == 1, F.lit("Default")),
    )
    return df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    ).filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       if LTV ne . then do; ... LTV_RISK_CAT = 'Low'|'Medium'|'High'; end;
#       if DEBTINC ne . then do; ... DTI_RISK_CAT = ...; end;
#       if DELINQ ne . then do; ... DELINQ_RISK_CAT = ...; end;
#       RISK_SCORE = 0;
#       if LTV ne . then ... RISK_SCORE + 3 | 1.5;
#       if DEBTINC ne . then ... RISK_SCORE + 3 | 2 | 1;
#       if DELINQ ne . then ... RISK_SCORE + 2 | 1.5 | 0.5;
#       if DEROG ne . then ... RISK_SCORE + 2 | 1;
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk'; ...
#   run;
#
# Note: the sequential RISK_SCORE accumulation becomes a sum of four
# independent component columns; each component is 0 when its input is
# missing, matching the `if X ne . then do;` guards.
# ------------------------------------------------------------------
RISK_LABELS = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}


def add_risk_segments(df: DataFrame) -> DataFrame:
    ltv, dti, delinq, derog = F.col("LTV"), F.col("DEBTINC"), F.col("DELINQ"), F.col("DEROG")

    ltvPts = F.when(ltv >= 0.80, 3.0).when(ltv >= 0.60, 1.5).otherwise(0.0)
    dtiPts = F.when(dti >= 50, 3.0).when(dti >= 40, 2.0).when(dti >= 30, 1.0).otherwise(0.0)
    delinqPts = F.when(delinq >= 4, 2.0).when(delinq >= 2, 1.5).when(delinq == 1, 0.5).otherwise(0.0)
    derogPts = F.when(derog >= 3, 2.0).when(derog >= 1, 1.0).otherwise(0.0)

    return (
        df.withColumn("LTV_RISK_CAT", apply_format(ltv, LTV_RISK_FMT))
        .withColumn("DTI_RISK_CAT", apply_format(dti, DTI_RISK_FMT))
        .withColumn("DELINQ_RISK_CAT", delinq_risk(delinq))
        .withColumn("RISK_SCORE", ltvPts + dtiPts + delinqPts + derogPts)
        .withColumn("RISK_SEGMENT", apply_format(F.col("RISK_SCORE"), RISK_SCORE_FMT))
    )


# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#
# Note: like PROC FREQ, missing levels are excluded from Frequency/Percent
# and reported separately as "Frequency Missing".
# ------------------------------------------------------------------
def one_way_freq(df: DataFrame, var: str) -> DataFrame:
    nonMissing = df.filter(F.col(var).isNotNull())
    total = nonMissing.count()
    return (
        nonMissing.groupBy(var).agg(F.count(F.lit(1)).alias("Frequency"))
        .withColumn("Percent", F.round(F.col("Frequency") * 100.0 / total, 2))
        .orderBy(var)
    )


# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#
# Note: N = count of non-missing values (F.count), Std = sample standard
# deviation (stddev_samp, SAS VARDEF=DF default). Mean of BAD is the
# default rate.
# ------------------------------------------------------------------
MEANS_VARS = ["BAD", "LOAN", "LTV", "DEBTINC"]


def means_by_segment(df: DataFrame) -> DataFrame:
    aggs = [F.count(F.lit(1)).alias("N_Obs")]
    for v in MEANS_VARS:
        aggs += [
            F.count(v).alias(f"{v}_N"),
            F.round(F.mean(v), 4).alias(f"{v}_Mean"),
            F.round(F.stddev_samp(v), 4).alias(f"{v}_Std"),
        ]
    return df.groupBy("RISK_SEGMENT").agg(*aggs).orderBy("RISK_SEGMENT")


# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#
# Note: a three-way TABLES request produces one DTI_RISK_CAT x BAD table per
# LTV_RISK_CAT level (frequencies only). Here it is a single groupBy + pivot
# on BAD with a derived default rate column; rows with a missing LTV or DTI
# category are excluded, as in PROC FREQ.
# ------------------------------------------------------------------
def ltv_dti_crosstab(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("LTV_RISK_CAT").isNotNull() & F.col("DTI_RISK_CAT").isNotNull())
        .groupBy("LTV_RISK_CAT", "DTI_RISK_CAT")
        .pivot("BAD", [0, 1])
        .count()
        .na.fill(0)
        .withColumnRenamed("0", "BAD_0")
        .withColumnRenamed("1", "BAD_1")
        .withColumn("Total", F.col("BAD_0") + F.col("BAD_1"))
        .withColumn("Default_Rate", F.round(F.col("BAD_1") / F.col("Total"), 4))
        .orderBy("LTV_RISK_CAT", "DTI_RISK_CAT")
    )


def main():
    # SAS equivalent: Starting a SAS session
    spark = SparkSession.builder \
        .appName("HomeEquity_RiskSegmentation") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dfFinal = load_home_equity_final(spark)
    dfRisk = add_risk_segments(dfFinal).cache()
    print(f"work.home_equity_risk: {dfRisk.count()} rows, {len(dfRisk.columns)} columns")

    print("\n" + "=" * 60)
    print("Risk variable labels (SAS LABEL statement)")
    print("=" * 60)
    for c, label in RISK_LABELS.items():
        print(f"  {c:16s} -> {label}")

    print("\n" + "=" * 60)
    print("Distribution of Loans by Risk Segment (PROC FREQ / nocum)")
    print("=" * 60)
    for var in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        one_way_freq(dfRisk, var).show(truncate=False)
        missing = dfRisk.filter(F.col(var).isNull()).count()
        print(f"  {var}: Frequency Missing = {missing}\n")

    print("=" * 60)
    print("Average Default Rate by Risk Segment (PROC MEANS n mean std)")
    print("=" * 60)
    means_by_segment(dfRisk).show(truncate=False, vertical=True)

    print("=" * 60)
    print("Default Rates by LTV Risk and DTI Risk (PROC FREQ 3-way)")
    print("=" * 60)
    ltv_dti_crosstab(dfRisk).show(50, truncate=False)

    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()
