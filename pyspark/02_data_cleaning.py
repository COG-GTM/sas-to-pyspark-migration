"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
    - Create derived columns (LTV, LOAN_OUTCOME)
    - Handle missing values
    - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load the source data (produced by 01_data_loading in SAS as work.home_equity)
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)


def printSummary(data, variables, title, percentiles=None):
    """Print PROC MEANS-style statistics: n, nmiss, mean, std, min, [percentiles], max."""
    percentiles = percentiles or {}

    described = {
        row["summary"]: row.asDict()
        for row in data.describe(*variables).collect()
    }

    aggExprs = [
        F.sum(F.col(v).isNull().cast("int")).alias(f"{v}__nmiss")
        for v in variables
    ]
    if percentiles:
        aggExprs += [
            F.percentile_approx(F.col(v), list(percentiles.values()), 10000)
            .alias(f"{v}__pct")
            for v in variables
        ]
    extra = data.agg(*aggExprs).collect()[0]

    headers = ["Variable", "N", "NMiss", "Mean", "Std Dev", "Min"] \
        + list(percentiles.keys()) + ["Max"]

    def fmt(value):
        return "." if value is None else f"{float(value):.4f}"

    rows = []
    for v in variables:
        pctValues = extra[f"{v}__pct"] if percentiles else []
        pctValues = pctValues or [None] * len(percentiles)
        rows.append(
            [v, described["count"][v], str(extra[f"{v}__nmiss"])]
            + [fmt(described[s][v]) for s in ("mean", "stddev", "min")]
            + [fmt(p) for p in pctValues]
            + [fmt(described["max"][v])]
        )

    widths = [max(len(str(r[i])) for r in [headers] + rows) for i in range(len(headers))]
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    print("  ".join(h.rjust(w) for h, w in zip(headers, widths)))
    for r in rows:
        print("  ".join(str(c).rjust(w) for c, w in zip(r, widths)))


# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else
#           LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#       label LTV = "Loan to Value Ratio"
#             LOAN_OUTCOME = "Loan Outcome";
#       format LTV percent8.2;
#   run;
#
# Note: SAS labels/formats have no native PySpark equivalent; LTV is
# stored as a raw ratio (format as a percentage at display time).
# ------------------------------------------------------------------
dfClean = df \
    .withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull()
            & F.col("MORTDUE").isNotNull()
            & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE")
        ).otherwise(F.lit(None).cast("double"))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid"))
        .when(F.col("BAD") == 1, F.lit("Default"))
        .otherwise(F.lit(""))
    ) \
    .withColumn("CITY", F.initcap(F.col("CITY")))

# ------------------------------------------------------------------
# Step 2: Flag missing values
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
# Note: The SAS array/DO loop becomes a single select over a Python list,
# which Spark evaluates column-wise rather than row-by-row.
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean.select(
    "*",
    *[
        F.when(F.col(v).isNull(), F.lit(1)).otherwise(F.lit(0)).alias(f"{v}_MISS")
        for v in numVars
    ]
)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    F.col("LOAN").isNotNull()
    & F.col("VALUE").isNotNull()
    & F.col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
#
# Note: df.describe() supplies n/mean/std/min/max; nmiss and percentiles
# come from a single aggregation using F.percentile_approx.
# ------------------------------------------------------------------
outlierVars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
outlierPercentiles = {
    "P1": 0.01,
    "P5": 0.05,
    "P25": 0.25,
    "Median": 0.50,
    "P75": 0.75,
    "P95": 0.95,
    "P99": 0.99,
}

printSummary(
    dfFiltered,
    outlierVars,
    "Summary Statistics for Outlier Detection",
    outlierPercentiles
)

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
#   title;
#
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
#
# Note: As in SAS, rows with missing LTV fail the LTV comparison and are
# dropped (Spark comparisons with null evaluate to null, i.e. not kept).
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (F.col("LTV") > 0) & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

printSummary(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    "Clean Dataset Summary"
)

print("\n" + "=" * 60)
print("First 10 Observations of Clean Dataset (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
