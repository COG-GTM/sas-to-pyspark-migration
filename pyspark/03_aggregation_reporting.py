"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas
  - PROC FREQ for categorical distributions
  - PROC MEANS for numeric summaries by group
  - PROC TABULATE for cross-tabulations
  - PROC SQL for ad hoc queries
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Build work.home_equity_final from the raw CSV
# SAS equivalent (prerequisite datasets created by sas/01 and sas/02):
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#   run;
#   data work.home_equity_filtered;
#       set work.home_equity_clean;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: Only the columns this report uses (LTV, LOAN_OUTCOME) are derived;
# the CITY propcase and *_MISS flags from sas/02 don't affect any output here.
# Spark null comparisons evaluate to null and drop the row, matching SAS,
# where missing (.) fails "LTV > 0", "LOAN > 0" and "VALUE > 0".
# ------------------------------------------------------------------
home_equity = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

home_equity_clean = home_equity.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
)

home_equity_filtered = home_equity_clean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

home_equity_final = home_equity_filtered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
).cache()

columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
    "NUM_LOANS": "Number of Loans",
    "AVG_LOAN": "Average Loan Amount",
    "AVG_PROPERTY_VALUE": "Average Property Value",
    "DEFAULT_RATE": "Default Rate",
}

columnFormats = {
    "LTV": "percent8.2",
    "AVG_LOAN": "dollar12.",
    "AVG_PROPERTY_VALUE": "dollar12.",
    "DEFAULT_RATE": "percent8.2",
    "PCT_BAD": "percent8.2",
}

print("=" * 60)
print("Input dataset: work.home_equity_final")
print("=" * 60)
print(f"Rows in work.home_equity:          {home_equity.count()}")
print(f"Rows in work.home_equity_filtered: {home_equity_filtered.count()}")
print(f"Rows in work.home_equity_final:    {home_equity_final.count()}")


def fmtPercent(c):
    """SAS percent8.2: 0.1234 -> '12.34%'."""
    return F.format_string("%.2f%%", F.round(c * 100, 2))


def fmtDollar(c):
    """SAS dollar12.: 12345.6 -> '$12,346'."""
    return F.concat(lit("$"), F.format_number(F.round(c, 0), 0))


def procFreq(df, var):
    """PROC FREQ one-way table with /nocum: Frequency and Percent per level.

    SAS excludes missing values from the table and the percent denominator
    and reports them as 'Frequency Missing'.
    """
    nonMissing = df.filter(col(var).isNotNull())
    total = nonMissing.count()
    missing = df.count() - total
    table = nonMissing.groupBy(var) \
        .agg(F.count(lit(1)).alias("Frequency")) \
        .withColumn("Percent", F.round(col("Frequency") * 100.0 / total, 2)) \
        .orderBy(var)
    return table, missing


def procMeans(df, classVars, analysisVars, stats):
    """PROC MEANS with CLASS: one row per class level per analysis variable.

    SAS drops observations with a missing CLASS value (no MISSING option).
    N counts non-missing values; Std Dev is the sample standard deviation.
    Median uses Spark's exact percentile (same as SAS for the 50th percentile).
    """
    statFns = {
        "N": lambda c: F.count(c),
        "Mean": lambda c: F.mean(c),
        "Median": lambda c: F.median(c),
        "Std Dev": lambda c: F.stddev_samp(c),
        "Minimum": lambda c: F.min(c),
        "Maximum": lambda c: F.max(c),
    }
    classed = df
    for c in classVars:
        classed = classed.filter(col(c).isNotNull())

    aggExprs = [F.count(lit(1)).alias("N Obs")]
    for v in analysisVars:
        for s in stats:
            aggExprs.append(statFns[s](col(v)).alias(f"{v}__{s}"))
    wide = classed.groupBy(*classVars).agg(*aggExprs)

    long = None
    for i, v in enumerate(analysisVars):
        part = wide.select(
            *classVars,
            "N Obs",
            lit(i).alias("_VAR_ORDER"),
            lit(v).alias("Variable"),
            *[F.round(col(f"{v}__{s}"), 4).alias(s) for s in stats]
        )
        long = part if long is None else long.unionByName(part)
    return long.orderBy(*classVars, "_VAR_ORDER").drop("_VAR_ORDER")


# ------------------------------------------------------------------
# Step 1: Frequency tables for categorical variables
# SAS equivalent:
#   title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
#   proc freq data=work.home_equity_final;
#       tables JOB REASON LOAN_OUTCOME REGION / nocum;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region")
print("=" * 60)
for var in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    freqTable, freqMissing = procFreq(home_equity_final, var)
    print(f"\nPROC FREQ: {var}")
    freqTable.show(truncate=False)
    if freqMissing > 0:
        print(f"Frequency Missing = {freqMissing}")

# ------------------------------------------------------------------
# Step 2: Summary statistics by loan outcome
# SAS equivalent:
#   title "Summary Statistics by Loan Outcome";
#   proc means data=work.home_equity_final
#       n mean median std min max;
#       class LOAN_OUTCOME;
#       var LOAN MORTDUE VALUE DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics by Loan Outcome")
print("=" * 60)
outcome_summary = procMeans(
    home_equity_final,
    ["LOAN_OUTCOME"],
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
    ["N", "Mean", "Median", "Std Dev", "Minimum", "Maximum"]
)
outcome_summary.show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Cross-tabulation of default rates by JOB and REGION
# SAS equivalent:
#   title "Default Rates by Job Category and Region";
#   proc tabulate data=work.home_equity_final;
#       class JOB REGION;
#       var BAD;
#       table JOB all='Total',
#             REGION * BAD * (n mean*f=percent8.2)
#             all='Total' * BAD * (n mean*f=percent8.2);
#   run;
#
# Note: PROC TABULATE drops any observation with a missing CLASS value
# (JOB or REGION) from the whole table, including the 'Total' row/column.
# The ALL dimensions are built as extra 'Total' groups and the layout
# uses groupBy().pivot(). Mean of BAD = default rate.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by Job Category and Region")
print("=" * 60)
tabulateInput = home_equity_final.filter(
    col("JOB").isNotNull() & col("REGION").isNotNull()
)


def badStats(df, rowKey, colKey):
    return df.groupBy(rowKey, colKey).agg(
        F.count("BAD").alias("N"),
        F.mean("BAD").alias("PCT_BAD")
    )


tabulateCells = badStats(
    tabulateInput, col("JOB"), col("REGION")
).unionByName(
    badStats(tabulateInput, col("JOB"), lit("Total").alias("REGION"))
).unionByName(
    badStats(tabulateInput, lit("Total").alias("JOB"), col("REGION"))
).unionByName(
    badStats(tabulateInput, lit("Total").alias("JOB"), lit("Total").alias("REGION"))
)

regions = sorted(
    r["REGION"] for r in tabulateInput.select("REGION").distinct().collect()
)
default_rates_tab = tabulateCells \
    .withColumn("PCT_BAD", fmtPercent(col("PCT_BAD"))) \
    .groupBy("JOB") \
    .pivot("REGION", regions + ["Total"]) \
    .agg(F.first("N").alias("N"), F.first("PCT_BAD").alias("Mean_BAD")) \
    .orderBy(when(col("JOB") == "Total", 1).otherwise(0), "JOB")
default_rates_tab.show(truncate=False)

# ------------------------------------------------------------------
# Step 4: Top 10 states by average loan amount using PROC SQL
# SAS equivalent:
#   title "Top 10 States by Average Loan Amount";
#   proc sql outobs=10;
#       select STATE,
#              count(*) as num_loans,
#              mean(LOAN) as avg_loan format=dollar12.,
#              mean(VALUE) as avg_property_value format=dollar12.,
#              mean(BAD) as default_rate format=percent8.2
#       from work.home_equity_final
#       group by STATE
#       having count(*) >= 10
#       order by avg_loan desc;
#   quit;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Top 10 States by Average Loan Amount")
print("=" * 60)
home_equity_final.createOrReplaceTempView("home_equity_final")
top_states = spark.sql("""
    SELECT STATE,
           COUNT(*)   AS NUM_LOANS,
           AVG(LOAN)  AS AVG_LOAN,
           AVG(VALUE) AS AVG_PROPERTY_VALUE,
           AVG(BAD)   AS DEFAULT_RATE
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY AVG_LOAN DESC
    LIMIT 10
""")
top_states.select(
    "STATE",
    "NUM_LOANS",
    fmtDollar(col("AVG_LOAN")).alias("AVG_LOAN"),
    fmtDollar(col("AVG_PROPERTY_VALUE")).alias("AVG_PROPERTY_VALUE"),
    fmtPercent(col("DEFAULT_RATE")).alias("DEFAULT_RATE")
).show(truncate=False)

# ------------------------------------------------------------------
# Step 5: Additional reporting - Loan distribution by reason and outcome
# SAS equivalent:
#   title "Loan Amount Distribution by Reason and Outcome";
#   proc means data=work.home_equity_final n mean std median;
#       class REASON LOAN_OUTCOME;
#       var LOAN LTV DEBTINC;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Loan Amount Distribution by Reason and Outcome")
print("=" * 60)
reason_outcome_summary = procMeans(
    home_equity_final,
    ["REASON", "LOAN_OUTCOME"],
    ["LOAN", "LTV", "DEBTINC"],
    ["N", "Mean", "Std Dev", "Median"]
)
reason_outcome_summary.show(50, truncate=False)

# Clean up
spark.stop()
