"""
PySpark Script: 03_aggregation_reporting.py
Purpose: Generate frequency tables, summary statistics, and reports
Equivalent SAS Program: sas/03_aggregation_reporting.sas

SAS PROC -> PySpark mapping used in this script:
  PROC FREQ     -> groupBy(col).count() / one-way & two-way frequencies
  PROC MEANS    -> groupBy(class).agg(count/mean/median/std/min/max)
  PROC TABULATE -> grouped pivot with row/column margins ("all='Total'")
  PROC SQL      -> spark.sql(...) over a createOrReplaceTempView temp view
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, first, lit, mean, stddev, percentile_approx, when,
    round as spark_round, min as spark_min, max as spark_max,
)

# ------------------------------------------------------------------
# Self-contained SparkSession (no shared helper module)
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("HomeEquity_AggregationReporting") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Reproduce work.home_equity_final (see sas/02_data_cleaning.sas):
#   - LTV = MORTDUE / VALUE where VALUE, MORTDUE present and VALUE > 0
#   - LOAN_OUTCOME derived from BAD (0 -> 'Paid', 1 -> 'Default')
#   - keep rows with LOAN/VALUE/BAD non-missing
#   - drop outliers: 0 < LTV < 5, LOAN > 0, VALUE > 0
# ------------------------------------------------------------------
raw = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

home_equity_final = raw \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ),
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
        & (col("LTV") > 0) & (col("LTV") < 5)
        & (col("LOAN") > 0) & (col("VALUE") > 0)
    )

home_equity_final.cache()

# Register once for the PROC SQL step below.
home_equity_final.createOrReplaceTempView("home_equity_final")


def _banner(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


# ==================================================================
# Step 1: Frequency tables for categorical variables
#   SAS:
#     proc freq data=work.home_equity_final;
#         tables JOB REASON LOAN_OUTCOME REGION / nocum;
#     run;
#   PROC FREQ -> groupBy(col).count(); "nocum" => show Frequency & Percent
#   only (no cumulative columns). SAS excludes missing class values.
# ==================================================================
_banner("Frequency Tables (PROC FREQ): JOB, REASON, LOAN_OUTCOME, REGION")

total_rows = home_equity_final.count()
for cat_col in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
    print(f"\n--- {cat_col} ---")
    home_equity_final \
        .filter(col(cat_col).isNotNull()) \
        .groupBy(cat_col) \
        .agg(
            count(lit(1)).alias("Frequency"),
            spark_round(count(lit(1)) / lit(total_rows) * 100, 2).alias("Percent"),
        ) \
        .orderBy(col(cat_col).asc()) \
        .show(truncate=False)


# ==================================================================
# Step 2: Summary statistics by loan outcome
#   SAS:
#     proc means data=work.home_equity_final n mean median std min max;
#         class LOAN_OUTCOME;
#         var LOAN MORTDUE VALUE DEBTINC;
#     run;
#   PROC MEANS -> groupBy(class).agg(...); one tidy row per (class, var).
# ==================================================================
def proc_means(source: DataFrame, class_cols, var_cols) -> DataFrame:
    """Emulate PROC MEANS (n mean median std min max) with a CLASS statement.

    Produces one long-format row per (class level(s), analysis variable),
    matching the per-variable blocks SAS prints for each class level.
    Missing class levels are excluded, as PROC MEANS does by default.
    """
    non_null = source
    for c in class_cols:
        non_null = non_null.filter(col(c).isNotNull())

    per_var = []
    for v in var_cols:
        per_var.append(
            non_null.groupBy(*class_cols).agg(
                lit(v).alias("Variable"),
                count(col(v)).alias("N"),
                spark_round(mean(col(v)), 2).alias("Mean"),
                spark_round(percentile_approx(col(v), 0.5), 2).alias("Median"),
                spark_round(stddev(col(v)), 2).alias("Std"),
                spark_round(spark_min(col(v)), 2).alias("Min"),
                spark_round(spark_max(col(v)), 2).alias("Max"),
            )
        )

    combined = per_var[0]
    for part in per_var[1:]:
        combined = combined.unionByName(part)
    return combined.orderBy(*class_cols, "Variable")


_banner("Summary Statistics by Loan Outcome (PROC MEANS)")
proc_means(
    home_equity_final,
    class_cols=["LOAN_OUTCOME"],
    var_cols=["LOAN", "MORTDUE", "VALUE", "DEBTINC"],
).show(truncate=False)


# ==================================================================
# Step 3: Cross-tabulation of default rates by JOB and REGION
#   SAS:
#     proc tabulate data=work.home_equity_final;
#         class JOB REGION;
#         var BAD;
#         table JOB all='Total',
#               REGION * BAD * (n mean*f=percent8.2)
#               all='Total' * BAD * (n mean*f=percent8.2);
#     run;
#   PROC TABULATE -> grouped pivot; mean(BAD) is the default rate, with
#   row ("Total" JOB) and column ("Total" REGION) margins, i.e. all='Total'.
# ==================================================================
_banner("Default Rate (%) by Job Category x Region (PROC TABULATE)")

tab_src = home_equity_final.filter(col("JOB").isNotNull() & col("REGION").isNotNull())

# Cell values + row margin (REGION='Total') + column margin (JOB='Total')
# + grand total, unioned together so a single pivot renders the full table.
cells = tab_src.groupBy("JOB", "REGION")
job_margin = tab_src.groupBy("JOB").agg(  # REGION collapsed -> 'Total'
    count(lit(1)).alias("N"), (mean("BAD") * 100).alias("Rate"),
).withColumn("REGION", lit("Total"))
region_margin = tab_src.groupBy("REGION").agg(  # JOB collapsed -> 'Total'
    count(lit(1)).alias("N"), (mean("BAD") * 100).alias("Rate"),
).withColumn("JOB", lit("Total"))
grand = tab_src.agg(
    count(lit(1)).alias("N"), (mean("BAD") * 100).alias("Rate"),
).withColumn("JOB", lit("Total")).withColumn("REGION", lit("Total"))

tab_long = cells.agg(
    count(lit(1)).alias("N"), (mean("BAD") * 100).alias("Rate"),
).unionByName(job_margin).unionByName(region_margin).unionByName(grand)

region_order = [
    r["REGION"] for r in tab_src.select("REGION").distinct().orderBy("REGION").collect()
] + ["Total"]

print("\nN (loan count) by JOB x REGION:")
tab_long.groupBy("JOB").pivot("REGION", region_order).agg(first("N")) \
    .orderBy("JOB").show(truncate=False)

print("Default rate (%) by JOB x REGION:")
tab_long.groupBy("JOB").pivot("REGION", region_order) \
    .agg(spark_round(first("Rate"), 2)) \
    .orderBy("JOB").show(truncate=False)


# ==================================================================
# Step 4: Top 10 states by average loan amount
#   SAS:
#     proc sql outobs=10;
#         select STATE, count(*) as num_loans,
#                mean(LOAN) as avg_loan format=dollar12.,
#                mean(VALUE) as avg_property_value format=dollar12.,
#                mean(BAD) as default_rate format=percent8.2
#         from work.home_equity_final
#         group by STATE
#         having count(*) >= 10
#         order by avg_loan desc;
#     quit;
#   PROC SQL -> spark.sql(...) over the temp view; outobs=10 -> LIMIT 10.
# ==================================================================
_banner("Top 10 States by Average Loan Amount (PROC SQL)")
spark.sql(
    """
    SELECT
        STATE,
        COUNT(*)                       AS num_loans,
        ROUND(AVG(LOAN), 2)            AS avg_loan,
        ROUND(AVG(VALUE), 2)           AS avg_property_value,
        ROUND(AVG(BAD) * 100, 2)       AS default_rate_pct
    FROM home_equity_final
    GROUP BY STATE
    HAVING COUNT(*) >= 10
    ORDER BY avg_loan DESC
    LIMIT 10
    """
).show(truncate=False)


# ==================================================================
# Step 5: Loan distribution by reason and outcome
#   SAS:
#     proc means data=work.home_equity_final n mean std median;
#         class REASON LOAN_OUTCOME;
#         var LOAN LTV DEBTINC;
#     run;
#   PROC MEANS with two CLASS vars -> groupBy(REASON, LOAN_OUTCOME).agg(...).
# ==================================================================
_banner("Loan Amount Distribution by Reason and Outcome (PROC MEANS)")
proc_means(
    home_equity_final,
    class_cols=["REASON", "LOAN_OUTCOME"],
    var_cols=["LOAN", "LTV", "DEBTINC"],
).show(truncate=False)

spark.stop()
