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
from pyspark.sql.functions import col

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load CSV data
# SAS equivalent: work.home_equity created by sas/01_data_loading.sas
#
# Note: Unlike SAS WORK libraries, each PySpark script is stateless,
# so the source CSV is re-read here.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)
print(f"Rows loaded (work.home_equity): {df.count()}")

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
# ------------------------------------------------------------------
df_clean = df \
    .withColumn(
        "LTV",
        F.when(
            col("VALUE").isNotNull()
            & col("MORTDUE").isNotNull()
            & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        F.when(col("BAD") == 0, "Paid")
         .when(col("BAD") == 1, "Default")
         .otherwise("")
    ) \
    .withColumn("CITY", F.initcap(col("CITY")))  # SAS propcase()

# PySpark has no native column labels/formats; document them as metadata.
# SAS `format LTV percent8.2` is a display format only -- the stored value
# remains the raw ratio (e.g. 0.6543 displays as 65.43%).
columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",
}

print("\n" + "=" * 60)
print("Derived Column Labels/Formats (SAS-style)")
print("=" * 60)
for name, label in columnLabels.items():
    fmt = columnFormats.get(name, "")
    print(f"  {name:12s} -> {label}" + (f"  [format {fmt}]" if fmt else ""))

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
# Note: SAS arrays are replaced by a Python loop over a column list.
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

df_imputed = df_clean
for name in numVars:
    df_imputed = df_imputed.withColumn(
        f"{name}_MISS",
        F.when(col(name).isNull(), 1).otherwise(0)
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
df_filtered = df_imputed.filter(
    col("LOAN").isNotNull()
    & col("VALUE").isNotNull()
    & col("BAD").isNotNull()
)
print(f"\nRows after critical-field filter (work.home_equity_filtered): "
      f"{df_filtered.count()}")


def print_means(data, var_list, stats, title):
    """Compute and print a PROC MEANS-style table for the given statistics."""
    pct_levels = {"p1": 0.01, "p5": 0.05, "p25": 0.25, "median": 0.5,
                  "p75": 0.75, "p95": 0.95, "p99": 0.99}
    pct_stats = [s for s in stats if s in pct_levels]

    agg_exprs = []
    for name in var_list:
        c = col(name)
        agg_exprs += [
            F.count(c).alias(f"{name}__n"),
            F.count(F.when(c.isNull(), 1)).alias(f"{name}__nmiss"),
            F.mean(c).alias(f"{name}__mean"),
            F.stddev(c).alias(f"{name}__std"),
            F.min(c).alias(f"{name}__min"),
            F.max(c).alias(f"{name}__max"),
        ]
        if pct_stats:
            agg_exprs.append(
                F.percentile_approx(
                    c, [pct_levels[s] for s in pct_stats]
                ).alias(f"{name}__pct")
            )
    row = data.agg(*agg_exprs).collect()[0]

    def fmt(value, stat):
        if value is None:
            return "."
        if stat in ("n", "nmiss"):
            return str(int(value))
        return f"{value:.4f}"

    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    header = f"{'Variable':10s}" + "".join(f"{s:>14s}" for s in stats)
    print(header)
    print("-" * len(header))
    for name in var_list:
        pct_values = dict(zip(pct_stats, row[f"{name}__pct"] or [])) \
            if pct_stats else {}
        cells = []
        for s in stats:
            value = pct_values.get(s) if s in pct_levels else row[f"{name}__{s}"]
            cells.append(f"{fmt(value, s):>14s}")
        print(f"{name:10s}" + "".join(cells))


# ------------------------------------------------------------------
# Step 4: Check for outliers using summary statistics
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#   title;
#
# Note: df.summary() only provides p25/p50/p75, so percentiles are
# computed with F.percentile_approx alongside the other aggregates.
# ------------------------------------------------------------------
print_means(
    df_filtered,
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"],
    ["n", "nmiss", "mean", "std", "min", "p1", "p5", "p25", "median",
     "p75", "p95", "p99", "max"],
    "Summary Statistics for Outlier Detection",
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
# ------------------------------------------------------------------
df_final = df_filtered.filter(
    (col("LTV") > 0) & (col("LTV") < 5)
    & (col("LOAN") > 0)
    & (col("VALUE") > 0)
)
print(f"\nRows in final clean dataset (work.home_equity_final): "
      f"{df_final.count()}")

print_means(
    df_final,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["n", "nmiss", "mean", "std", "min", "max"],
    "Clean Dataset Summary",
)

print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
df_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
