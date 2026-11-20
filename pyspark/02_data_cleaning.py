"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
  - Create derived columns (LTV, LOAN_OUTCOME)
  - Flag missing values
  - Filter records and check for outliers
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "home_equity.csv"
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load the raw dataset (equivalent to work.home_equity from 01_data_loading)
df = spark.read.csv(
    DATA_PATH,
    header=True,
    inferSchema=True
)


def propcase(column):
    """Replicate SAS propcase(): capitalize the first letter of every word,
    lowercase the rest. initcap() only splits on blanks, so the other default
    SAS delimiters (- / ( . tab) are handled explicitly (e.g. winston-salem)."""
    result = F.initcap(column)
    for delimiter in ["-", "/", "(", ".", "\t"]:
        result = F.array_join(
            F.transform(
                F.split(result, re.escape(delimiter)),
                lambda w: F.concat(F.upper(F.substring(w, 1, 1)), F.substring(w, 2, 1 << 30))
            ),
            delimiter
        )
    return result


def procMeans(data, variables, stats):
    """Print PROC MEANS-style statistics (one row per variable).

    std is the sample standard deviation (SAS VARDEF=DF default). Percentiles
    use Spark's exact percentile(), which interpolates linearly; SAS's default
    PCTLDEF=5 can differ slightly at non-integer ranks.
    """
    percentiles = {"p1": 0.01, "p5": 0.05, "p25": 0.25, "median": 0.5,
                   "p75": 0.75, "p95": 0.95, "p99": 0.99}
    aggregates = {
        "n": lambda c: F.count(col(c)),
        "nmiss": lambda c: F.sum(when(col(c).isNull(), 1).otherwise(0)),
        "mean": lambda c: F.mean(col(c)),
        "std": lambda c: F.stddev_samp(col(c)),
        "min": lambda c: F.min(col(c)),
        "max": lambda c: F.max(col(c)),
    }
    for name, p in percentiles.items():
        aggregates[name] = lambda c, p=p: F.percentile(col(c), lit(p))

    exprs = [aggregates[s](v).alias(f"{v}__{s}") for v in variables for s in stats]
    row = data.agg(*exprs).collect()[0]

    print(f"{'Variable':10s}" + "".join(f"{s:>14s}" for s in stats))
    for v in variables:
        cells = []
        for s in stats:
            value = row[f"{v}__{s}"]
            if value is None:
                cells.append(f"{'.':>14s}")
            elif s in ("n", "nmiss"):
                cells.append(f"{int(value):>14d}")
            else:
                cells.append(f"{value:>14.4f}")
        print(f"{v:10s}" + "".join(cells))


# ------------------------------------------------------------------
# Step 1: Create derived columns
# SAS equivalent:
#   data work.home_equity_clean;
#       length LOAN_OUTCOME $ 7;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#       label LTV = "Loan to Value Ratio"
#             LOAN_OUTCOME = "Loan Outcome";
#       format LTV percent8.2;
#   run;
#
# Note: PySpark has no column labels or display formats. Labels are kept
# in a dictionary (as in 01_data_loading.py); percent8.2 is applied only
# when displaying LTV, not to the stored value.
# ------------------------------------------------------------------
dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() &
            col("MORTDUE").isNotNull() &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(None))
    ) \
    .withColumn("CITY", propcase(col("CITY")))

columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",
}

print("=" * 60)
print("Derived Columns (DATA step home_equity_clean)")
print("=" * 60)
for name, label in columnLabels.items():
    fmt = columnFormats.get(name, "")
    print(f"  {name:12s} -> {label}" + (f" (format {fmt})" if fmt else ""))
dfClean.select(
    "BAD", "LOAN_OUTCOME", "MORTDUE", "VALUE", "LTV",
    when(col("LTV").isNotNull(), F.format_string("%.2f%%", col("LTV") * 100))
    .alias("LTV_PCT"),
    "CITY"
).show(5, truncate=False)

# ------------------------------------------------------------------
# Step 2: Flag missing values
# SAS equivalent:
#   data work.home_equity_imputed;
#       set work.home_equity_clean;
#       array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#       array num_flags{8} LOAN_MISS MORTDUE_MISS ... NINQ_MISS;
#       do i = 1 to 8;
#           if num_vars{i} = . then num_flags{i} = 1;
#           else num_flags{i} = 0;
#       end;
#       drop i;
#   run;
#
# Note: The SAS array/DO loop becomes a Python loop over column names.
# ------------------------------------------------------------------
numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

dfImputed = dfClean
for varName in numVars:
    dfImputed = dfImputed.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), 1).otherwise(0)
    )

print("\n" + "=" * 60)
print("Missing Value Flags (array processing)")
print("=" * 60)
dfImputed.agg(
    *[F.sum(f"{v}_MISS").alias(f"{v}_MISS") for v in numVars]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
dfFiltered = dfImputed.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

print("\n" + "=" * 60)
print("Filtered Dataset (LOAN, VALUE, BAD non-missing)")
print("=" * 60)
print(f"Rows before filter: {dfImputed.count()}")
print(f"Rows after filter:  {dfFiltered.count()}")

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   title "Summary Statistics for Outlier Detection";
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (PROC MEANS)")
print("=" * 60)
procMeans(
    dfFiltered,
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"],
    ["n", "nmiss", "mean", "std", "min", "p1", "p5", "p25", "median",
     "p75", "p95", "p99", "max"]
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
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
print("\n" + "=" * 60)
print("Clean Dataset Summary (PROC MEANS)")
print("=" * 60)
print(f"Number of rows: {dfFinal.count()}")
procMeans(
    dfFinal,
    ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
    ["n", "nmiss", "mean", "std", "min", "max"]
)

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME",
    "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
