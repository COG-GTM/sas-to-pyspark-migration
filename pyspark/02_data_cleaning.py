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

# ------------------------------------------------------------------
# Load source data (work.home_equity produced by 01_data_loading.sas)
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

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
# Note: PySpark does not have native column labels or formats like SAS.
# We document them as metadata in a dictionary. The SAS percent8.2
# format is display-only; LTV is stored as a raw ratio (e.g. 0.75).
# ------------------------------------------------------------------
# SAS propcase() starts a new word after blank, "/", "-", "(" and ".";
# Spark initcap() only splits on whitespace, so re-capitalize the first
# letter after each of the other delimiters (e.g. "Winston-Salem").
PROPCASE_DELIMITERS = ["/", "-", "(", "."]


def propcase(col):
    result = F.initcap(col)
    for delim in PROPCASE_DELIMITERS:
        result = F.array_join(
            F.transform(
                F.split(result, "\\" + delim),
                lambda s: F.concat(
                    F.upper(s.substr(F.lit(1), F.lit(1))),
                    s.substr(F.lit(2), F.length(s))
                )
            ),
            delim
        )
    return result


home_equity_clean = df \
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
        F.when(F.col("BAD") == 0, "Paid")
         .when(F.col("BAD") == 1, "Default")
         .otherwise("")
    ) \
    .withColumn("CITY", propcase(F.col("CITY")))

columnLabels = {
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
}
columnFormats = {
    "LTV": "percent8.2",
}

print("=" * 60)
print("Derived Column Labels/Formats (SAS-style metadata)")
print("=" * 60)
for col, label in columnLabels.items():
    fmt = columnFormats.get(col, "")
    print(f"  {col:12s} -> {label}" + (f"  [format {fmt}]" if fmt else ""))

# ------------------------------------------------------------------
# Step 2: Flag missing values (array processing)
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
# Note: SAS arrays are emulated with a Python list of
# (source column, flag column) pairs.
# ------------------------------------------------------------------
num_vars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
num_flags = [f"{v}_MISS" for v in num_vars]

home_equity_imputed = home_equity_clean
for var, flag in zip(num_vars, num_flags):
    home_equity_imputed = home_equity_imputed.withColumn(
        flag,
        F.when(F.col(var).isNull(), 1).otherwise(0)
    )

print("\n" + "=" * 60)
print("Missing-Value Flag Counts (sum of *_MISS flags)")
print("=" * 60)
home_equity_imputed.agg(
    *[F.sum(flag).alias(flag) for flag in num_flags]
).show(truncate=False)

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
# ------------------------------------------------------------------
home_equity_filtered = home_equity_imputed.filter(
    F.col("LOAN").isNotNull()
    & F.col("VALUE").isNotNull()
    & F.col("BAD").isNotNull()
)

print(f"Rows before filter: {home_equity_imputed.count()}")
print(f"Rows after filter:  {home_equity_filtered.count()}")

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
# Note: percentile_approx gives approximate percentiles; SAS PROC MEANS
# percentiles are exact, so small differences are expected.
# ------------------------------------------------------------------
outlier_vars = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "DEROG", "DELINQ"]
percentiles = [("p1", 0.01), ("p5", 0.05), ("p25", 0.25), ("median", 0.50),
               ("p75", 0.75), ("p95", 0.95), ("p99", 0.99)]


def proc_means(data, variables, with_percentiles):
    """Build a PROC MEANS-style table: one row per variable."""
    exprs = []
    for v in variables:
        exprs += [
            F.count(v).alias(f"{v}__n"),
            F.sum(F.when(F.col(v).isNull(), 1).otherwise(0)).alias(f"{v}__nmiss"),
            F.mean(v).alias(f"{v}__mean"),
            F.stddev(v).alias(f"{v}__std"),
            F.min(v).cast("double").alias(f"{v}__min"),
        ]
        if with_percentiles:
            exprs += [
                F.percentile_approx(v, p).cast("double").alias(f"{v}__{name}")
                for name, p in percentiles
            ]
        exprs.append(F.max(v).cast("double").alias(f"{v}__max"))

    row = data.agg(*exprs).collect()[0].asDict()
    stats = ["n", "nmiss", "mean", "std", "min"]
    if with_percentiles:
        stats += [name for name, _ in percentiles]
    stats.append("max")

    rows = [tuple([v] + [row[f"{v}__{s}"] for s in stats]) for v in variables]
    schema = ["Variable"] + [
        "N" if s == "n" else "NMiss" if s == "nmiss" else s.capitalize()
        for s in stats
    ]
    return spark.createDataFrame(rows, schema)


print("\n" + "=" * 60)
print("Summary Statistics for Outlier Detection (equivalent to PROC MEANS)")
print("=" * 60)
proc_means(home_equity_filtered, outlier_vars, with_percentiles=True) \
    .show(truncate=False)

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
# Note: In SAS a missing LTV compares as less than any number, so
# "LTV > 0" drops it; Spark's null comparison likewise drops the row.
# ------------------------------------------------------------------
home_equity_final = home_equity_filtered.filter(
    (F.col("LTV") > 0) & (F.col("LTV") < 5)
    & (F.col("LOAN") > 0)
    & (F.col("VALUE") > 0)
)

print(f"Rows in home_equity_final: {home_equity_final.count()}")

# SAS equivalent:
#   title "Clean Dataset Summary";
#   proc means data=work.home_equity_final n nmiss mean std min max;
#       var LOAN MORTDUE VALUE LTV DEBTINC;
#   run;
#   title;
print("\n" + "=" * 60)
print("Clean Dataset Summary (equivalent to PROC MEANS)")
print("=" * 60)
proc_means(home_equity_final, ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"],
           with_percentiles=False) \
    .show(truncate=False)

# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
print("\n" + "=" * 60)
print("First 10 Observations (equivalent to PROC PRINT obs=10)")
print("=" * 60)
home_equity_final.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
