"""
PySpark Script: 02_data_cleaning.py
Purpose: Clean and transform the HOME_EQUITY dataset
Equivalent SAS Program: sas/02_data_cleaning.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_DataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load the dataset produced by 01_data_loading.py
# SAS equivalent: set work.home_equity;
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
#   run;
#
# The SAS missing value (.) maps to a Spark null, which is what
# when(...) returns when no otherwise() branch is supplied.
# ------------------------------------------------------------------
dfClean = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ) \
    .withColumn(
        "CITY",
        initcap(col("CITY"))  # SAS equivalent: propcase(CITY)
    )

# ------------------------------------------------------------------
# Step 2: Flag missing values
# SAS equivalent:
#   array num_vars{8}  LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
#                      DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
#   do i = 1 to 8;
#       if num_vars{i} = . then num_flags{i} = 1;
#       else num_flags{i} = 0;
#   end;
#
# A SAS array loop becomes a plain Python loop over column names,
# building one withColumn per flag.
# ------------------------------------------------------------------
numericCols = [
    "LOAN", "MORTDUE", "VALUE", "YOJ",
    "DEROG", "DELINQ", "CLAGE", "NINQ"
]

for numericCol in numericCols:
    dfClean = dfClean.withColumn(
        f"{numericCol}_MISS",
        when(col(numericCol).isNull(), lit(1)).otherwise(lit(0))
    )

# ------------------------------------------------------------------
# Step 3: Filter out records with missing critical fields
# SAS equivalent:
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#
# A subsetting IF statement becomes DataFrame.filter().
# ------------------------------------------------------------------
dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

# ------------------------------------------------------------------
# Step 4: Check for outliers
# SAS equivalent:
#   proc means data=work.home_equity_filtered
#       n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
#       var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
#   run;
#
# describe() covers count/mean/stddev/min/max; approxQuantile()
# supplies the percentile columns PROC MEANS reports.
# ------------------------------------------------------------------
outlierCols = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "LTV", "CLAGE", "DEROG", "DELINQ"
]

print("=" * 60)
print("Summary Statistics for Outlier Detection")
print("(equivalent to PROC MEANS)")
print("=" * 60)
dfFiltered.select(*outlierCols).describe().show()

print("Approximate Percentiles (p1, p5, p25, median, p75, p95, p99):")
for outlierCol in outlierCols:
    p1, p5, p25, median, p75, p95, p99 = dfFiltered.approxQuantile(
        outlierCol, [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99], 0.01
    )
    print(
        f"  {outlierCol:8s} p1={p1:.4f} p5={p5:.4f} p25={p25:.4f} "
        f"median={median:.4f} p75={p75:.4f} p95={p95:.4f} p99={p99:.4f}"
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
# Consecutive subsetting IF statements are chained into one filter().
# ------------------------------------------------------------------
dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("\n" + "=" * 60)
print("Clean Dataset Summary")
print("=" * 60)
print(f"Rows loaded:        {df.count()}")
print(f"After null filter:  {dfFiltered.count()}")
print(f"Final clean rows:   {dfFinal.count()}")

dfFinal.select("LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC").describe().show()

# ------------------------------------------------------------------
# Preview the first 10 observations
# SAS equivalent:
#   proc print data=work.home_equity_final(obs=10);
#       var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
#   run;
# ------------------------------------------------------------------
dfFinal.select(
    "BAD", "LOAN", "MORTDUE", "VALUE", "LTV",
    "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
).show(10, truncate=False)

# End the Spark session
# SAS equivalent: end of program
spark.stop()
