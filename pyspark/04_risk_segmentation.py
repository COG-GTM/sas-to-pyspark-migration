"""
PySpark Script: 04_risk_segmentation.py
Purpose: Create risk segments for loan portfolio analysis
  - Define risk buckets (PROC FORMAT equivalent)
  - Create composite risk scores
  - Analyze default rates by risk segment
Equivalent SAS Program: sas/04_risk_segmentation.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, round as spark_round, sum as spark_sum
)

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_RiskSegmentation") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Build the input dataset work.home_equity_final
# SAS equivalent (prerequisite from sas/02_data_cleaning.sas):
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#   run;
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
#
# Note: sas/04_risk_segmentation.sas reads work.home_equity_final from the
# 02 program. It is rebuilt here from the CSV so this script is standalone.
# Only the 02 logic that affects 04 is reproduced (LTV and the row filters);
# columns 04 never uses (LOAN_OUTCOME, propcase CITY, *_MISS flags) are omitted.
# In SAS a missing LTV is below 0, so "LTV > 0" drops it; Spark's null
# comparison drops it too, so no explicit isNull() handling is needed.
# ------------------------------------------------------------------
home_equity = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

home_equity_clean = home_equity.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
)

home_equity_filtered = home_equity_clean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
)

home_equity_final = home_equity_filtered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Input dataset work.home_equity_final")
print("=" * 60)
print(f"Rows in work.home_equity: {home_equity.count()}")
print(f"Rows in work.home_equity_filtered: {home_equity_filtered.count()}")
print(f"Rows in work.home_equity_final: {home_equity_final.count()}")

# ------------------------------------------------------------------
# Step 1: Define risk bucket formats
# SAS equivalent:
#   proc format;
#       value ltv_risk
#           low  -< 0.60 = 'Low'
#           0.60 -< 0.80 = 'Medium'
#           0.80 - high   = 'High';
#       value dti_risk
#           low  -< 30  = 'Low'
#           30   -< 40  = 'Medium'
#           40   -< 50  = 'High'
#           50   - high  = 'Very High';
#       value delinq_risk
#           0         = 'None'
#           1         = 'Low'
#           2 - 3     = 'Medium'
#           4 - high  = 'High';
#       value risk_score
#           low  -< 3 = 'Low Risk'
#           3    -< 5 = 'Medium Risk'
#           5    -< 7 = 'High Risk'
#           7 - high  = 'Very High Risk';
#   run;
#
# Note: PySpark has no PROC FORMAT. Each format is kept as a list of
# (SAS range, label) pairs for documentation. The SAS program defines these
# formats but never applies them with a FORMAT statement; Step 2 derives the
# categories with IF/THEN logic, so the formats don't change any output.
# ------------------------------------------------------------------
columnFormats = {
    "ltv_risk": [
        ("low -< 0.60", "Low"),
        ("0.60 -< 0.80", "Medium"),
        ("0.80 - high", "High"),
    ],
    "dti_risk": [
        ("low -< 30", "Low"),
        ("30 -< 40", "Medium"),
        ("40 -< 50", "High"),
        ("50 - high", "Very High"),
    ],
    "delinq_risk": [
        ("0", "None"),
        ("1", "Low"),
        ("2 - 3", "Medium"),
        ("4 - high", "High"),
    ],
    "risk_score": [
        ("low -< 3", "Low Risk"),
        ("3 -< 5", "Medium Risk"),
        ("5 -< 7", "High Risk"),
        ("7 - high", "Very High Risk"),
    ],
}

print("\n" + "=" * 60)
print("Risk Formats (equivalent to PROC FORMAT)")
print("=" * 60)
for fmtName, ranges in columnFormats.items():
    print(f"  {fmtName}")
    for sasRange, label in ranges:
        print(f"    {sasRange:14s} = '{label}'")

# ------------------------------------------------------------------
# Step 2: Create risk segment variables
# SAS equivalent:
#   data work.home_equity_risk;
#       set work.home_equity_final;
#       length LTV_RISK_CAT $6 DTI_RISK_CAT $9 DELINQ_RISK_CAT $6;
#       if LTV ne . then do;
#           if LTV < 0.60 then LTV_RISK_CAT = 'Low';
#           else if LTV < 0.80 then LTV_RISK_CAT = 'Medium';
#           else LTV_RISK_CAT = 'High';
#       end;
#       ... (DTI_RISK_CAT, DELINQ_RISK_CAT similarly)
#       RISK_SCORE = 0;
#       if LTV ne . then do;
#           if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
#           else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
#       end;
#       ... (DTI 0-3, DELINQ 0-2, DEROG 0-2 points)
#       length RISK_SEGMENT $14;
#       if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
#       else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
#       else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
#       else RISK_SEGMENT = 'Very High Risk';
#       label LTV_RISK_CAT = "LTV Risk Category" ...;
#   run;
#
# Note: SAS leaves a character variable blank when its IF block is skipped
# (missing input); here it is null. Every missing score component adds 0
# points, so RISK_SCORE and RISK_SEGMENT are never missing. The LENGTH
# statements need no Spark equivalent: every label fits its declared length.
# ------------------------------------------------------------------
ltvScore = when(col("LTV").isNull(), lit(0)) \
    .when(col("LTV") >= 0.80, lit(3)) \
    .when(col("LTV") >= 0.60, lit(1.5)) \
    .otherwise(lit(0))
dtiScore = when(col("DEBTINC").isNull(), lit(0)) \
    .when(col("DEBTINC") >= 50, lit(3)) \
    .when(col("DEBTINC") >= 40, lit(2)) \
    .when(col("DEBTINC") >= 30, lit(1)) \
    .otherwise(lit(0))
delinqScore = when(col("DELINQ").isNull(), lit(0)) \
    .when(col("DELINQ") >= 4, lit(2)) \
    .when(col("DELINQ") >= 2, lit(1.5)) \
    .when(col("DELINQ") == 1, lit(0.5)) \
    .otherwise(lit(0))
derogScore = when(col("DEROG").isNull(), lit(0)) \
    .when(col("DEROG") >= 3, lit(2)) \
    .when(col("DEROG") >= 1, lit(1)) \
    .otherwise(lit(0))

home_equity_risk = home_equity_final.withColumn(
    "LTV_RISK_CAT",
    when(col("LTV").isNull(), lit(None))
    .when(col("LTV") < 0.60, lit("Low"))
    .when(col("LTV") < 0.80, lit("Medium"))
    .otherwise(lit("High"))
).withColumn(
    "DTI_RISK_CAT",
    when(col("DEBTINC").isNull(), lit(None))
    .when(col("DEBTINC") < 30, lit("Low"))
    .when(col("DEBTINC") < 40, lit("Medium"))
    .when(col("DEBTINC") < 50, lit("High"))
    .otherwise(lit("Very High"))
).withColumn(
    "DELINQ_RISK_CAT",
    when(col("DELINQ").isNull(), lit(None))
    .when(col("DELINQ") == 0, lit("None"))
    .when(col("DELINQ") == 1, lit("Low"))
    .when(col("DELINQ") <= 3, lit("Medium"))
    .otherwise(lit("High"))
).withColumn(
    "RISK_SCORE",
    (ltvScore + dtiScore + delinqScore + derogScore).cast("double")
).withColumn(
    "RISK_SEGMENT",
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
).cache()

columnLabels = {
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

print("\n" + "=" * 60)
print("Risk Segment Variables (work.home_equity_risk)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:16s} -> {label}")
print(f"Rows in work.home_equity_risk: {home_equity_risk.count()}")
home_equity_risk.select(
    "BAD", "LTV", "DEBTINC", "DELINQ", "DEROG",
    "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT",
    "RISK_SCORE", "RISK_SEGMENT"
).show(10, truncate=False)


def procFreqOneWay(df, colName):
    """One-way PROC FREQ table with the NOCUM option (Frequency and Percent).

    Like PROC FREQ, missing values are excluded from the table and from the
    percent denominator, and reported as "Frequency Missing". Levels are in
    ascending order of the value (PROC FREQ's default ORDER=INTERNAL).
    """
    nonMissing = df.filter(col(colName).isNotNull())
    total = nonMissing.count()
    freqTable = nonMissing.groupBy(colName).agg(count(lit(1)).alias("Frequency")) \
        .withColumn("Percent", spark_round(col("Frequency") * 100.0 / total, 2)) \
        .orderBy(colName)
    print(f"\n{colName}  ({columnLabels.get(colName, colName)})")
    freqTable.show(truncate=False)
    nMissing = df.count() - total
    if nMissing > 0:
        print(f"Frequency Missing = {nMissing}")
    return freqTable


# ------------------------------------------------------------------
# Step 3: Distribution across risk segments
# SAS equivalent:
#   title "Distribution of Loans by Risk Segment";
#   proc freq data=work.home_equity_risk;
#       tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
#   run;
#   title;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Distribution of Loans by Risk Segment (equivalent to PROC FREQ)")
print("=" * 60)
for colName in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
    procFreqOneWay(home_equity_risk, colName)


def procMeansByClass(df, classVar, analysisVars):
    """PROC MEANS N MEAN STD with a CLASS statement.

    Returns one row per (class level, analysis variable), like the SAS
    output layout. N counts non-missing values; STD is the sample standard
    deviation (stddev in Spark), as in SAS. N_OBS is the class level row count.
    """
    aggExprs = [count(lit(1)).alias("N_OBS")]
    for varName in analysisVars:
        aggExprs += [
            count(varName).alias(f"{varName}__N"),
            mean(varName).alias(f"{varName}__MEAN"),
            stddev(varName).alias(f"{varName}__STD"),
        ]
    wide = df.filter(col(classVar).isNotNull()).groupBy(classVar).agg(*aggExprs)

    longParts = [
        wide.select(
            col(classVar),
            col("N_OBS"),
            lit(varName).alias("VARIABLE"),
            col(f"{varName}__N").alias("N"),
            spark_round(col(f"{varName}__MEAN"), 4).alias("MEAN"),
            spark_round(col(f"{varName}__STD"), 4).alias("STD_DEV"),
        )
        for varName in analysisVars
    ]
    result = longParts[0]
    for part in longParts[1:]:
        result = result.unionByName(part)
    varOrder = when(col("VARIABLE") == analysisVars[0], lit(0))
    for i, varName in enumerate(analysisVars[1:], start=1):
        varOrder = varOrder.when(col("VARIABLE") == varName, lit(i))
    return result.orderBy(classVar, varOrder)


# ------------------------------------------------------------------
# Step 4: Default rate by risk segment
# SAS equivalent:
#   title "Average Default Rate by Risk Segment";
#   proc means data=work.home_equity_risk n mean std;
#       class RISK_SEGMENT;
#       var BAD LOAN LTV DEBTINC;
#   run;
#   title;
#
# Note: MEAN and STD_DEV are rounded to 4 decimals for display only.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Average Default Rate by Risk Segment (equivalent to PROC MEANS)")
print("=" * 60)
riskSegmentMeans = procMeansByClass(
    home_equity_risk, "RISK_SEGMENT", ["BAD", "LOAN", "LTV", "DEBTINC"]
)
riskSegmentMeans.show(20, truncate=False)

# ------------------------------------------------------------------
# Step 5: Cross-tabulation of risk segments
# SAS equivalent:
#   title "Default Rates by LTV Risk and DTI Risk";
#   proc freq data=work.home_equity_risk;
#       tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
#   run;
#   title;
#
# Note: a three-way TABLES request prints one DTI_RISK_CAT x BAD table per
# LTV_RISK_CAT level. NOROW NOCOL NOPERCENT leave only cell frequencies plus
# row/column totals. Rows with a missing value in any of the three variables
# are excluded, as in PROC FREQ; the excluded count is printed per table.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Default Rates by LTV Risk and DTI Risk (equivalent to PROC FREQ)")
print("=" * 60)
crossTabInput = home_equity_risk.filter(
    col("LTV_RISK_CAT").isNotNull() &
    col("DTI_RISK_CAT").isNotNull() &
    col("BAD").isNotNull()
)
badLevels = [row["BAD"] for row in crossTabInput.select("BAD").distinct().orderBy("BAD").collect()]
ltvLevels = [row["LTV_RISK_CAT"] for row in crossTabInput.select("LTV_RISK_CAT").distinct().orderBy("LTV_RISK_CAT").collect()]

for tableNum, ltvLevel in enumerate(ltvLevels, start=1):
    stratum = crossTabInput.filter(col("LTV_RISK_CAT") == ltvLevel)
    cells = stratum.groupBy("DTI_RISK_CAT").pivot("BAD", badLevels).count().na.fill(0)
    badCols = [str(level) for level in badLevels]
    cells = cells.withColumn("Total", sum((col(c) for c in badCols), lit(0)))
    totalRow = cells.agg(
        *[spark_sum(c).alias(c) for c in badCols + ["Total"]]
    ).select(lit("Total").alias("DTI_RISK_CAT"), *badCols, "Total")
    crossTab = cells.orderBy("DTI_RISK_CAT").unionByName(totalRow) \
        .withColumnsRenamed({c: f"BAD={c}" for c in badCols})
    print(f"\nTable {tableNum} of DTI_RISK_CAT by BAD")
    print(f"Controlling for LTV_RISK_CAT={ltvLevel}")
    crossTab.show(truncate=False)
    nMissing = home_equity_risk.filter(col("LTV_RISK_CAT") == ltvLevel).count() - stratum.count()
    if nMissing > 0:
        print(f"Frequency Missing = {nMissing}")

# Clean up
spark.stop()
