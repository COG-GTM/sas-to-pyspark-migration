"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Logistic regression with stepwise-style feature selection
         - Model evaluation (concordance, AUC)
         - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas

Self-contained: the upstream WORK datasets (home_equity_final from
sas/02_data_cleaning.sas and home_equity_risk from
sas/04_risk_segmentation.sas) are rebuilt inline by helper functions.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as spark_min,
    max as spark_max, percentile_approx, sum as spark_sum
)
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

NUMERIC_FEATURES = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                    "DELINQ", "DEROG", "CLAGE", "NINQ"]
CLASS_FEATURES = ["JOB", "REASON"]
SEED = 42
TRAIN_RATE = 0.7
CUTOFF = 0.5


def clean_home_equity(df):
    """Replicates sas/02_data_cleaning.sas -> work.home_equity_final."""
    # SAS: DATA step IF/THEN -> when().otherwise()
    df = df \
        .withColumn(
            "LTV",
            when(
                col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default"))
        ) \
        .withColumn("CITY", initcap(col("CITY")))  # SAS: propcase() -> initcap()

    # SAS: ARRAY + DO loop -> Python loop over columns with withColumn
    for varName in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
        df = df.withColumn(f"{varName}_MISS", when(col(varName).isNull(), 1).otherwise(0))

    # SAS: subsetting IF with "ne ." -> filter(isNotNull())
    filtered = df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )
    final = filtered.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )
    return filtered, final


def add_risk_segments(df):
    """Replicates sas/04_risk_segmentation.sas -> work.home_equity_risk.

    A null component contributes 0 points, matching SAS "if X ne . then do".
    """
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

    # SAS: PROC FORMAT value ranges -> chained when()
    return df \
        .withColumn(
            "LTV_RISK_CAT",
            when(col("LTV") < 0.60, "Low").when(col("LTV") < 0.80, "Medium")
            .when(col("LTV").isNotNull(), "High")
        ) \
        .withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC") < 30, "Low").when(col("DEBTINC") < 40, "Medium")
            .when(col("DEBTINC") < 50, "High").when(col("DEBTINC").isNotNull(), "Very High")
        ) \
        .withColumn(
            "DELINQ_RISK_CAT",
            when(col("DELINQ") == 0, "None").when(col("DELINQ") == 1, "Low")
            .when(col("DELINQ") <= 3, "Medium").when(col("DELINQ").isNotNull(), "High")
        ) \
        .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore) \
        .withColumn(
            "RISK_SEGMENT",
            when(col("RISK_SCORE") < 3, "Low Risk").when(col("RISK_SCORE") < 5, "Medium Risk")
            .when(col("RISK_SCORE") < 7, "High Risk").otherwise("Very High Risk")
        )


def build_logit_pipeline(elasticNetParam):
    """SAS: CLASS + MODEL statements -> StringIndexer/OneHotEncoder/VectorAssembler/LR."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_IDX", handleInvalid="keep")
        for c in CLASS_FEATURES
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_IDX", outputCol=f"{c}_VEC") for c in CLASS_FEATURES
    ]
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + [f"{c}_VEC" for c in CLASS_FEATURES],
        outputCol="features"
    )
    lr = LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=50, regParam=0.01, elasticNetParam=elasticNetParam
    )
    return Pipeline(stages=indexers + encoders + [assembler, lr])


def score(model, df):
    """SAS: OUTPUT PREDICTED= / PROC PLM SCORE ... / ilink -> transform + P(BAD=1)."""
    return model.transform(df) \
        .withColumn("pred_prob", vector_to_array(col("probability"))[1])


def association_stats(scored):
    """SAS: ODS OUTPUT Association -> pairwise concordance over (event, non-event) pairs."""
    events = scored.filter(col("label") == 1).select(col("pred_prob").alias("p1"))
    nonEvents = scored.filter(col("label") == 0).select(col("pred_prob").alias("p0"))
    row = events.crossJoin(nonEvents).agg(
        count(lit(1)).alias("pairs"),
        spark_sum(when(col("p1") > col("p0"), 1).otherwise(0)).alias("concordant"),
        spark_sum(when(col("p1") < col("p0"), 1).otherwise(0)).alias("discordant"),
    ).collect()[0]
    pairs = row["pairs"]
    pctC = row["concordant"] / pairs
    pctD = row["discordant"] / pairs
    pctT = 1.0 - pctC - pctD
    return [
        ("Percent Concordant", 100 * pctC),
        ("Percent Discordant", 100 * pctD),
        ("Percent Tied", 100 * pctT),
        ("Pairs", pairs),
        ("Somers' D", pctC - pctD),
        ("c", pctC + 0.5 * pctT),
    ]


# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild upstream WORK datasets
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv" out=work.home_equity ...;
#   (sas/02_data_cleaning.sas)   -> work.home_equity_final
#   (sas/04_risk_segmentation.sas) -> work.home_equity_risk
# ------------------------------------------------------------------
# SAS: PROC IMPORT -> spark.read.csv (empty fields become null, i.e. SAS ".")
raw = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
filtered, final = clean_home_equity(raw)
homeEquityRisk = add_risk_segments(final)

print("=" * 60)
print("Upstream datasets (sas/02 + sas/04 replicated inline)")
print("=" * 60)
print(f"  work.home_equity          : {raw.count()} rows")
print(f"  work.home_equity_filtered : {filtered.count()} rows")
print(f"  work.home_equity_final    : {final.count()} rows")
print(f"  work.home_equity_risk     : {homeEquityRisk.count()} rows")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - remove records with excessive missing
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# PROC LOGISTIC also drops observations with a missing value in any MODEL
# variable, so DEROG, NINQ, JOB and REASON are required non-null as well.
# ------------------------------------------------------------------
requiredCols = NUMERIC_FEATURES + CLASS_FEATURES
completeCase = lit(True)
for c in requiredCols:
    completeCase = completeCase & col(c).isNotNull()

# SAS: DESCENDING (model P(BAD=1)) -> label = BAD as double, event = 1.0
modelData = homeEquityRisk.filter(completeCase) \
    .withColumn("label", col("BAD").cast("double")) \
    .cache()

print(f"\nModeling dataset (work.model_data): {modelData.count()} rows")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
#   data work.train work.valid;
#       set work.model_split;
#       if selected = 1 then output work.train;
#       else output work.valid;
#   run;
# ------------------------------------------------------------------
# SAS: PROC SURVEYSELECT SRS -> randomSplit (same rate and seed; the RNGs differ)
train, valid = modelData.randomSplit([TRAIN_RATE, 1 - TRAIN_RATE], seed=SEED)
train = train.cache()
valid = valid.cache()
print(f"Training set   (work.train): {train.count()} rows")
print(f"Validation set (work.valid): {valid.count()} rows")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression with stepwise selection
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON
#                 / selection=stepwise slentry=0.05 slstay=0.05
#                   details lackfit;
#       output out=work.train_scored predicted=pred_prob;
#       store work.logit_model;
#   run;
#
# Spark ML has no p-value-driven stepwise selection. L1 regularisation
# (elasticNetParam=1.0) is used instead: it shrinks weak predictors to
# exactly 0, which plays the role of SLENTRY/SLSTAY.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)

# SAS: STORE work.logit_model -> fitted PipelineModel (model.save(path) to persist)
logitModel = build_logit_pipeline(elasticNetParam=1.0).fit(train)
lrModel = logitModel.stages[-1]

# Map vector slots back to readable parameter names (SAS "Analysis of Maximum Likelihood Estimates")
featureNames = list(NUMERIC_FEATURES)
for c, indexerModel in zip(CLASS_FEATURES, logitModel.stages[:len(CLASS_FEATURES)]):
    # OneHotEncoder(dropLast=True) drops the extra handleInvalid="keep" slot
    featureNames += [f"{c}={label}" for label in indexerModel.labels]

print(f"  {'Parameter':20s} {'Estimate':>14s}  Selected")
print(f"  {'Intercept':20s} {lrModel.intercept:14.6f}  yes")
for name, coef in zip(featureNames, lrModel.coefficients.toArray()):
    print(f"  {name:20s} {coef:14.6g}  {'yes' if coef != 0 else 'no (dropped)'}")

trainScored = score(logitModel, train)

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------
validScored = score(logitModel, valid)

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD", when(col("pred_prob") >= CUTOFF, 1).otherwise(0)
).cache()

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("=" * 60)
# SAS: PROC FREQ two-way table -> groupBy().pivot().count()
validScored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).count() \
    .na.fill(0).orderBy("BAD").show()

cm = {(r["BAD"], r["PREDICTED_BAD"]): r["count"]
      for r in validScored.groupBy("BAD", "PREDICTED_BAD").count().collect()}
tn, fp = cm.get((0, 0), 0), cm.get((0, 1), 0)
fn, tp = cm.get((1, 0), 0), cm.get((1, 1), 0)
total = tn + fp + fn + tp
print(f"  Accuracy   : {(tp + tn) / total:.4f}")
print(f"  Sensitivity: {tp / (tp + fn) if tp + fn else 0.0:.4f}")
print(f"  Specificity: {tn / (tn + fp) if tn + fp else 0.0:.4f}")

# ------------------------------------------------------------------
# Step 6: Calculate model performance metrics
# SAS equivalent:
#   proc logistic data=work.valid descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON;
#       roc;
#       ods output Association=work.association_stats;
#   run;
#
# As in SAS, the full model (no selection) is refit on work.valid.
# The out-of-sample AUC of the stepwise-style model is reported too.
# ------------------------------------------------------------------
# SAS: ROC statement / c statistic -> BinaryClassificationEvaluator(areaUnderROC)
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
fullValidModel = build_logit_pipeline(elasticNetParam=0.0).fit(valid)
fullValidScored = score(fullValidModel, valid)
associationStats = association_stats(fullValidScored)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Association Statistics (full model refit on work.valid)")
print("=" * 60)
for label, value in associationStats:
    print(f"  {label:20s} {value:>12,.4f}" if label != "Pairs" else f"  {label:20s} {value:>12,d}")
print(f"  {'AUC (evaluator)':20s} {evaluator.evaluate(fullValidScored):>12.4f}")

print("\nOut-of-sample performance of the training model:")
print(f"  Train AUC     : {evaluator.evaluate(trainScored):.4f}")
print(f"  Validation AUC: {evaluator.evaluate(validScored):.4f}")

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 60)
# SAS: PROC MEANS CLASS -> groupBy().agg(); percentiles via percentile_approx
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("Std"),
    spark_min("pred_prob").alias("Min"),
    percentile_approx("pred_prob", 0.25).alias("P25"),
    percentile_approx("pred_prob", 0.5).alias("Median"),
    percentile_approx("pred_prob", 0.75).alias("P75"),
    spark_max("pred_prob").alias("Max"),
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
