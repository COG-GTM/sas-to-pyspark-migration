"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Logistic regression (Spark MLlib) on a 70/30 train/validation split
         - Model evaluation (concordance, AUC)
         - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, min, max, expr, round
)
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexerModel, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild the upstream input dataset (work.home_equity_risk)
# SAS equivalent (sas/02_data_cleaning.sas, sas/04_risk_segmentation.sas):
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then
#           LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_clean;
#       where LTV > 0 and LTV < 5 and LOAN > 0 and VALUE > 0;
#   run;
#   data work.home_equity_risk; set work.home_equity_final; ... run;
#
# Note: SAS chains off WORK datasets built by earlier programs. This
# script is self-contained, so the upstream cleaning that affects the
# modeling rows is reproduced inline. The 04 risk-segment columns do not
# filter rows and are not model inputs, so they are not recreated here.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

homeEquityRisk = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None).cast("double"))
    ) \
    .filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    ) \
    .filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) & (col("VALUE") > 0)
    )

print("=" * 60)
print("Input Dataset (equivalent to work.home_equity_risk)")
print("=" * 60)
print(f"Number of rows: {homeEquityRisk.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - keep complete cases
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# Note: PROC LOGISTIC silently drops observations with a missing value
# in ANY model variable (including CLASS variables). Spark ML fails on
# nulls instead, so DEROG, NINQ, JOB and REASON are filtered explicitly
# to reproduce the observations SAS actually uses to fit the model.
# ------------------------------------------------------------------
numericPredictors = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "DELINQ", "DEROG", "CLAGE", "NINQ"
]
classPredictors = ["JOB", "REASON"]

modelData = homeEquityRisk.filter(
    col("LOAN").isNotNull() &
    col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() &
    col("CLAGE").isNotNull()
)

print("\n" + "=" * 60)
print("Modeling Dataset (equivalent to work.model_data)")
print("=" * 60)
print(f"Number of rows: {modelData.count()}")

modelData = modelData \
    .filter(
        col("DEROG").isNotNull() &
        col("NINQ").isNotNull() &
        col("JOB").isNotNull() &
        col("REASON").isNotNull()
    ) \
    .withColumn("label", col("BAD").cast("double"))

print(f"Number of rows used by the model (no missing model variables): "
      f"{modelData.count()}")

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
#
# Note: randomSplit is a Bernoulli split, so the training share is
# approximately (not exactly) 70%, and the seed does not reproduce the
# same rows SAS would select.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
train = train.cache()
valid = valid.cache()

print("\n" + "=" * 60)
print("Training / Validation Split (equivalent to PROC SURVEYSELECT)")
print("=" * 60)
print(f"Training rows: {train.count()}")
print(f"Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression
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
# CLASS ... / param=ref  -> StringIndexerModel with fixed label order
#                           (reference level last) + OneHotEncoder
#                           (dropLast=True drops the reference level)
# MODEL                  -> VectorAssembler + LogisticRegression
# descending             -> label = BAD, probability[1] = P(BAD=1)
# STORE                  -> fitted PipelineModel (logitModel)
#
# Note: MLlib has no stepwise selection (slentry/slstay), p-values or
# LACKFIT (Hosmer-Lemeshow) test, so the full model is fitted. regParam
# is 0.0 to match PROC LOGISTIC's unpenalized maximum likelihood fit.
# ------------------------------------------------------------------
jobLevels = ["Mgr", "Office", "ProfExe", "Sales", "Self", "Other"]
reasonLevels = ["DebtCon", "HomeImp"]

jobIdx = StringIndexerModel.from_labels(
    jobLevels, inputCol="JOB", outputCol="JOB_IDX"
)
reasonIdx = StringIndexerModel.from_labels(
    reasonLevels, inputCol="REASON", outputCol="REASON_IDX"
)
classEnc = OneHotEncoder(
    inputCols=["JOB_IDX", "REASON_IDX"],
    outputCols=["JOB_VEC", "REASON_VEC"],
    dropLast=True
)
assembler = VectorAssembler(
    inputCols=numericPredictors + ["JOB_VEC", "REASON_VEC"],
    outputCol="features"
)
lr = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=100, regParam=0.0
)

pipeline = Pipeline(stages=[jobIdx, reasonIdx, classEnc, assembler, lr])
logitModel = pipeline.fit(train)
lrModel = logitModel.stages[-1]

featureNames = (
    numericPredictors +
    [f"JOB {level}" for level in jobLevels[:-1]] +
    [f"REASON {level}" for level in reasonLevels[:-1]]
)

print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)
print("Response Profile (training set)")
train.groupBy("BAD").agg(count("*").alias("Total_Frequency")) \
    .orderBy(col("BAD").desc()) \
    .show()

print("Probability modeled is BAD=1")
print("Reference levels: JOB='Other', REASON='HomeImp'\n")

coefRows = [Row(Parameter="Intercept", Estimate=float(lrModel.intercept))] + [
    Row(Parameter=name, Estimate=float(est))
    for name, est in zip(featureNames, lrModel.coefficients.toArray())
]
print("Parameter Estimates")
spark.createDataFrame(coefRows) \
    .withColumn("Odds_Ratio", round(F.exp(col("Estimate")), 4)) \
    .show(truncate=False)

print(f"Training AUC: {lrModel.summary.areaUnderROC:.4f}")

# Equivalent of OUTPUT OUT=work.train_scored PREDICTED=pred_prob
trainScored = logitModel.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

print("\nTraining Set Scored (equivalent to work.train_scored, first 10)")
trainScored.select(
    "BAD", *numericPredictors, *classPredictors, "pred_prob"
).show(10)

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored
#             predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------
validScored = logitModel.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

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
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, 1).otherwise(0)
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("=" * 60)
confusionMatrix = validScored \
    .groupBy("BAD") \
    .pivot("PREDICTED_BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "PREDICTED_BAD=0") \
    .withColumnRenamed("1", "PREDICTED_BAD=1") \
    .withColumn("Total", col("`PREDICTED_BAD=0`") + col("`PREDICTED_BAD=1`")) \
    .orderBy("BAD")
confusionMatrix.show()

accuracy = validScored.filter(col("BAD") == col("PREDICTED_BAD")).count() \
    / validScored.count()
print(f"Validation accuracy at 0.5 cutoff: {accuracy:.4f}")

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
# Note: as in the SAS program, the full model is refitted on the
# validation set. Concordance is computed from all (event, non-event)
# pairs on unbinned probabilities, so values can differ slightly from
# SAS, which bins predicted probabilities before counting ties.
# ------------------------------------------------------------------
validModel = pipeline.fit(valid)
validRefit = validModel.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
validAuc = evaluator.evaluate(validRefit)

events = validRefit.filter(col("BAD") == 1).select(col("pred_prob").alias("pEvent"))
nonEvents = validRefit.filter(col("BAD") == 0).select(col("pred_prob").alias("pNonEvent"))

pairCounts = events.crossJoin(nonEvents).agg(
    count("*").alias("pairs"),
    F.sum(when(col("pEvent") > col("pNonEvent"), 1).otherwise(0)).alias("concordant"),
    F.sum(when(col("pEvent") < col("pNonEvent"), 1).otherwise(0)).alias("discordant"),
    F.sum(when(col("pEvent") == col("pNonEvent"), 1).otherwise(0)).alias("tied"),
).collect()[0]

nValid = validRefit.count()
pairs = pairCounts["pairs"]
concordant = pairCounts["concordant"]
discordant = pairCounts["discordant"]
tied = pairCounts["tied"]

somersD = (concordant - discordant) / pairs
gammaStat = (concordant - discordant) / (concordant + discordant)
tauA = (concordant - discordant) / (0.5 * nValid * (nValid - 1))
cStat = (concordant + 0.5 * tied) / pairs

print("\n" + "=" * 60)
print("Model Performance - Concordance and AUC")
print("=" * 60)
print(f"Area under the ROC curve (AUC): {validAuc:.4f}")

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
associationStats = spark.createDataFrame([
    Row(Label1="Percent Concordant", cValue1=f"{100 * concordant / pairs:.1f}",
        Label2="Somers' D", cValue2=f"{somersD:.3f}"),
    Row(Label1="Percent Discordant", cValue1=f"{100 * discordant / pairs:.1f}",
        Label2="Gamma", cValue2=f"{gammaStat:.3f}"),
    Row(Label1="Percent Tied", cValue1=f"{100 * tied / pairs:.1f}",
        Label2="Tau-a", cValue2=f"{tauA:.3f}"),
    Row(Label1="Pairs", cValue1=f"{pairs}",
        Label2="c", cValue2=f"{cStat:.3f}"),
])

print("\n" + "=" * 60)
print("Model Association Statistics")
print("=" * 60)
associationStats.show(truncate=False)

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
#
# Note: Spark's exact percentile() interpolates between order statistics,
# which can differ slightly from the SAS default (PCTLDEF=5).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 60)
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    round(mean("pred_prob"), 4).alias("Mean"),
    round(stddev("pred_prob"), 4).alias("Std_Dev"),
    round(min("pred_prob"), 4).alias("Minimum"),
    round(expr("percentile(pred_prob, 0.25)"), 4).alias("P25"),
    round(expr("percentile(pred_prob, 0.5)"), 4).alias("Median"),
    round(expr("percentile(pred_prob, 0.75)"), 4).alias("P75"),
    round(max("pred_prob"), 4).alias("Maximum"),
).orderBy("BAD").show()

# Clean up
spark.stop()
