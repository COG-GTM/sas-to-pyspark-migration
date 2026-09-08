"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - Spark MLlib LogisticRegression (equivalent to PROC LOGISTIC)
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, min as spark_min, max as spark_max,
    expr, round as spark_round
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load the input data
# SAS equivalent:
#   set work.home_equity_risk;   /* dataset created by 04_risk_segmentation.sas */
#
# Note: work.home_equity_risk carries every column of the raw dataset plus
# derived risk columns; only raw columns are used by this program, so the
# CSV is loaded directly.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - remove records with excessive missing
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
modelData = df.filter(
    col("LOAN").isNotNull()
    & col("MORTDUE").isNotNull()
    & col("VALUE").isNotNull()
    & col("DEBTINC").isNotNull()
    & col("DELINQ").isNotNull()
    & col("CLAGE").isNotNull()
)

print("=" * 60)
print("Step 1: Modeling dataset (complete cases for key predictors)")
print("=" * 60)
print(f"Input rows:        {df.count()}")
print(f"Modeling rows:     {modelData.count()}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
#
#   data work.train work.valid;
#       set work.model_split;
#       if selected = 1 then output work.train;
#       else output work.valid;
#   run;
#
# Note: PROC SURVEYSELECT method=srs draws exactly 70% of rows; Spark's
# randomSplit assigns each row independently, so the split is ~70/30.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
train = train.cache()
valid = valid.cache()

print("\n" + "=" * 60)
print("Step 2: Train / validation split (equivalent to PROC SURVEYSELECT)")
print("=" * 60)
print(f"Training rows:     {train.count()}")
print(f"Validation rows:   {valid.count()}")

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
# Notes:
#   - DESCENDING: model the probability of BAD=1. MLlib LogisticRegression
#     always models P(label=1), so BAD is cast to a double "label".
#   - CLASS ... / param=ref: reference-cell coding. Each non-reference level
#     becomes a 0/1 dummy column; the reference level (JOB='Other',
#     REASON='HomeImp') is the all-zeros baseline.
#   - PROC LOGISTIC silently excludes observations with a missing value in
#     any model variable. VectorAssembler(handleInvalid="skip") does the same
#     for DEROG / NINQ, and the dummy columns are only defined for non-null
#     JOB / REASON.
#   - SELECTION=STEPWISE has no MLlib equivalent; the full model is fitted
#     with light L2 regularization and the DETAILS output is emulated by
#     printing the coefficient table. LACKFIT (Hosmer-Lemeshow) has no
#     MLlib equivalent.
#   - STORE work.logit_model: the fitted PipelineModel object plays the role
#     of the item store and is reused to score the validation set in Step 4.
# ------------------------------------------------------------------
numericPredictors = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                     "DELINQ", "DEROG", "CLAGE", "NINQ"]
classPredictors = {"JOB": "Other", "REASON": "HomeImp"}


def addReferenceDummies(dataFrame, levelsByVar):
    """Reference-cell (param=ref) coding for CLASS variables.

    levelsByVar maps each CLASS variable to its ordered list of non-reference
    levels. Rows where the CLASS variable is null keep null dummies so that
    handleInvalid="skip" excludes them, mirroring PROC LOGISTIC.
    """
    result = dataFrame
    for varName, levels in levelsByVar.items():
        for level in levels:
            result = result.withColumn(
                f"{varName}_{level}",
                when(col(varName).isNull(), lit(None).cast("double"))
                .when(col(varName) == level, lit(1.0))
                .otherwise(lit(0.0))
            )
    return result


# Non-reference levels are determined from the training data (as PROC
# LOGISTIC does) and reused when scoring the validation set.
nonRefLevels = {}
for varName, refLevel in classPredictors.items():
    levels = sorted(
        row[varName]
        for row in train.select(varName).distinct().collect()
        if row[varName] is not None and row[varName] != refLevel
    )
    nonRefLevels[varName] = levels

dummyColumns = [
    f"{varName}_{level}"
    for varName, levels in nonRefLevels.items()
    for level in levels
]
featureColumns = numericPredictors + dummyColumns

trainPrepared = addReferenceDummies(train, nonRefLevels) \
    .withColumn("label", col("BAD").cast("double"))

assembler = VectorAssembler(
    inputCols=featureColumns,
    outputCol="features",
    handleInvalid="skip"
)
logit = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    regParam=0.01
)
pipeline = Pipeline(stages=[assembler, logit])

logitModel = pipeline.fit(trainPrepared)
lrModel = logitModel.stages[-1]

print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)
print(f"Response:          BAD (event = 1, DESCENDING)")
print(f"Observations used: {lrModel.summary.predictions.count()}")
print(f"CLASS coding:      param=ref  "
      f"(JOB ref='{classPredictors['JOB']}', "
      f"REASON ref='{classPredictors['REASON']}')")

print("\nAnalysis of Maximum Likelihood Estimates (equivalent to DETAILS)")
print(f"  {'Parameter':18s} {'Estimate':>14s}")
print(f"  {'Intercept':18s} {lrModel.intercept:14.6f}")
for featureName, estimate in zip(featureColumns, lrModel.coefficients):
    print(f"  {featureName:18s} {estimate:14.6f}")

print("\nModel Convergence")
print(f"  Iterations:        {lrModel.summary.totalIterations}")
print(f"  Final objective:   {lrModel.summary.objectiveHistory[-1]:.6f}")
print(f"  Training AUC:      {lrModel.summary.areaUnderROC:.4f}")

# output out=work.train_scored predicted=pred_prob;
trainScored = logitModel.transform(trainPrepared) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1]) \
    .drop("features", "rawPrediction", "probability", "prediction")

print("\nTraining set scored (work.train_scored) - first 10 observations")
trainScored.select("BAD", *numericPredictors, "JOB", "REASON", "pred_prob") \
    .show(10, truncate=False)

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
#
# Notes:
#   - /ilink applies the inverse logit so pred_prob is a probability;
#     MLlib's "probability" vector already holds P(label=0), P(label=1).
#   - PROC PLM emits pred_prob = . for rows with a missing model variable;
#     handleInvalid="skip" drops those rows instead, so work.valid_scored
#     contains only scorable observations.
# ------------------------------------------------------------------
validPrepared = addReferenceDummies(valid, nonRefLevels) \
    .withColumn("label", col("BAD").cast("double"))

validScored = logitModel.transform(validPrepared) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1]) \
    .drop("features", "rawPrediction", "probability", "prediction")

print("\n" + "=" * 60)
print("Step 4: Validation set scored (equivalent to PROC PLM SCORE)")
print("=" * 60)
print(f"Validation rows scored: {validScored.count()}")
validScored.select("BAD", "LOAN", "DEBTINC", "JOB", "REASON", "pred_prob") \
    .show(10, truncate=False)

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
).cache()

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("=" * 60)
validScored.stat.crosstab("BAD", "PREDICTED_BAD") \
    .orderBy("BAD_PREDICTED_BAD") \
    .show()

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
# Note: this SAS step re-fits the model on the validation set and reports
# the "Association of Predicted Probabilities and Observed Responses"
# table. The same statistics are computed here from all (event, non-event)
# pairs of the validation-set fit: Percent Concordant / Discordant / Tied,
# Somers' D, Gamma, Tau-a and c (area under the ROC curve).
# ------------------------------------------------------------------
validFitModel = pipeline.fit(validPrepared)
validFitScored = validFitModel.transform(validPrepared) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1]) \
    .select("label", "pred_prob")

events = validFitScored.filter(col("label") == 1.0) \
    .select(col("pred_prob").alias("eventProb"))
nonEvents = validFitScored.filter(col("label") == 0.0) \
    .select(col("pred_prob").alias("nonEventProb"))

pairCounts = events.crossJoin(nonEvents).agg(
    count(when(col("eventProb") > col("nonEventProb"), 1)).alias("concordant"),
    count(when(col("eventProb") < col("nonEventProb"), 1)).alias("discordant"),
    count(when(col("eventProb") == col("nonEventProb"), 1)).alias("tied"),
).collect()[0]

concordant = pairCounts["concordant"]
discordant = pairCounts["discordant"]
tied = pairCounts["tied"]
totalPairs = concordant + discordant + tied
nObs = validFitScored.count()

associationStats = spark.createDataFrame(
    [
        ("Percent Concordant", 100.0 * concordant / totalPairs,
         "Somers' D", (concordant - discordant) / totalPairs),
        ("Percent Discordant", 100.0 * discordant / totalPairs,
         "Gamma", (concordant - discordant) / (concordant + discordant)),
        ("Percent Tied", 100.0 * tied / totalPairs,
         "Tau-a", (concordant - discordant) / (0.5 * nObs * (nObs - 1))),
        ("Pairs", float(totalPairs),
         "c", (concordant + 0.5 * tied) / totalPairs),
    ],
    ["Label1", "nValue1", "Label2", "nValue2"]
)

# roc; -> the ROC curve of the validation-set fit is available as a
# DataFrame (FPR, TPR) from the training summary.
print("\n" + "=" * 60)
print("Model Performance - Concordance and AUC")
print("=" * 60)
print(f"Validation-set fit AUC (c): {validFitModel.stages[-1].summary.areaUnderROC:.4f}")
print("ROC curve points (False Positive Rate, True Positive Rate) - first 10")
validFitModel.stages[-1].summary.roc.show(10)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Association Statistics")
print("=" * 60)
associationStats \
    .withColumn("nValue1", spark_round(col("nValue1"), 4)) \
    .withColumn("nValue2", spark_round(col("nValue2"), 4)) \
    .show(truncate=False)

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
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    spark_round(mean("pred_prob"), 4).alias("Mean"),
    spark_round(stddev("pred_prob"), 4).alias("Std_Dev"),
    spark_round(spark_min("pred_prob"), 4).alias("Minimum"),
    spark_round(expr("percentile(pred_prob, 0.25)"), 4).alias("P25"),
    spark_round(expr("percentile(pred_prob, 0.50)"), 4).alias("Median"),
    spark_round(expr("percentile(pred_prob, 0.75)"), 4).alias("P75"),
    spark_round(spark_max("pred_prob"), 4).alias("Maximum"),
).orderBy("BAD").show()

# Clean up
spark.stop()
