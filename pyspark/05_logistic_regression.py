"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from bisect import bisect_left, bisect_right

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, min as spark_min,
    max as spark_max, expr, round as spark_round
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from scripts 02/04)
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
df = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull() &
        (col("LOAN") > 0) & (col("VALUE") > 0)
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
    col("LOAN").isNotNull() &
    col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() &
    col("CLAGE").isNotNull() &
    col("DEROG").isNotNull() &
    col("NINQ").isNotNull() &
    col("JOB").isNotNull() &
    col("REASON").isNotNull()
)

# Cast BAD to double for ML
modelData = modelData.withColumn("label", col("BAD").cast("double"))

print(f"Modeling dataset size: {modelData.count()} rows")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data
#       out=work.model_split method=srs samprate=0.7 seed=42;
#   run;
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"Training set: {train.count()} rows")
print(f"Validation set: {valid.count()} rows")

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
# PySpark uses a Pipeline with StringIndexer, OneHotEncoder,
# VectorAssembler, and LogisticRegression.
# ------------------------------------------------------------------

# Index categorical variables (equivalent to CLASS statement)
jobIndexer = StringIndexer(
    inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep"
)
reasonIndexer = StringIndexer(
    inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep"
)

# One-hot encode categorical variables (equivalent to param=ref)
jobEncoder = OneHotEncoder(
    inputCol="JOB_IDX", outputCol="JOB_VEC"
)
reasonEncoder = OneHotEncoder(
    inputCol="REASON_IDX", outputCol="REASON_VEC"
)

# Numeric feature columns
numericFeatures = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "DELINQ", "DEROG", "CLAGE", "NINQ"
]

# Assemble all features into a single vector
assembler = VectorAssembler(
    inputCols=numericFeatures + ["JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# Logistic regression model
# SAS equivalent: PROC LOGISTIC with selection=stepwise
# Note: PySpark's LogisticRegression uses elasticNet for regularization
# which provides built-in feature selection similar to stepwise
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.8,  # L1 ratio for feature sparsity (like stepwise)
    threshold=0.5
)

# Build the pipeline
pipeline = Pipeline(stages=[
    jobIndexer, reasonIndexer,
    jobEncoder, reasonEncoder,
    assembler, lr
])

print("\n" + "=" * 60)
print("Training Logistic Regression Model")
print("=" * 60)

model = pipeline.fit(train)

# Extract the fitted stages from the pipeline
jobIndexerModel = model.stages[0]
reasonIndexerModel = model.stages[1]
lrModel = model.stages[-1]

# Feature names in assembler order: numerics, then the one-hot dummy
# columns for each CLASS variable (handleInvalid="keep" reserves the
# trailing slot that OneHotEncoder drops, so every observed level keeps
# a dummy column)
featureNames = (
    numericFeatures +
    [f"JOB_{lbl}" for lbl in jobIndexerModel.labels] +
    [f"REASON_{lbl}" for lbl in reasonIndexerModel.labels]
)

# Display model coefficients (equivalent to the PROC LOGISTIC
# parameter estimates table; L1 regularization zeroes out weak
# predictors, playing the role of stepwise selection)
print(f"\nIntercept: {lrModel.intercept:.4f}")
print(f"Number of features: {len(lrModel.coefficients)}")
print("\nCoefficients (non-zero - variables retained by selection):")
for i, coef in enumerate(lrModel.coefficients):
    if abs(coef) > 0.0001:
        name = featureNames[i] if i < len(featureNames) else f"Feature {i}"
        print(f"  {name}: {coef:.6f}")

print("\nVariables dropped by selection (zero coefficient):")
droppedFeatures = [
    featureNames[i] if i < len(featureNames) else f"Feature {i}"
    for i, coef in enumerate(lrModel.coefficients)
    if abs(coef) <= 0.0001
]
print(f"  {droppedFeatures if droppedFeatures else 'None'}")

# Training-set scores
# SAS equivalent: output out=work.train_scored predicted=pred_prob;
trainScored = model.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

print("\nTraining set scores (work.train_scored) - first 5 rows:")
trainScored.select("BAD", "pred_prob").show(5)

# Goodness-of-fit check on the training data
# SAS equivalent: the LACKFIT (Hosmer-Lemeshow) option; PySpark has no
# direct equivalent, so compare observed vs predicted default rates
print("Training fit check (observed vs mean predicted default rate):")
trainScored.agg(
    spark_round(mean("label"), 4).alias("Observed_Rate"),
    spark_round(mean("pred_prob"), 4).alias("Predicted_Rate")
).show()

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob
#           / ilink;
#   run;
# ------------------------------------------------------------------
# vector_to_array()[1] is the modelled probability of BAD=1, i.e. the
# ILINK-transformed predicted value SAS writes as pred_prob
predictions = model.transform(valid) \
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
validScored = predictions.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("(equivalent to PROC FREQ tables BAD * PREDICTED_BAD)")
print("=" * 60)

# Cross-tabulation with BAD on rows and PREDICTED_BAD on columns,
# frequencies only (nopercent norow nocol)
validScored.groupBy("BAD") \
    .pivot("PREDICTED_BAD", [0, 1]) \
    .agg(count("*")) \
    .na.fill(0) \
    .orderBy("BAD") \
    .show()

# ------------------------------------------------------------------
# Step 6: Calculate model performance metrics
# SAS equivalent:
#   proc logistic ... ;
#       roc;
#       ods output Association=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Model Performance Metrics")
print("(equivalent to PROC LOGISTIC concordance/AUC)")
print("=" * 60)

# AUC - Area Under ROC Curve
# SAS equivalent: c statistic / concordance
binaryEval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = binaryEval.evaluate(predictions)
print(f"\nAUC (Area Under ROC): {auc:.4f}")

# Area Under PR Curve
binaryEvalPr = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)
aupr = binaryEvalPr.evaluate(predictions)
print(f"AUPR (Area Under PR Curve): {aupr:.4f}")

# Accuracy, Precision, Recall, F1
multiEval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction"
)

for metricName in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    multiEval.setMetricName(metricName)
    value = multiEval.evaluate(predictions)
    print(f"{metricName}: {value:.4f}")

# Rank-order association statistics between the predicted probability
# and the observed response, as produced by the PROC LOGISTIC
# "Association of Predicted Probabilities and Observed Responses" table
scoredPairs = validScored.select("label", "pred_prob").collect()
eventProbs = sorted(r["pred_prob"] for r in scoredPairs if r["label"] == 1.0)
nonEventProbs = sorted(r["pred_prob"] for r in scoredPairs if r["label"] == 0.0)

concordant = 0
discordant = 0
tied = 0
for p in eventProbs:
    lower = bisect_left(nonEventProbs, p)
    upper = bisect_right(nonEventProbs, p)
    concordant += lower
    tied += upper - lower
    discordant += len(nonEventProbs) - upper

totalPairs = len(eventProbs) * len(nonEventProbs)
totalObs = len(scoredPairs)
cStatistic = (concordant + 0.5 * tied) / totalPairs
somersD = (concordant - discordant) / totalPairs
gamma = (concordant - discordant) / (concordant + discordant)
tauA = (concordant - discordant) / (0.5 * totalObs * (totalObs - 1))

associationStats = spark.createDataFrame(
    [
        ("Percent Concordant", round(100.0 * concordant / totalPairs, 4)),
        ("Percent Discordant", round(100.0 * discordant / totalPairs, 4)),
        ("Percent Tied", round(100.0 * tied / totalPairs, 4)),
        ("Pairs", float(totalPairs)),
        ("Somers' D", round(somersD, 4)),
        ("Gamma", round(gamma, 4)),
        ("Tau-a", round(tauA, 4)),
        ("c", round(cStatistic, 4)),
    ],
    ["Label", "Value"]
)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
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
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("(equivalent to PROC MEANS with CLASS BAD)")
print("=" * 60)

# n mean std min p25 median p75 max, classified by BAD
validScored.groupBy("BAD") \
    .agg(
        count("pred_prob").alias("N"),
        spark_round(mean("pred_prob"), 4).alias("Mean"),
        spark_round(stddev("pred_prob"), 4).alias("Std_Dev"),
        spark_round(spark_min("pred_prob"), 4).alias("Minimum"),
        spark_round(expr("percentile_approx(pred_prob, 0.25)"), 4).alias("P25"),
        spark_round(expr("percentile_approx(pred_prob, 0.5)"), 4).alias("Median"),
        spark_round(expr("percentile_approx(pred_prob, 0.75)"), 4).alias("P75"),
        spark_round(spark_max("pred_prob"), 4).alias("Maximum")
    ) \
    .orderBy("BAD") \
    .show(truncate=False)

# Clean up
spark.stop()
