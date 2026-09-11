"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, min as spark_min, max as spark_max, expr
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
        (col("LTV") > 0) & (col("LTV") < 5) &
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
    col("CLAGE").isNotNull()
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
# Step 3: Build ML pipeline
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON
#                 / selection=stepwise;
#       output out=work.train_scored predicted=pred_prob;
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

# PROC LOGISTIC / PROC PLM only use rows where every model variable is present
modelVariables = numericFeatures + ["JOB", "REASON"]

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

# ------------------------------------------------------------------
# Step 4: Train the model
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Training Logistic Regression Model")
print("=" * 60)

trainComplete = train.dropna(subset=modelVariables)
print(f"Training rows used for fit (complete predictors): {trainComplete.count()}")

model = pipeline.fit(trainComplete)

# Extract the logistic regression model from the pipeline
lrModel = model.stages[-1]

# Display model coefficients
print(f"\nIntercept: {lrModel.intercept:.4f}")
print(f"Number of features: {len(lrModel.coefficients)}")
print("\nCoefficients (non-zero):")
for i, coef in enumerate(lrModel.coefficients):
    if abs(coef) > 0.0001:
        print(f"  Feature {i}: {coef:.6f}")

# ------------------------------------------------------------------
# Step 5: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob;
#   run;
# ------------------------------------------------------------------
# Rows with a missing predictor get a missing pred_prob in SAS, which the
# DATA step then classifies as PREDICTED_BAD = 0
validComplete = valid.dropna(subset=modelVariables)
validIncomplete = valid.filter(
    " OR ".join(f"{c} IS NULL" for c in modelVariables)
)
print(f"Validation rows with missing predictors (scored as PREDICTED_BAD=0): {validIncomplete.count()}")

predictions = model.transform(validComplete) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

validScored = predictions.select("label", "prediction", "pred_prob").union(
    validIncomplete.select(
        col("label"),
        lit(0.0).alias("prediction"),
        lit(None).cast("double").alias("pred_prob")
    )
)

# ------------------------------------------------------------------
# Step 6: Create confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("(equivalent to PROC FREQ tables BAD * PREDICTED_BAD)")
print("=" * 60)

validScored.groupBy("label", "prediction") \
    .count() \
    .orderBy("label", "prediction") \
    .show()

# ------------------------------------------------------------------
# Step 7: Calculate model performance metrics
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
    value = multiEval.evaluate(validScored)
    print(f"{metricName}: {value:.4f}")

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

# PROC MEANS drops rows whose analysis variable is missing
validScored.filter(col("pred_prob").isNotNull()) \
    .groupBy("label") \
    .agg(
        count("pred_prob").alias("n"),
        mean("pred_prob").alias("mean"),
        stddev("pred_prob").alias("std"),
        spark_min("pred_prob").alias("min"),
        expr("percentile(pred_prob, 0.25)").alias("p25"),
        expr("percentile(pred_prob, 0.5)").alias("median"),
        expr("percentile(pred_prob, 0.75)").alias("p75"),
        spark_max("pred_prob").alias("max")
    ) \
    .orderBy("label") \
    .show(truncate=False)

# Clean up
spark.stop()
