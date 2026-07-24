"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas

Migrates PROC LOGISTIC (CLASS statement + stepwise selection, ROC/AUC) to an
idiomatic pyspark.ml Pipeline:
  StringIndexer + OneHotEncoder (CLASS vars) -> VectorAssembler -> LogisticRegression
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    expr, round as spark_round
)
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

# Initialize SparkSession (self-contained, no shared helper module)
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# Model predictors (numeric + categorical CLASS variables)
NUMERIC_FEATURES = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "DELINQ", "DEROG", "CLAGE", "NINQ",
]
CATEGORICAL_FEATURES = ["JOB", "REASON"]

# ------------------------------------------------------------------
# Step 1: Prepare the modeling dataset
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# PROC LOGISTIC additionally drops rows missing any other model variable, so we
# keep complete cases across every predictor. BAD becomes the ML label.
# ------------------------------------------------------------------
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

modelData = df
for c in NUMERIC_FEATURES + CATEGORICAL_FEATURES + ["BAD"]:
    modelData = modelData.filter(col(c).isNotNull())

modelData = modelData.withColumn("label", col("BAD").cast("double"))
print(f"Modeling dataset size: {modelData.count()} rows")

# ------------------------------------------------------------------
# Step 2: Train/validation split (70/30, fixed seed)
# SAS equivalent:
#   proc surveyselect data=work.model_data method=srs samprate=0.7 seed=42;
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"Training set:   {train.count()} rows")
print(f"Validation set: {valid.count()} rows")

# ------------------------------------------------------------------
# Step 3: Build the ML pipeline
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON
#             / selection=stepwise;
#   run;
#
# CLASS statement       -> StringIndexer + OneHotEncoder (param=ref reference coding)
# selection=stepwise    -> ElasticNet (L1) regularization for feature sparsity
# ------------------------------------------------------------------
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_IDX", handleInvalid="keep")
    for c in CATEGORICAL_FEATURES
]
encoders = [
    OneHotEncoder(inputCol=f"{c}_IDX", outputCol=f"{c}_VEC")
    for c in CATEGORICAL_FEATURES
]

assembler = VectorAssembler(
    inputCols=NUMERIC_FEATURES + [f"{c}_VEC" for c in CATEGORICAL_FEATURES],
    outputCol="features",
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.8,  # L1-heavy: drives weak coefficients to 0 (like stepwise)
)

pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

# ------------------------------------------------------------------
# Step 4: Fit the model and report coefficients
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Training Logistic Regression Model")
print("=" * 60)

model = pipeline.fit(train)
lrModel = model.stages[-1]

# Recover assembled feature names from the vector's ML metadata so each
# coefficient is labeled with the predictor it belongs to.
featureAttrs = (
    model.transform(train.limit(1))
    .schema["features"]
    .metadata["ml_attr"]["attrs"]
)
featureNames = [None] * len(lrModel.coefficients)
for attrs in featureAttrs.values():
    for attr in attrs:
        featureNames[attr["idx"]] = attr["name"]

print(f"\nIntercept: {lrModel.intercept:.4f}")
print(f"Number of features: {len(lrModel.coefficients)}")
print("\nCoefficients:")
for name, coef in zip(featureNames, lrModel.coefficients):
    marker = "" if abs(coef) > 1e-6 else "   (dropped by L1)"
    print(f"  {name:<24} {coef: .6f}{marker}")

# ------------------------------------------------------------------
# Step 5: Score the validation set
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------
# Probability of default (class 1) as an explicit column, like SAS pred_prob.
# probability is an ML vector, so convert to an array before indexing.
predictions = model.transform(valid).withColumn(
    "pred_prob", vector_to_array(col("probability"))[1]
)

# ------------------------------------------------------------------
# Step 6: Confusion matrix
# SAS equivalent:
#   data work.valid_scored; if pred_prob >= 0.5 then PREDICTED_BAD=1; ...
#   proc freq data=work.valid_scored; tables BAD * PREDICTED_BAD; run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("(rows = actual BAD, cols = predicted)")
print("=" * 60)

predictions.groupBy("label", "prediction") \
    .agg(count("*").alias("n")) \
    .orderBy("label", "prediction") \
    .show()

# ------------------------------------------------------------------
# Step 7: Model performance metrics
# SAS equivalent:
#   proc logistic ...; roc; ods output Association=...; (concordance / c statistic)
# ------------------------------------------------------------------
print("=" * 60)
print("Model Performance Metrics")
print("=" * 60)

auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(predictions)
aupr = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
).evaluate(predictions)
print(f"\nAUC (Area Under ROC): {auc:.4f}   # SAS c statistic / concordance")
print(f"AUPR (Area Under PR): {aupr:.4f}")

multiEval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
for metricName in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    value = multiEval.setMetricName(metricName).evaluate(predictions)
    print(f"{metricName}: {value:.4f}")

# ------------------------------------------------------------------
# Step 8: Predicted probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD; var pred_prob;
#   run;
#
# NOTE: use explicit column aggregations. A dict like
#   .agg({"pred_prob": "count", "pred_prob": "mean"})
# would silently drop the "count" (duplicate dict key), keeping only the mean.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 60)

predictions.groupBy("label").agg(
    count("pred_prob").alias("n"),
    spark_round(mean("pred_prob"), 4).alias("mean_prob"),
    spark_round(stddev("pred_prob"), 4).alias("std_prob"),
    spark_round(spark_min("pred_prob"), 4).alias("min_prob"),
    spark_round(expr("percentile_approx(pred_prob, 0.25)"), 4).alias("p25_prob"),
    spark_round(expr("percentile_approx(pred_prob, 0.50)"), 4).alias("median_prob"),
    spark_round(expr("percentile_approx(pred_prob, 0.75)"), 4).alias("p75_prob"),
    spark_round(spark_max("pred_prob"), 4).alias("max_prob"),
).orderBy("label").show()

spark.stop()
