"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction (BAD)
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, mean, count, stddev, udf, min as spark_min, max as spark_max
)
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load CSV data
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;
#   run;
# (The SAS program reads work.home_equity_risk, derived from the cleaned
#  data; this script loads the raw CSV directly so it is self-contained.)
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - keep only complete cases
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
).withColumn("label", col("BAD").cast("double"))

print("=" * 60)
print("Modeling dataset (complete cases)")
print("=" * 60)
print(f"Complete-case rows: {modelData.count()}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"Training rows: {train.count()}  Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression pipeline
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON / selection=stepwise;
#       store work.logit_model;
#   run;
#
# CLASS statements -> StringIndexer + OneHotEncoder
# ------------------------------------------------------------------
jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

# MODEL statement predictors -> VectorAssembler
assembler = VectorAssembler(
    inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
               "DELINQ", "DEROG", "CLAGE", "NINQ",
               "JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# PROC LOGISTIC -> Spark ML LogisticRegression
lr = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=50, regParam=0.01
)

pipeline = Pipeline(stages=[
    jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
])

model = pipeline.fit(train)
predictions = model.transform(valid)

# ------------------------------------------------------------------
# Model coefficients / summary (PROC LOGISTIC parameter estimates)
# ------------------------------------------------------------------
lrModel = model.stages[-1]
print("\n" + "=" * 60)
print("Logistic Regression Model Summary")
print("=" * 60)
print(f"Intercept: {lrModel.intercept:.6f}")
print("Coefficients:")
for i, coef in enumerate(lrModel.coefficients):
    print(f"  feature[{i:2d}] -> {coef:.6f}")

# ------------------------------------------------------------------
# Step 5: Confusion matrix on the validation set
# SAS equivalent:
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set (BAD * PREDICTED)")
print("=" * 60)
predictions.groupBy("label").pivot("prediction").count().orderBy("label").show()

# ------------------------------------------------------------------
# Step 6: Model performance - AUC
# SAS equivalent:
#   proc logistic data=work.valid descending; ... roc; run;
# ------------------------------------------------------------------
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator.evaluate(predictions)
print("\n" + "=" * 60)
print("Model Performance - Area Under ROC")
print("=" * 60)
print(f"AUC (areaUnderROC): {auc:.4f}")

# ------------------------------------------------------------------
# Step 8: Predicted-probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
# Extract P(BAD=1) from the probability vector
prob_bad = udf(lambda v: float(v[1]), DoubleType())
scored = predictions.withColumn("pred_prob", prob_bad(col("probability")))

print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome (BAD)")
print("=" * 60)
scored.groupBy("label").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("StdDev"),
    spark_min("pred_prob").alias("Min"),
    spark_max("pred_prob").alias("Max")
).orderBy("label").show()

# Clean up
spark.stop()
