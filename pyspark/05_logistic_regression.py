"""
PySpark Script: 05_logistic_regression.py
Purpose: Logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, expr,
    min as sparkMin, max as sparkMax, round as sparkRound
)
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexerModel, VectorAssembler
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Rebuild work.home_equity_final (see 02_data_cleaning.py)
# ------------------------------------------------------------------
dfFinal = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True) \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")).otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY"))) \
    .filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()) \
    .filter((col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0))

# ------------------------------------------------------------------
# Step 1: Modeling dataset - complete cases only
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# PROC LOGISTIC also drops rows with missing CLASS or model variables,
# so DEROG, NINQ, JOB and REASON are required as well.
# ------------------------------------------------------------------
modelData = dfFinal.filter(
    col("LOAN").isNotNull() & col("MORTDUE").isNotNull() & col("VALUE").isNotNull()
    & col("DEBTINC").isNotNull() & col("DELINQ").isNotNull() & col("CLAGE").isNotNull()
    & col("DEROG").isNotNull() & col("NINQ").isNotNull()
    & col("JOB").isNotNull() & col("REASON").isNotNull()
).withColumn("label", col("BAD").cast("double")).cache()

print(f"Complete cases available for modeling: {modelData.count()}")

# ------------------------------------------------------------------
# Step 2: 70/30 train/validation split
# SAS equivalent:
#   proc surveyselect method=srs samprate=0.7 seed=42;
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"Training rows: {train.count()}, Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Encode CLASS variables with reference levels
# SAS equivalent:
#   class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#
# OneHotEncoder drops the last index, so the reference level is ordered last.
# ------------------------------------------------------------------
def refOrderedLabels(dataFrame, columnName, refLevel):
    """Distinct levels with the SAS reference level placed last."""
    levels = sorted(
        row[columnName] for row in dataFrame.select(columnName).distinct().collect()
    )
    return [level for level in levels if level != refLevel] + [refLevel]


jobIdx = StringIndexerModel.from_labels(
    refOrderedLabels(modelData, "JOB", "Other"),
    inputCol="JOB", outputCol="JOB_IDX", handleInvalid="skip"
)
reasonIdx = StringIndexerModel.from_labels(
    refOrderedLabels(modelData, "REASON", "HomeImp"),
    inputCol="REASON", outputCol="REASON_IDX", handleInvalid="skip"
)
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

numericFeatures = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
assembler = VectorAssembler(
    inputCols=numericFeatures + ["JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# ------------------------------------------------------------------
# Step 4: Fit the model
# SAS equivalent:
#   proc logistic data=work.train descending;
#       model BAD = ... / selection=stepwise slentry=0.05 slstay=0.05;
#
# Spark MLlib has no stepwise selection; L1 (LASSO) regularization performs
# the equivalent job of shrinking uninformative coefficients to zero.
# ------------------------------------------------------------------
lr = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=100, regParam=0.01, elasticNetParam=1.0, standardization=True
)

pipeline = Pipeline(stages=[jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr])
model = pipeline.fit(train)
lrModel = model.stages[-1]

featureNames = numericFeatures \
    + [f"JOB={level}" for level in jobIdx.labels[:-1]] \
    + [f"REASON={level}" for level in reasonIdx.labels[:-1]]

print("\n" + "=" * 70)
print("Logistic Regression: Loan Default Prediction")
print("Reference levels: JOB='Other', REASON='HomeImp'")
print("=" * 70)
print(f"{'Effect':22s} {'Estimate':>14s}")
print(f"{'Intercept':22s} {lrModel.intercept:14.6f}")
for name, coefficient in zip(featureNames, lrModel.coefficients.toArray()):
    selected = "" if coefficient != 0.0 else "   (dropped by L1)"
    print(f"{name:22s} {coefficient:14.6f}{selected}")

# ------------------------------------------------------------------
# Step 5: Score the validation set and build the confusion matrix
# SAS equivalent: proc plm score ... then proc freq BAD * PREDICTED_BAD
# ------------------------------------------------------------------
predictions = model.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1]) \
    .withColumn("PREDICTED_BAD", when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))) \
    .cache()

print("\n" + "=" * 70)
print("Confusion Matrix - Validation Set")
print("=" * 70)
predictions.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).agg(count(lit(1))) \
    .orderBy("BAD").show(truncate=False)

correct = predictions.filter(col("BAD") == col("PREDICTED_BAD")).count()
validCount = predictions.count()
print(f"Accuracy: {correct / validCount:.4f}")

# ------------------------------------------------------------------
# Step 6: Model performance
# SAS equivalent: proc logistic ... roc; ods output Association=...;
# SAS "c statistic" (concordance) is the area under the ROC curve.
# ------------------------------------------------------------------
auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(predictions)
aucPr = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
).evaluate(predictions)

print("\n" + "=" * 70)
print("Model Association Statistics")
print("=" * 70)
print(f"Area under ROC (c statistic): {auc:.4f}")
print(f"Area under PR curve:          {aucPr:.4f}")
print(f"Somers' D (2 * AUC - 1):      {2 * auc - 1:.4f}")

# ------------------------------------------------------------------
# Step 7: Predicted probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD; var pred_prob;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 70)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 70)
predictions.groupBy("BAD").agg(
    count(col("pred_prob")).alias("N"),
    sparkRound(mean(col("pred_prob")), 4).alias("Mean"),
    sparkRound(stddev(col("pred_prob")), 4).alias("Std"),
    sparkRound(sparkMin(col("pred_prob")), 4).alias("Min"),
    sparkRound(expr("percentile_approx(pred_prob, 0.25)"), 4).alias("P25"),
    sparkRound(expr("percentile_approx(pred_prob, 0.5)"), 4).alias("Median"),
    sparkRound(expr("percentile_approx(pred_prob, 0.75)"), 4).alias("P75"),
    sparkRound(sparkMax(col("pred_prob")), 4).alias("Max"),
).orderBy("BAD").show(truncate=False)

spark.stop()
