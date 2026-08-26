"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - PROC LOGISTIC with stepwise selection
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lit, when

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Rebuild the upstream cleaned/segmented dataset inline
# SAS equivalent: work.home_equity_risk, produced by
#   sas/02_data_cleaning.sas (LTV, LOAN_OUTCOME, null filter, outlier trim)
#   sas/04_risk_segmentation.sas (risk categories - not used as predictors)
# This keeps the script runnable standalone from the repo root.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

dfClean = df.withColumn(
    "LTV",
    when(
        (col("VALUE").isNotNull()) &
        (col("MORTDUE").isNotNull()) &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn(
    "CITY",
    initcap(col("CITY"))
)

dfRisk = dfClean.filter(
    col("LOAN").isNotNull() &
    col("VALUE").isNotNull() &
    col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) &
    (col("LOAN") > 0) &
    (col("VALUE") > 0)
)

print("=" * 60)
print("Upstream Cleaned Dataset (work.home_equity_risk)")
print("=" * 60)
print(f"Rows available for modeling input: {dfRisk.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - keep complete cases only
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# Note: DEROG, NINQ, JOB and REASON are also required to be non-missing
# because they enter the MODEL / CLASS statements below; Spark ML cannot
# assemble null feature values.
# ------------------------------------------------------------------
modelData = dfRisk.filter(
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
).withColumn(
    # SAS equivalent: proc logistic ... descending; (model BAD=1 as the event)
    "label",
    col("BAD").cast("double")
)

print("\n" + "=" * 60)
print("Modeling Dataset (work.model_data)")
print("=" * 60)
print(f"Complete cases: {modelData.count()}")

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
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)

print(f"Training rows:   {train.count()}")
print(f"Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON
#           / selection=stepwise slentry=0.05 slstay=0.05 details lackfit;
#       output out=work.train_scored predicted=pred_prob;
#       store work.logit_model;
#   run;
#
# Notes:
#   - CLASS / param=ref becomes StringIndexer + OneHotEncoder
#   - MODEL becomes VectorAssembler + LogisticRegression
#   - SELECTION=STEPWISE has no direct equivalent; regularization
#     (regParam) performs the shrinkage/feature-selection role
# ------------------------------------------------------------------
jobIdx = StringIndexer(
    inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep"
)
reasonIdx = StringIndexer(
    inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep"
)
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

featureCols = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "DELINQ", "DEROG", "CLAGE", "NINQ",
    "JOB_VEC", "REASON_VEC"
]
assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    regParam=0.01
)

pipeline = Pipeline(stages=[
    jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
])

model = pipeline.fit(train)
lrModel = model.stages[-1]

# One-hot encoded columns expand to several coefficients each, so the
# printed names are taken from the assembled feature vector metadata
# (the SAS equivalent is the 'Analysis of Maximum Likelihood Estimates' table).
featuresMetadata = model.transform(train).schema["features"].metadata
attrs = featuresMetadata["ml_attr"]["attrs"]
featureNames = [
    attr["name"]
    for attrGroup in attrs.values()
    for attr in sorted(attrGroup, key=lambda a: a["idx"])
]

print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)
print(f"Intercept: {lrModel.intercept:.6f}")
print("Coefficients (assembled feature order):")
for featureName, coefficient in zip(featureNames, lrModel.coefficients):
    print(f"  {featureName:14s} -> {coefficient:.6f}")

# SAS equivalent: output out=work.train_scored predicted=pred_prob;
trainScored = model.transform(train)
print("\nTraining Set Fit (work.train_scored)")
trainEvaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
print(f"  Training AUC: {trainEvaluator.evaluate(trainScored):.4f}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------
validScored = model.transform(valid)

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#   title "Confusion Matrix - Validation Set";
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD",
    when(col("prediction") >= 0.5, lit(1)).otherwise(lit(0))
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set (PROC FREQ)")
print("=" * 60)
validScored.crosstab("BAD", "PREDICTED_BAD").show(truncate=False)

# ------------------------------------------------------------------
# Step 6: Calculate model performance metrics
# SAS equivalent:
#   title "Model Performance - Concordance and AUC";
#   proc logistic data=work.valid descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON;
#       roc;
#       ods output Association=work.association_stats;
#   run;
#
# Note: SAS reports concordance from the rank correlation of predicted
# probabilities; areaUnderROC is the same quantity (c statistic).
# ------------------------------------------------------------------
aucEvaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
prEvaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

validAuc = aucEvaluator.evaluate(validScored)
validPr = prEvaluator.evaluate(validScored)

correctCount = validScored.filter(
    col("label") == col("PREDICTED_BAD")
).count()
validCount = validScored.count()

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   title "Model Association Statistics";
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
associationStats = {
    "Area Under ROC (c statistic)": validAuc,
    "Somers' D (2 * AUC - 1)": 2 * validAuc - 1,
    "Area Under PR Curve": validPr,
    "Accuracy": correctCount / validCount if validCount else 0.0,
}

print("\n" + "=" * 60)
print("Model Association Statistics (work.association_stats)")
print("=" * 60)
for statName, statValue in associationStats.items():
    print(f"  {statName:30s} -> {statValue:.4f}")

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   title "Predicted Probability Distribution by Actual Outcome";
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
#
# Note: the SAS pred_prob is the modeled event probability; in Spark it is
# element 1 of the probability vector, extracted here for reporting.
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "pred_prob",
    vector_to_array(col("probability")).getItem(1)
)

print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome (PROC MEANS)")
print("=" * 60)
for badValue in [0, 1]:
    subset = validScored.filter(col("BAD") == badValue)
    print(f"\nBAD = {badValue}")
    subset.select("pred_prob").summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    ).show(truncate=False)

print("\n" + "=" * 60)
print("Scored Validation Sample (PROC PRINT)")
print("=" * 60)
validScored.select(
    "BAD", "PREDICTED_BAD", "pred_prob", "LOAN", "MORTDUE",
    "VALUE", "DEBTINC", "DELINQ", "DEROG", "JOB", "REASON"
).show(10, truncate=False)

# Clean up
spark.stop()
