"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         (complete-case selection, 70/30 split, ML pipeline, AUC/confusion matrix)
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, mean, stddev, min as sparkMin,
    max as sparkMax, percentile_approx
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# Load the source dataset
# SAS equivalent: set work.home_equity;
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Step 0: Recreate the upstream cleaning / risk features inline so this
# script runs independently.
# SAS equivalent: work.home_equity_risk, built by
#   02_data_cleaning.sas (LTV, LOAN_OUTCOME) and
#   04_risk_segmentation.sas (RISK_SCORE, RISK_SEGMENT)
# ------------------------------------------------------------------
dfRisk = df \
    .withColumn(
        "LTV",
        # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        # SAS: if BAD = 0 then LOAN_OUTCOME = 'Paid'; else if BAD = 1 ...
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    )

# SAS: composite RISK_SCORE built by additive if/then blocks; a missing
# component contributes 0 points.
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

dfRisk = dfRisk.withColumn(
    "RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore
).withColumn(
    "RISK_SEGMENT",
    # SAS: PROC FORMAT value risk_score buckets
    when(col("RISK_SCORE") < 3, lit("Low Risk"))
    .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
    .when(col("RISK_SCORE") < 7, lit("High Risk"))
    .otherwise(lit("Very High Risk"))
)

# ------------------------------------------------------------------
# Step 1: Prepare the modeling dataset (complete cases only)
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# The CLASS predictors JOB and REASON must also be present for the
# encoders, so nulls are dropped for the full model column list.
# SAS missing (.) maps to null -> isNotNull().
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
).withColumn("label", col("BAD").cast("double"))

print("=" * 60)
print("Modeling Dataset (complete cases)")
print("=" * 60)
print(f"Rows available for modeling: {modelData.count()}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
#   data work.train work.valid;
#       set work.model_split;
#       if selected = 1 then output work.train; else output work.valid;
#   run;
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"Training rows: {train.count()}   Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit the logistic regression model
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON / selection=stepwise slentry=0.05 slstay=0.05;
#   run;
#
# CLASS ... / param=ref  -> StringIndexer + OneHotEncoder (reference coding)
# MODEL statement        -> VectorAssembler + LogisticRegression
# selection=stepwise     -> regularization (regParam / elasticNetParam), the
#                           Spark idiom for shrinking uninformative predictors;
#                           params match tests/test_pyspark_outputs.py
# ------------------------------------------------------------------
jobIdx = StringIndexer(
    inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep"
)
reasonIdx = StringIndexer(
    inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep"
)
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

assembler = VectorAssembler(
    inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
               "DELINQ", "DEROG", "CLAGE", "NINQ",
               "JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

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

# Expand the assembled vector metadata so each one-hot level is named,
# mirroring the PROC LOGISTIC parameter estimates table.
featureAttrs = model.transform(train).schema["features"] \
    .metadata["ml_attr"]["attrs"]
featureNames = [
    attr["name"]
    for group in featureAttrs.values()
    for attr in sorted(group, key=lambda a: a["idx"])
]

print("\n" + "=" * 60)
print("Model Coefficients (equivalent to PROC LOGISTIC parameter estimates)")
print("=" * 60)
print(f"Intercept: {lrModel.intercept:.6f}")
for name, coefficient in zip(featureNames, lrModel.coefficients):
    print(f"  {name:16s} -> {coefficient: .6f}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------
predictions = model.transform(valid)

# ------------------------------------------------------------------
# Step 5: Predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
#
# LogisticRegression already applies the 0.5 threshold in `prediction`.
# PROC FREQ cross-tabulation -> groupBy().pivot().count()
# ------------------------------------------------------------------
scored = predictions.withColumn(
    "PREDICTED_BAD", col("prediction").cast("int")
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set (PROC FREQ BAD * PREDICTED_BAD)")
print("=" * 60)
scored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).count() \
    .orderBy("BAD").show()

# ------------------------------------------------------------------
# Step 6: Model performance - AUC
# SAS equivalent:
#   proc logistic data=work.valid descending; ... roc;
#       ods output Association=work.association_stats;
#   run;
#
# ROC / concordance statistics -> BinaryClassificationEvaluator
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

print("\n" + "=" * 60)
print("Model Association Statistics - Validation Set")
print("=" * 60)
print(f"Area under ROC (c-statistic): {aucEvaluator.evaluate(predictions):.4f}")
print(f"Area under PR curve:          {prEvaluator.evaluate(predictions):.4f}")
print(f"Training AUC:                 {lrModel.summary.areaUnderROC:.4f}")

# ------------------------------------------------------------------
# Step 7: Predicted probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
#
# PROC MEANS with CLASS -> groupBy().agg();
# percentile_approx is used for p25/median/p75 (broadly compatible).
# ------------------------------------------------------------------
scoredProb = scored.withColumn(
    "pred_prob",
    # probability is an ML vector: element 1 is P(BAD = 1)
    vector_to_array(col("probability")).getItem(1)
)

print("\n" + "=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 60)
scoredProb.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("StdDev"),
    sparkMin("pred_prob").alias("Min"),
    percentile_approx("pred_prob", 0.25).alias("P25"),
    percentile_approx("pred_prob", 0.50).alias("Median"),
    percentile_approx("pred_prob", 0.75).alias("P75"),
    sparkMax("pred_prob").alias("Max")
).orderBy("BAD").show()

# ------------------------------------------------------------------
# Step 8: Default rate by risk segment among scored loans
# SAS equivalent: proc means data=work.valid_scored; class RISK_SEGMENT; var BAD;
# ------------------------------------------------------------------
print("=" * 60)
print("Validation Default Rate by Risk Segment")
print("=" * 60)
scoredProb.groupBy("RISK_SEGMENT").agg(
    count("BAD").alias("N"),
    mean("BAD").alias("Actual_Default_Rate"),
    mean("pred_prob").alias("Mean_Predicted_Prob")
).orderBy("RISK_SEGMENT").show()

# Clean up
spark.stop()
