"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas

Migrates PROC LOGISTIC (stepwise selection) to a Spark MLlib Pipeline:
  - CLASS statement          -> StringIndexer + OneHotEncoder
  - MODEL statement          -> VectorAssembler + LogisticRegression
  - SELECTION=STEPWISE       -> L1/L2 regularization (regParam / elasticNetParam)
  - OUTPUT PREDICTED=        -> model.transform() (prediction / probability)
  - concordance / AUC / ROC  -> BinaryClassificationEvaluator(areaUnderROC)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min as smin, max as smax
from pyspark.sql.functions import expr
from pyspark.ml.functions import vector_to_array
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
# Load the source dataset
# SAS equivalent:
#   The SAS program reads work.home_equity_risk, which derives from
#   work.home_equity loaded in 01_data_loading.sas. Each migrated
#   PySpark script is self-contained, so we re-load the raw CSV here
#   (same convention as 01_data_loading.py).
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
#
# The CLASS predictors JOB and REASON are also required for the model,
# so we additionally drop rows where they (or DEROG / NINQ) are missing.
# The target BAD is cast to a numeric `label` column for MLlib.
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
print("Step 1: Modeling dataset (complete cases)")
print("=" * 60)
print(f"Complete-case rows available for modeling: {modelData.count()}")

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
# PROC SURVEYSELECT simple random sampling -> DataFrame.randomSplit.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
print(f"\nTraining rows:   {train.count()}")
print(f"Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression with feature preparation
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
# Mapping:
#   CLASS ... / param=ref  -> StringIndexer + OneHotEncoder (reference encoding)
#   MODEL numeric vars     -> VectorAssembler input columns
#   SELECTION=STEPWISE     -> L2 regularization (regParam) shrinks weak
#                             predictors, the idiomatic Spark analog of
#                             SAS automated variable selection.
#   descending             -> BAD=1 (default) is the modeled event; MLlib
#                             already treats label=1 as the positive class.
# ------------------------------------------------------------------
# CLASS statement -> index then one-hot encode the categorical predictors.
jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

# MODEL statement -> assemble numeric + encoded categorical features.
assembler = VectorAssembler(
    inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
               "DELINQ", "DEROG", "CLAGE", "NINQ",
               "JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# SELECTION=STEPWISE -> regularized logistic regression.
lr = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=50, regParam=0.01
)

pipeline = Pipeline(stages=[
    jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
])

model = pipeline.fit(train)

# Print the fitted coefficients (equivalent to the PROC LOGISTIC
# parameter estimates table).
lrModel = model.stages[-1]
print("\n" + "=" * 60)
print("Step 3: Fitted Logistic Regression (parameter estimates)")
print("=" * 60)
print(f"Intercept: {lrModel.intercept:.6f}")
print("Coefficients:")
for name, coef in zip(assembler.getInputCols(), lrModel.coefficients):
    print(f"  {name:12s} -> {coef: .6f}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
#
# model.transform() applies the fitted pipeline, producing the
# `probability`, `rawPrediction` and `prediction` columns.
# We extract the P(BAD=1) component as `pred_prob` (ilink = probability scale).
# ------------------------------------------------------------------
predictions = model.transform(valid)
# `probability` is a Spark ML vector [P(BAD=0), P(BAD=1)]; convert to an
# array and take the second element to obtain P(BAD=1) on the probability
# scale (equivalent to the SAS `/ ilink` option).
predictions = predictions.withColumn(
    "pred_prob", vector_to_array(col("probability"))[1]
)

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
#
# LogisticRegression uses a 0.5 threshold by default, so the model's
# `prediction` column already equals PREDICTED_BAD. PROC FREQ crosstab
# -> groupBy / crosstab.
# ------------------------------------------------------------------
predictions = predictions.withColumn(
    "PREDICTED_BAD", col("prediction").cast("int")
)

print("\n" + "=" * 60)
print("Step 5: Confusion Matrix - Validation Set (BAD x PREDICTED_BAD)")
print("=" * 60)
predictions.crosstab("BAD", "PREDICTED_BAD").orderBy("BAD_PREDICTED_BAD").show()

# ------------------------------------------------------------------
# Step 6: Calculate model performance metrics (concordance / AUC)
# SAS equivalent:
#   proc logistic data=work.valid descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = ... ;
#       roc;
#       ods output Association=work.association_stats;
#   run;
#
# The SAS Association table reports the c-statistic (== area under the
# ROC curve). BinaryClassificationEvaluator(areaUnderROC) is the direct
# equivalent.
# ------------------------------------------------------------------
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator.evaluate(predictions)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 7: Model Association Statistics")
print("=" * 60)
print(f"  Area Under ROC (c-statistic): {auc:.4f}")
print(f"  Somers' D (2*AUC - 1):        {2 * auc - 1:.4f}")

# ------------------------------------------------------------------
# Step 8: Predicted probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
#
# PROC MEANS with a CLASS variable -> groupBy().agg(); quantiles via
# percentile_approx (p25 / median / p75).
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 8: Predicted Probability Distribution by Actual Outcome (BAD)")
print("=" * 60)
predictions.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("Std"),
    smin("pred_prob").alias("Min"),
    expr("percentile_approx(pred_prob, 0.25)").alias("P25"),
    expr("percentile_approx(pred_prob, 0.50)").alias("Median"),
    expr("percentile_approx(pred_prob, 0.75)").alias("P75"),
    smax("pred_prob").alias("Max"),
).orderBy("BAD").show()

# Clean up
spark.stop()
