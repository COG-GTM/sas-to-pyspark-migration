"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Logistic regression with reference-coded CLASS variables
         - Model evaluation (concordance, AUC)
         - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as spark_min,
    max as spark_max, percentile_approx
)
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
# Step 0: Load CSV data and reproduce upstream cleaning inline
# SAS equivalent (02_data_cleaning.sas -> work.home_equity_final):
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_imputed;  /* array of <COL>_MISS 0/1 flags */
#   data work.home_equity_filtered; if LOAN ne . and VALUE ne . and BAD ne .;
#   data work.home_equity_final;
#       if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
#   run;
#
# Note: 04_risk_segmentation.sas (work.home_equity_risk) only adds derived
# risk columns and drops no rows; none of those columns are model inputs,
# so work.home_equity_final is sufficient here.
# ------------------------------------------------------------------
dataPath = "data/home_equity.csv"
df = spark.read.csv(dataPath, header=True, inferSchema=True)
print(f"Number of rows (source): {df.count()}")

cleanDf = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None).cast("double"))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, "Paid")
        .when(col("BAD") == 1, "Default")
        .otherwise(lit(None).cast("string"))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

missFlagColumns = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for colName in missFlagColumns:
    cleanDf = cleanDf.withColumn(
        f"{colName}_MISS", when(col(colName).isNull(), 1).otherwise(0)
    )

filteredDf = cleanDf.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

finalDf = filteredDf.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)
print(f"Number of rows (home_equity_final): {finalDf.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - complete cases only
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# Note: PROC LOGISTIC silently drops observations with a missing value in
# any MODEL or CLASS variable (DEROG, NINQ, JOB, REASON). VectorAssembler
# fails on nulls instead, so those exclusions are made explicit here.
# ------------------------------------------------------------------
numericFeatures = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                   "DELINQ", "DEROG", "CLAGE", "NINQ"]
classFeatures = ["JOB", "REASON"]

modelData = finalDf
for colName in numericFeatures + classFeatures:
    modelData = modelData.filter(col(colName).isNotNull())
modelData = modelData.withColumn("label", col("BAD").cast("double"))
print(f"Number of rows (model_data): {modelData.count()}")

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
# Note: randomSplit is approximate (Bernoulli per row), and Spark's RNG
# differs from SAS, so the exact rows in each partition will differ.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
train = train.cache()
valid = valid.cache()
print(f"Number of rows (train): {train.count()}")
print(f"Number of rows (valid): {valid.count()}")

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
# CLASS ... (ref='X') / param=ref: the reference level is placed last in a
# fixed label order, and OneHotEncoder(dropLast=True) drops it, giving the
# same reference-cell coding as SAS.
# DESCENDING: MLlib models P(label = 1), i.e. P(BAD = 1), by default.
# SELECTION=STEPWISE / LACKFIT: MLlib has no p-value-driven stepwise
# selection or Hosmer-Lemeshow test; the full model is fitted unpenalized
# (regParam=0.0) to match PROC LOGISTIC's maximum-likelihood estimates.
# ------------------------------------------------------------------
jobLevels = ["Mgr", "Office", "ProfExe", "Sales", "Self", "Other"]   # ref='Other'
reasonLevels = ["DebtCon", "HomeImp"]                                # ref='HomeImp'

jobIndexer = StringIndexerModel.from_labels(
    jobLevels, inputCol="JOB", outputCol="JOB_IDX", handleInvalid="error"
)
reasonIndexer = StringIndexerModel.from_labels(
    reasonLevels, inputCol="REASON", outputCol="REASON_IDX", handleInvalid="error"
)
encoder = OneHotEncoder(
    inputCols=["JOB_IDX", "REASON_IDX"],
    outputCols=["JOB_VEC", "REASON_VEC"],
    dropLast=True
)
assembler = VectorAssembler(
    inputCols=numericFeatures + ["JOB_VEC", "REASON_VEC"],
    outputCol="features"
)
logReg = LogisticRegression(
    featuresCol="features", labelCol="label",
    maxIter=100, regParam=0.0
)
pipeline = Pipeline(stages=[jobIndexer, reasonIndexer, encoder, assembler, logReg])

logitModel = pipeline.fit(train)
lrModel = logitModel.stages[-1]

featureNames = numericFeatures \
    + [f"JOB_{level}" for level in jobLevels[:-1]] \
    + [f"REASON_{level}" for level in reasonLevels[:-1]]

print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)
print(f"  {'Parameter':18s} {'Estimate':>14s}")
print(f"  {'Intercept':18s} {lrModel.intercept:14.6f}")
for name, coef in zip(featureNames, lrModel.coefficients.toArray()):
    print(f"  {name:18s} {coef:14.6f}")

trainScored = logitModel.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
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
    "PREDICTED_BAD", when(col("pred_prob") >= 0.5, 1).otherwise(0)
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set (equivalent to PROC FREQ)")
print("=" * 60)
validScored.groupBy("BAD") \
    .pivot("PREDICTED_BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .withColumnRenamed("0", "PREDICTED_BAD=0") \
    .withColumnRenamed("1", "PREDICTED_BAD=1") \
    .orderBy("BAD") \
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
# The SAS step refits the full model on the validation set and reports its
# association statistics. The c statistic (concordance) equals the area
# under the ROC curve, and Somers' D = 2c - 1. The holdout AUC of the
# training model is reported alongside for out-of-sample performance.
# ------------------------------------------------------------------
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

validRefitModel = pipeline.fit(valid)
validRefitAuc = evaluator.evaluate(validRefitModel.transform(valid))
holdoutAuc = evaluator.evaluate(validScored)
trainAuc = evaluator.evaluate(trainScored)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
associationStats = spark.createDataFrame(
    [
        ("c (AUC) - validation refit", float(validRefitAuc)),
        ("Somers' D - validation refit", float(2 * validRefitAuc - 1)),
        ("AUC - training model on train", float(trainAuc)),
        ("AUC - training model on valid", float(holdoutAuc)),
    ],
    ["STATISTIC", "VALUE"]
)

print("\n" + "=" * 60)
print("Model Association Statistics (equivalent to PROC PRINT)")
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
print("Predicted Probability Distribution by Actual Outcome (PROC MEANS)")
print("=" * 60)
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("MEAN"),
    stddev("pred_prob").alias("STD"),
    spark_min("pred_prob").alias("MIN"),
    percentile_approx("pred_prob", 0.25).alias("P25"),
    percentile_approx("pred_prob", 0.50).alias("MEDIAN"),
    percentile_approx("pred_prob", 0.75).alias("P75"),
    spark_max("pred_prob").alias("MAX"),
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
