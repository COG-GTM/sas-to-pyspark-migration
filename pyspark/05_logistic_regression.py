"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - Logistic regression on the risk-modeling dataset (PROC LOGISTIC)
  - Model evaluation (concordance, AUC, Hosmer-Lemeshow style lack of fit)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas

Note: The SAS program starts from work.home_equity_risk, which is produced
by 02_data_cleaning.sas / 04_risk_segmentation.sas. This script is
self-contained: it loads data/home_equity.csv and re-derives the
prerequisite columns and row filters (LTV, LOAN_OUTCOME, cleaning filters)
in Step 0 so it does not depend on the other PySpark scripts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, mean, stddev, min as spark_min, max as spark_max,
    percentile_approx, ntile, sum as spark_sum, initcap, round as spark_round
)
from pyspark.sql.window import Window
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
# Step 0: Load CSV data and re-derive the prerequisite dataset
# SAS equivalent (from 02_data_cleaning.sas, feeding work.home_equity_risk):
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered;
#       set work.home_equity_imputed;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
#       set work.home_equity_filtered;
#       if LTV > 0 and LTV < 5;
#       if LOAN > 0;
#       if VALUE > 0;
#   run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)
# Strip a possible UTF-8 BOM from the first column name
df = df.toDF(*[c.lstrip("\ufeff") for c in df.columns])

dfClean = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    ).otherwise(lit(None))
).withColumn(
    "LOAN_OUTCOME",
    when(col("BAD") == 0, lit("Paid"))
    .when(col("BAD") == 1, lit("Default"))
    .otherwise(lit(""))
).withColumn("CITY", initcap(col("CITY")))

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)

# SAS subsetting IF on a missing LTV evaluates false, so nulls are dropped
dfRisk = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)

print("=" * 60)
print("Prerequisite dataset (equivalent to work.home_equity_risk)")
print("=" * 60)
print(f"Rows available for modeling pipeline: {dfRisk.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - remove records with excessive missing
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
dfModelData = dfRisk.filter(
    col("LOAN").isNotNull() &
    col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() &
    col("CLAGE").isNotNull()
)

# PROC LOGISTIC silently drops any observation with a missing value in
# any model effect (listwise deletion). Spark ML's VectorAssembler and
# StringIndexer cannot accept nulls, so the same deletion is applied
# here explicitly for the remaining predictors DEROG, NINQ, JOB, REASON.
dfModelData = dfModelData.filter(
    col("DEROG").isNotNull() &
    col("NINQ").isNotNull() &
    col("JOB").isNotNull() &
    col("REASON").isNotNull()
).withColumn("label", col("BAD").cast("double"))

print("\n" + "=" * 60)
print("Step 1: Modeling dataset (complete cases)")
print("=" * 60)
print(f"Rows in work.model_data equivalent: {dfModelData.count()}")

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
# Note: randomSplit is a Bernoulli split, so the 70/30 proportion is
# approximate rather than the exact sample size PROC SURVEYSELECT draws.
# ------------------------------------------------------------------
dfTrain, dfValid = dfModelData.randomSplit([0.7, 0.3], seed=42)
dfTrain = dfTrain.cache()
dfValid = dfValid.cache()

print("\n" + "=" * 60)
print("Step 2: Train / Validation split (equivalent to PROC SURVEYSELECT)")
print("=" * 60)
print(f"Training rows:   {dfTrain.count()}")
print(f"Validation rows: {dfValid.count()}")

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
#   - DESCENDING: model P(BAD=1). Spark ML models P(label=1) by default.
#   - CLASS ... / param=ref: StringIndexerModel.from_labels places the
#     reference level last so OneHotEncoder (dropLast=True) drops it,
#     giving the same reference-cell coding as SAS.
#   - selection=stepwise: Spark MLlib has no p-value based stepwise
#     selection. The full model is fitted and its coefficients are
#     printed (the "details" output). This is documented as a known
#     difference rather than emulated.
#   - lackfit: a Hosmer-Lemeshow style decile table of observed vs
#     expected events is printed on the training set.
#   - store work.logit_model: the fitted PipelineModel object plays the
#     role of the item store and is reused in Step 4 for scoring.
# ------------------------------------------------------------------
numericPredictors = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                     "DELINQ", "DEROG", "CLAGE", "NINQ"]
classReferenceLevels = {"JOB": "Other", "REASON": "HomeImp"}


def buildClassLevels(dfSource, colName, refLevel):
    """Return the distinct levels of a CLASS variable with the reference level last."""
    levels = sorted(
        row[colName]
        for row in dfSource.select(colName).distinct().collect()
        if row[colName] != refLevel
    )
    return levels + [refLevel]


def buildPipelineStages(dfSource):
    """Build StringIndexer / OneHotEncoder / VectorAssembler / LogisticRegression stages."""
    stages = []
    encodedCols = []
    for classCol, refLevel in classReferenceLevels.items():
        levels = buildClassLevels(dfSource, classCol, refLevel)
        indexer = StringIndexerModel.from_labels(
            levels, inputCol=classCol, outputCol=f"{classCol}_IDX",
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCol=f"{classCol}_IDX", outputCol=f"{classCol}_VEC", dropLast=True
        )
        stages.extend([indexer, encoder])
        encodedCols.append(f"{classCol}_VEC")

    assembler = VectorAssembler(
        inputCols=numericPredictors + encodedCols,
        outputCol="features"
    )
    lr = LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=50, regParam=0.01
    )
    stages.extend([assembler, lr])
    return stages


def buildFeatureNames(dfSource):
    """Feature names in VectorAssembler order (numeric, then non-reference class levels)."""
    names = list(numericPredictors)
    for classCol, refLevel in classReferenceLevels.items():
        for level in buildClassLevels(dfSource, classCol, refLevel)[:-1]:
            names.append(f"{classCol}={level}")
    return names


def printCoefficients(lrModel, featureNames, title):
    """Print intercept and coefficients, similar to PROC LOGISTIC parameter estimates."""
    print("\n" + "-" * 60)
    print(title)
    print("-" * 60)
    print(f"  {'Parameter':20s} {'Estimate':>12s}")
    print(f"  {'Intercept':20s} {lrModel.intercept:12.6f}")
    for name, coef in zip(featureNames, lrModel.coefficients):
        print(f"  {name:20s} {coef:12.6f}")


print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)

pipeline = Pipeline(stages=buildPipelineStages(dfTrain))
logitModel = pipeline.fit(dfTrain)
trainFeatureNames = buildFeatureNames(dfTrain)
lrTrainModel = logitModel.stages[-1]
printCoefficients(lrTrainModel, trainFeatureNames,
                  "Parameter Estimates (training model, reference-cell coding)")

# output out=work.train_scored predicted=pred_prob
dfTrainScored = logitModel.transform(dfTrain).withColumn(
    "pred_prob", vector_to_array(col("probability"))[1]
)

# lackfit: Hosmer-Lemeshow style partition of training data into deciles
print("\n" + "-" * 60)
print("Partition for the Hosmer and Lemeshow Test (training set, lackfit)")
print("-" * 60)
dfHl = dfTrainScored.withColumn(
    "Group", ntile(10).over(Window.orderBy(col("pred_prob")))
).groupBy("Group").agg(
    count("*").alias("Total"),
    spark_sum(col("label")).alias("Observed_Event"),
    spark_round(spark_sum(col("pred_prob")), 2).alias("Expected_Event"),
    spark_sum(lit(1) - col("label")).alias("Observed_NonEvent"),
    spark_round(spark_sum(lit(1) - col("pred_prob")), 2).alias("Expected_NonEvent")
).orderBy("Group")
dfHl.show(10, truncate=False)

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# Note: / ilink returns the probability scale, i.e. probability[1].
# ------------------------------------------------------------------
dfValidScored = logitModel.transform(dfValid).withColumn(
    "pred_prob", vector_to_array(col("probability"))[1]
)

print("\n" + "=" * 60)
print("Step 4: Validation set scored (equivalent to PROC PLM SCORE / ilink)")
print("=" * 60)
dfValidScored.select("BAD", "LOAN", "MORTDUE", "VALUE", "DEBTINC",
                     "JOB", "REASON", "pred_prob").show(10, truncate=False)

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
dfValidScored = dfValidScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
)

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("=" * 60)
dfConfusion = dfValidScored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]) \
    .count().na.fill(0).orderBy("BAD")
dfConfusion = dfConfusion.withColumnRenamed("0", "PREDICTED_BAD=0") \
    .withColumnRenamed("1", "PREDICTED_BAD=1")
dfConfusion.show(truncate=False)

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
# Note: SAS refits a full model on the validation set and reports the
# association statistics (Percent Concordant / Discordant / Tied, Somers' D,
# Gamma, Tau-a, c) of that fit. The same is done here, and additionally
# the AUC of the training model applied to the validation set is reported.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Performance - Concordance and AUC")
print("=" * 60)

evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
validAuc = evaluator.evaluate(dfValidScored)
print(f"AUC of training model scored on validation set: {validAuc:.4f}")

validPipeline = Pipeline(stages=buildPipelineStages(dfValid))
validModel = validPipeline.fit(dfValid)
printCoefficients(validModel.stages[-1], buildFeatureNames(dfValid),
                  "Parameter Estimates (validation refit, as in SAS Step 6)")

dfValidRefit = validModel.transform(dfValid).withColumn(
    "pred_prob", vector_to_array(col("probability"))[1]
)
refitAuc = evaluator.evaluate(dfValidRefit)

# Association statistics: compare every (event, non-event) pair
dfEvents = dfValidRefit.filter(col("label") == 1.0) \
    .select(col("pred_prob").alias("p_event"))
dfNonEvents = dfValidRefit.filter(col("label") == 0.0) \
    .select(col("pred_prob").alias("p_nonevent"))
pairStats = dfEvents.crossJoin(dfNonEvents).agg(
    spark_sum(when(col("p_event") > col("p_nonevent"), 1).otherwise(0)).alias("concordant"),
    spark_sum(when(col("p_event") < col("p_nonevent"), 1).otherwise(0)).alias("discordant"),
    spark_sum(when(col("p_event") == col("p_nonevent"), 1).otherwise(0)).alias("tied"),
).collect()[0]

nConcordant = pairStats["concordant"]
nDiscordant = pairStats["discordant"]
nTied = pairStats["tied"]
nPairs = nConcordant + nDiscordant + nTied
nValid = dfValid.count()

somersD = (nConcordant - nDiscordant) / nPairs if nPairs else 0.0
gamma = (nConcordant - nDiscordant) / (nConcordant + nDiscordant) \
    if (nConcordant + nDiscordant) else 0.0
tauA = (nConcordant - nDiscordant) / (nValid * (nValid - 1) / 2) if nValid > 1 else 0.0
cStat = (nConcordant + 0.5 * nTied) / nPairs if nPairs else 0.0

associationStats = spark.createDataFrame(
    [
        ("Percent Concordant", round(100.0 * nConcordant / nPairs, 1) if nPairs else 0.0,
         "Somers' D", round(somersD, 3)),
        ("Percent Discordant", round(100.0 * nDiscordant / nPairs, 1) if nPairs else 0.0,
         "Gamma", round(gamma, 3)),
        ("Percent Tied", round(100.0 * nTied / nPairs, 1) if nPairs else 0.0,
         "Tau-a", round(tauA, 3)),
        ("Pairs", float(nPairs), "c", round(cStat, 3)),
    ],
    ["Label1", "nValue1", "Label2", "nValue2"]
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
print(f"Validation refit AUC (areaUnderROC, should equal c): {refitAuc:.4f}")

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
dfValidScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    spark_round(mean("pred_prob"), 4).alias("Mean"),
    spark_round(stddev("pred_prob"), 4).alias("Std_Dev"),
    spark_round(spark_min("pred_prob"), 4).alias("Minimum"),
    spark_round(percentile_approx("pred_prob", 0.25), 4).alias("P25"),
    spark_round(percentile_approx("pred_prob", 0.5), 4).alias("Median"),
    spark_round(percentile_approx("pred_prob", 0.75), 4).alias("P75"),
    spark_round(spark_max("pred_prob"), 4).alias("Maximum"),
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
