"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Stepwise effect selection (likelihood-ratio tests)
         - Model evaluation (concordance, AUC)
         - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas

Self-contained: the SAS program reads work.home_equity_risk, which is built by
02_data_cleaning.sas and 04_risk_segmentation.sas. This script loads
data/home_equity.csv and reproduces the 02 cleaning inline. The risk-segment
columns added by 04 are not used by the model, so they are not rebuilt here.
"""

import math

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, initcap, rand, row_number, expr, count, mean, stddev,
    min as spark_min, max as spark_max, sum as spark_sum
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexerModel, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Step 0: Load CSV data and reproduce 02_data_cleaning inline
# SAS equivalent (02_data_cleaning.sas):
#   data work.home_equity_clean;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_imputed;  /* *_MISS flags via array/do loop */
#   data work.home_equity_filtered;
#       if LOAN ne . and VALUE ne . and BAD ne .;
#   run;
#   data work.home_equity_final;
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

numVars = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
for varName in numVars:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS", when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

homeEquityFinal = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)

print("=" * 60)
print("Upstream cleaning (02_data_cleaning) reproduced inline")
print("=" * 60)
print(f"Number of rows: {homeEquityFinal.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - complete cases only
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# Note: PROC LOGISTIC also silently drops any row with a missing MODEL or
# CLASS variable (DEROG, NINQ, JOB, REASON). Spark ML does not, so those
# columns are filtered here too, giving the same rows SAS actually fits on.
# ------------------------------------------------------------------
numericEffects = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
classEffects = ["JOB", "REASON"]
allEffects = numericEffects + classEffects

modelData = homeEquityFinal
for varName in allEffects:
    modelData = modelData.filter(col(varName).isNotNull())
modelData = modelData.withColumn("label", col("BAD").cast("double"))

print("\n" + "=" * 60)
print("Modeling Dataset (complete cases)")
print("=" * 60)
print(f"Number of rows: {modelData.count()}")

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
# Simple random sampling without replacement: shuffle with a seeded rand()
# and flag exactly ceil(0.7 * N) rows as SELECTED. Spark's random stream
# differs from SAS's, so the chosen rows differ but the design is the same.
# ------------------------------------------------------------------
totalRows = modelData.count()
sampleSize = math.ceil(0.7 * totalRows)
modelSplit = modelData.withColumn(
    "SELECTED",
    when(
        row_number().over(Window.orderBy(rand(seed=42))) <= sampleSize, lit(1)
    ).otherwise(lit(0))
).cache()

train = modelSplit.filter(col("SELECTED") == 1).cache()
valid = modelSplit.filter(col("SELECTED") == 0).cache()

print("\n" + "=" * 60)
print("Training / Validation Split (equivalent to PROC SURVEYSELECT)")
print("=" * 60)
print(f"Training rows:   {train.count()}")
print(f"Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# CLASS statement encoding
# SAS equivalent:
#   class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#
# param=ref is reference-cell (dummy) coding. Putting the reference level
# last in the StringIndexer labels and using OneHotEncoder(dropLast=True)
# drops exactly that level, so it becomes the baseline.
# ------------------------------------------------------------------
classRefs = {"JOB": "Other", "REASON": "HomeImp"}
encodingStages = []
for varName, refLevel in classRefs.items():
    levels = sorted(
        row[varName] for row in modelData.select(varName).distinct().collect()
        if row[varName] != refLevel
    )
    encodingStages.append(
        StringIndexerModel.from_labels(
            levels + [refLevel], inputCol=varName, outputCol=f"{varName}_IDX"
        )
    )
    encodingStages.append(
        OneHotEncoder(inputCol=f"{varName}_IDX", outputCol=f"{varName}_VEC", dropLast=True)
    )

effectColumn = {name: name for name in numericEffects}
effectColumn.update({name: f"{name}_VEC" for name in classEffects})
effectDf = {name: 1 for name in numericEffects}
effectDf.update({
    name: modelData.select(name).distinct().count() - 1 for name in classEffects
})

encoder = Pipeline(stages=encodingStages).fit(modelData)
trainEncoded = encoder.transform(train).cache()

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
# Spark ML has no stepwise option, so it is reproduced with a loop over
# effects. Each step adds the candidate with the smallest entry p-value
# (if < SLENTRY), then removes the effect with the largest stay p-value
# (if > SLSTAY). SAS uses a score test for entry and a Wald test for stay;
# here both use the likelihood-ratio test (deviance difference from a
# binomial/logit GeneralizedLinearRegression), which is asymptotically
# equivalent and tests multi-level CLASS effects as a whole.
# The LACKFIT (Hosmer-Lemeshow) test has no Spark equivalent and is omitted.
# ------------------------------------------------------------------
slEntry = 0.05
slStay = 0.05


def chiSquareSurvival(statistic, degreesOfFreedom):
    """P(X > statistic) for a chi-square with integer degrees of freedom."""
    if statistic <= 0:
        return 1.0
    half = statistic / 2.0
    if degreesOfFreedom % 2 == 0:
        term, total = 1.0, 1.0
        for i in range(1, degreesOfFreedom // 2):
            term *= half / i
            total += term
        return math.exp(-half) * total
    root = math.sqrt(statistic)
    total = math.erfc(root / math.sqrt(2.0))
    term = root * math.sqrt(2.0 / math.pi) * math.exp(-half)
    for i in range(1, (degreesOfFreedom + 1) // 2):
        total += term
        term *= statistic / (2 * i + 1)
    return total


# Intercept-only deviance: -2 * log-likelihood at p = mean(BAD)
trainRows = trainEncoded.count()
trainEvents = trainEncoded.filter(col("label") == 1).count()
eventRate = trainEvents / trainRows
nullDeviance = -2.0 * (
    trainEvents * math.log(eventRate)
    + (trainRows - trainEvents) * math.log(1.0 - eventRate)
)


def fitDeviance(effects):
    """Residual deviance of a logit model on the given effects (intercept only if empty)."""
    if not effects:
        return nullDeviance
    assembler = VectorAssembler(
        inputCols=[effectColumn[name] for name in effects], outputCol="features"
    )
    glm = GeneralizedLinearRegression(
        family="binomial", link="logit", labelCol="label",
        featuresCol="features", maxIter=50, regParam=0.0
    )
    return glm.fit(assembler.transform(trainEncoded)).summary.deviance


print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("Stepwise selection (SLENTRY=0.05, SLSTAY=0.05)")
print("=" * 60)

selected = []
currentDeviance = fitDeviance(selected)
stepNumber = 0
history = set()
while True:
    stepNumber += 1
    changed = False

    candidates = [name for name in allEffects if name not in selected]
    entryResults = []
    for name in candidates:
        chiSq = currentDeviance - fitDeviance(selected + [name])
        entryResults.append((chiSquareSurvival(chiSq, effectDf[name]), chiSq, name))
    if entryResults:
        pValue, chiSq, name = min(entryResults)
        if pValue < slEntry and tuple(sorted(selected + [name])) not in history:
            selected.append(name)
            currentDeviance = fitDeviance(selected)
            history.add(tuple(sorted(selected)))
            changed = True
            print(f"  Step {stepNumber}: entered {name:8s} "
                  f"(LR chi-sq={chiSq:9.4f}, DF={effectDf[name]}, p={pValue:.4g})")

    stayResults = []
    for name in selected:
        reduced = [other for other in selected if other != name]
        chiSq = fitDeviance(reduced) - currentDeviance
        stayResults.append((chiSquareSurvival(chiSq, effectDf[name]), chiSq, name))
    if stayResults:
        pValue, chiSq, name = max(stayResults)
        if pValue > slStay:
            selected.remove(name)
            currentDeviance = fitDeviance(selected)
            history.add(tuple(sorted(selected)))
            changed = True
            print(f"  Step {stepNumber}: removed {name:8s} "
                  f"(LR chi-sq={chiSq:9.4f}, DF={effectDf[name]}, p={pValue:.4g})")

    if not changed:
        break

selectedEffects = [name for name in allEffects if name in selected]
print(f"\nEffects selected: {', '.join(selectedEffects)}")

# Final model on the selected effects (unpenalized, like PROC LOGISTIC)
finalAssembler = VectorAssembler(
    inputCols=[effectColumn[name] for name in selectedEffects], outputCol="features"
)
logisticRegression = LogisticRegression(
    featuresCol="features", labelCol="label", maxIter=100, regParam=0.0
)
logitModel = Pipeline(
    stages=encodingStages + [finalAssembler, logisticRegression]
).fit(train)

fittedLr = logitModel.stages[-1]
featureNames = []
for name in selectedEffects:
    if name in classRefs:
        indexer = next(s for s in encodingStages
                       if isinstance(s, StringIndexerModel) and s.getInputCol() == name)
        featureNames += [f"{name} {level}" for level in indexer.labels[:-1]]
    else:
        featureNames.append(name)

print("\nAnalysis of Maximum Likelihood Estimates")
print(f"  {'Parameter':20s} {'Estimate':>14s}")
print(f"  {'Intercept':20s} {fittedLr.intercept:14.6f}")
for featureName, coefficient in zip(featureNames, fittedLr.coefficients):
    print(f"  {featureName:20s} {coefficient:14.6f}")

# output out=work.train_scored predicted=pred_prob;
trainScored = logitModel.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])
print(f"\nTraining rows scored: {trainScored.count()}")

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
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD", when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
).cache()

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set (equivalent to PROC FREQ)")
print("=" * 60)
validScored.crosstab("BAD", "PREDICTED_BAD").orderBy("BAD_PREDICTED_BAD").show()

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
# As in the SAS program, a full (all-effects) model is fitted on the
# validation set and its association statistics are reported.
# ------------------------------------------------------------------
fullAssembler = VectorAssembler(
    inputCols=[effectColumn[name] for name in allEffects], outputCol="features"
)
validModel = Pipeline(
    stages=encodingStages + [
        fullAssembler,
        LogisticRegression(featuresCol="features", labelCol="label",
                           maxIter=100, regParam=0.0)
    ]
).fit(valid)
validFullScored = validModel.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(validFullScored)

# Association of predicted probabilities and observed responses:
# compare every (event, non-event) pair.
events = validFullScored.filter(col("BAD") == 1).select(col("pred_prob").alias("p1"))
nonEvents = validFullScored.filter(col("BAD") == 0).select(col("pred_prob").alias("p0"))
pairCounts = events.crossJoin(nonEvents).agg(
    count(lit(1)).alias("pairs"),
    spark_sum(when(col("p1") > col("p0"), 1).otherwise(0)).alias("concordant"),
    spark_sum(when(col("p1") < col("p0"), 1).otherwise(0)).alias("discordant"),
).collect()[0]

pairs = pairCounts["pairs"]
concordant = pairCounts["concordant"]
discordant = pairCounts["discordant"]
tied = pairs - concordant - discordant
validRows = valid.count()

associationStats = spark.createDataFrame(
    [
        ("Percent Concordant", round(100.0 * concordant / pairs, 1)),
        ("Percent Discordant", round(100.0 * discordant / pairs, 1)),
        ("Percent Tied", round(100.0 * tied / pairs, 1)),
        ("Pairs", float(pairs)),
        ("Somers' D", round((concordant - discordant) / pairs, 3)),
        ("Gamma", round((concordant - discordant) / (concordant + discordant), 3)),
        ("Tau-a", round((concordant - discordant) / (0.5 * validRows * (validRows - 1)), 3)),
        ("c", round((concordant + 0.5 * tied) / pairs, 3)),
        ("AUC (BinaryClassificationEvaluator)", round(auc, 4)),
    ],
    ["Statistic", "Value"],
)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Association Statistics (Concordance and AUC)")
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
print("=" * 60)
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("Std_Dev"),
    spark_min("pred_prob").alias("Minimum"),
    expr("percentile(pred_prob, 0.25)").alias("P25"),
    expr("percentile(pred_prob, 0.5)").alias("Median"),
    expr("percentile(pred_prob, 0.75)").alias("P75"),
    spark_max("pred_prob").alias("Maximum"),
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
