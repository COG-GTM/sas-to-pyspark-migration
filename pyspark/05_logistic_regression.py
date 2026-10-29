"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - Logistic regression with stepwise selection
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

import math

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, initcap, count, mean, stddev, min as spark_min,
    max as spark_max, sum as spark_sum, percentile_approx, ntile
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
# Step 0: Load data and rebuild the upstream work.home_equity_risk table
# SAS equivalent:
#   proc import datafile="/data/home_equity.csv"
#       dbms=csv out=work.home_equity replace;   (01_data_loading.sas)
#   data work.home_equity_final; ...             (02_data_cleaning.sas)
#   data work.home_equity_risk;  ...             (04_risk_segmentation.sas)
#
# Note: SAS chains work.* datasets between programs. This script is
# self-contained, so the cleaning and risk-segmentation steps that
# produce work.home_equity_risk are reproduced inline here.
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

# 02_data_cleaning.sas, Step 1: derived columns (LTV, LOAN_OUTCOME, CITY)
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   if BAD = 0 then LOAN_OUTCOME = 'Paid'; else if BAD = 1 then ... 'Default';
#   CITY = propcase(CITY);
dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

# 02_data_cleaning.sas, Step 2: missing-value flags (ARRAY + DO loop)
#   array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
#   do i = 1 to 8; if num_vars{i} = . then num_flags{i} = 1; else ... = 0; end;
for varName in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
    dfClean = dfClean.withColumn(
        f"{varName}_MISS",
        when(col(varName).isNull(), lit(1)).otherwise(lit(0))
    )

# 02_data_cleaning.sas, Steps 3 and 5: drop missing critical fields and outliers
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
dfFinal = dfClean \
    .filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()) \
    .filter((col("LTV") > 0) & (col("LTV") < 5)) \
    .filter(col("LOAN") > 0) \
    .filter(col("VALUE") > 0)

# 04_risk_segmentation.sas, Step 2: risk categories, RISK_SCORE, RISK_SEGMENT
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

dfRisk = dfFinal \
    .withColumn(
        "LTV_RISK_CAT",
        when(col("LTV").isNull(), lit(None))
        .when(col("LTV") < 0.60, lit("Low"))
        .when(col("LTV") < 0.80, lit("Medium"))
        .otherwise(lit("High"))
    ) \
    .withColumn(
        "DTI_RISK_CAT",
        when(col("DEBTINC").isNull(), lit(None))
        .when(col("DEBTINC") < 30, lit("Low"))
        .when(col("DEBTINC") < 40, lit("Medium"))
        .when(col("DEBTINC") < 50, lit("High"))
        .otherwise(lit("Very High"))
    ) \
    .withColumn(
        "DELINQ_RISK_CAT",
        when(col("DELINQ").isNull(), lit(None))
        .when(col("DELINQ") == 0, lit("None"))
        .when(col("DELINQ") == 1, lit("Low"))
        .when(col("DELINQ") <= 3, lit("Medium"))
        .otherwise(lit("High"))
    ) \
    .withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore) \
    .withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk"))
    )

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - keep complete cases for key predictors
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
modelData = dfRisk.filter(
    col("LOAN").isNotNull() &
    col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() &
    col("CLAGE").isNotNull()
).withColumn("label", col("BAD").cast("double"))

print("=" * 60)
print("Modeling Dataset (work.model_data)")
print("=" * 60)
print(f"Rows in work.home_equity_risk: {dfRisk.count()}")
print(f"Rows in work.model_data:       {modelData.count()}")

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
#
# Note: randomSplit draws each row independently, so the split is
# approximately (not exactly) 70/30 and differs from the SAS sample.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([0.7, 0.3], seed=42)
train = train.cache()
valid = valid.cache()

print(f"Training rows:   {train.count()}")
print(f"Validation rows: {valid.count()}")

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
# Notes:
#   - descending: model P(BAD=1); Spark ML uses label 1.0 as the positive
#     class by default.
#   - CLASS ... / param=ref: StringIndexer with the reference level placed
#     last, then OneHotEncoder(dropLast=True) drops it (reference coding).
#   - PROC LOGISTIC drops rows with a missing value in any model variable;
#     Spark ML requires complete rows, so the same listwise deletion is
#     applied explicitly.
#   - selection=stepwise has no Spark ML equivalent. It is implemented
#     below with likelihood-ratio chi-square tests (SAS uses score tests
#     for entry and Wald tests for removal), fitting each candidate model
#     with GeneralizedLinearRegression(family="binomial", link="logit").
# ------------------------------------------------------------------
numericEffects = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
classEffects = {"JOB": "Other", "REASON": "HomeImp"}
allEffects = numericEffects + list(classEffects.keys())
slEntry = 0.05
slStay = 0.05


def completeCases(data, effects):
    """Listwise deletion on the given model effects."""
    condition = lit(True)
    for effect in effects:
        condition = condition & col(effect).isNotNull()
    return data.filter(condition)


def referenceLabels(data, colName, refLevel):
    """Distinct levels sorted, with the reference level last."""
    levels = sorted(r[0] for r in data.select(colName).distinct().collect() if r[0] is not None)
    return [lvl for lvl in levels if lvl != refLevel] + [refLevel]


classLabels = {
    name: referenceLabels(modelData, name, ref) for name, ref in classEffects.items()
}
classIndexers = [
    StringIndexerModel.from_labels(labels, inputCol=name, outputCol=f"{name}_IDX")
    for name, labels in classLabels.items()
]
classEncoder = OneHotEncoder(
    inputCols=[f"{name}_IDX" for name in classEffects],
    outputCols=[f"{name}_VEC" for name in classEffects],
    dropLast=True
)
featureCol = {e: (f"{e}_VEC" if e in classEffects else e) for e in allEffects}
effectDf = {e: (len(classLabels[e]) - 1 if e in classEffects else 1) for e in allEffects}


def featureNames(effects):
    """Parameter names in VectorAssembler order (e.g. JOB_Mgr for a JOB dummy)."""
    names = []
    for effect in effects:
        if effect in classEffects:
            names += [f"{effect}_{lvl}" for lvl in classLabels[effect][:-1]]
        else:
            names.append(effect)
    return names


def chiSquareSf(x, dof):
    """Upper-tail p-value of a chi-square statistic with integer dof."""
    if x <= 0:
        return 1.0
    half = x / 2.0
    if dof % 2 == 0:
        terms = range(dof // 2)
        total = sum(math.exp(i * math.log(half) - half - math.lgamma(i + 1)) for i in terms)
    else:
        total = math.erfc(math.sqrt(half))
        for j in range(1, (dof - 1) // 2 + 1):
            total += math.exp((j - 0.5) * math.log(half) - half - math.lgamma(j + 0.5))
    return min(max(total, 0.0), 1.0)


trainComplete = completeCases(train, allEffects)
encoder = Pipeline(stages=classIndexers + [classEncoder]).fit(trainComplete)
trainEncoded = encoder.transform(trainComplete).cache()
print(f"Training rows used (complete cases on all model variables): {trainEncoded.count()}")


def fitGlm(effects):
    """Fit a binomial/logit GLM on the encoded training data."""
    assembled = VectorAssembler(
        inputCols=[featureCol[e] for e in effects], outputCol="glm_features"
    ).transform(trainEncoded)
    glm = GeneralizedLinearRegression(
        family="binomial", link="logit", featuresCol="glm_features",
        labelCol="label", maxIter=100
    )
    return glm.fit(assembled)


devianceCache = {}


def deviance(effects):
    """-2 log-likelihood for a model with the given effects."""
    key = tuple(sorted(effects))
    if key not in devianceCache:
        if not effects:
            devianceCache[key] = fitGlm([allEffects[0]]).summary.nullDeviance
        else:
            devianceCache[key] = fitGlm(list(effects)).summary.deviance
    return devianceCache[key]


print("\n" + "=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("Stepwise Selection (slentry=0.05, slstay=0.05)")
print("=" * 60)

selected = []
lastRemoved = None
step = 0
while True:
    step += 1
    changed = False

    # Entry: candidate with the smallest likelihood-ratio p-value
    currentDev = deviance(selected)
    entryTests = []
    for effect in allEffects:
        if effect in selected:
            continue
        chiSq = currentDev - deviance(selected + [effect])
        entryTests.append((chiSquareSf(chiSq, effectDf[effect]), chiSq, effect))
    if entryTests:
        pValue, chiSq, effect = min(entryTests)
        print(f"\nStep {step}: best entry candidate {effect:8s} "
              f"LR Chi-Sq={chiSq:9.4f} DF={effectDf[effect]} Pr>ChiSq={pValue:.4g}")
        if pValue < slEntry and effect != lastRemoved:
            selected.append(effect)
            changed = True
            print(f"  Effect {effect} entered.")

    # Removal: effect with the largest likelihood-ratio p-value
    if len(selected) > 1:
        currentDev = deviance(selected)
        stayTests = []
        for effect in selected:
            reduced = [e for e in selected if e != effect]
            chiSq = deviance(reduced) - currentDev
            stayTests.append((chiSquareSf(chiSq, effectDf[effect]), chiSq, effect))
        pValue, chiSq, effect = max(stayTests)
        if pValue > slStay:
            selected.remove(effect)
            lastRemoved = effect
            changed = True
            print(f"  Effect {effect} removed (LR Chi-Sq={chiSq:.4f}, Pr>ChiSq={pValue:.4g}).")

    if not changed:
        print("  No (additional) effects met the significance levels.")
        break

print(f"\nSelected effects: {', '.join(selected)}")

# Analysis of Maximum Likelihood Estimates for the selected model
finalGlm = fitGlm(selected)
glmSummary = finalGlm.summary
paramNames = featureNames(selected) + ["Intercept"]
estimates = list(finalGlm.coefficients) + [finalGlm.intercept]
estimateRows = []
for name, est, se, pVal in zip(
    paramNames, estimates, glmSummary.coefficientStandardErrors, glmSummary.pValues
):
    waldChiSq = (est / se) ** 2 if se > 0 else float("nan")
    oddsRatio = math.exp(est) if name != "Intercept" else None
    estimateRows.append((name, float(est), float(se), float(waldChiSq), float(pVal), oddsRatio))

print("\nAnalysis of Maximum Likelihood Estimates")
spark.createDataFrame(
    estimateRows,
    "Parameter string, Estimate double, StdErr double, WaldChiSq double, "
    "ProbChiSq double, OddsRatio double"
).show(truncate=False)
print(f"-2 Log L (intercept only): {glmSummary.nullDeviance:.4f}")
print(f"-2 Log L (model):          {glmSummary.deviance:.4f}")
print(f"AIC:                       {glmSummary.aic:.4f}")


def logitPipeline(effects):
    """StringIndexer/OneHotEncoder (CLASS) + VectorAssembler (MODEL) + LogisticRegression."""
    usedClasses = [e for e in effects if e in classEffects]
    stages = [idx for idx in classIndexers if idx.getInputCol() in usedClasses]
    if usedClasses:
        stages.append(OneHotEncoder(
            inputCols=[f"{e}_IDX" for e in usedClasses],
            outputCols=[f"{e}_VEC" for e in usedClasses],
            dropLast=True
        ))
    stages += [
        VectorAssembler(inputCols=[featureCol[e] for e in effects], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=0.0),
    ]
    return Pipeline(stages=stages)


# Final model as an ML Pipeline (equivalent to store work.logit_model)
logitModel = logitPipeline(selected).fit(trainComplete)

# output out=work.train_scored predicted=pred_prob;
trainScored = logitModel.transform(completeCases(train, selected)) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# lackfit: Hosmer-Lemeshow goodness-of-fit test (10 groups by pred_prob)
hlGroups = trainScored \
    .withColumn("HL_GROUP", ntile(10).over(Window.orderBy("pred_prob"))) \
    .groupBy("HL_GROUP") \
    .agg(
        count(lit(1)).alias("N"),
        spark_sum("label").alias("OBSERVED"),
        spark_sum("pred_prob").alias("EXPECTED")
    ) \
    .orderBy("HL_GROUP")
hlRows = hlGroups.collect()
hlChiSq = sum(
    (r["OBSERVED"] - r["EXPECTED"]) ** 2 / (r["EXPECTED"] * (1 - r["EXPECTED"] / r["N"]))
    for r in hlRows
)
hlDf = len(hlRows) - 2
print("\nPartition for the Hosmer and Lemeshow Test")
hlGroups.show(truncate=False)
print(f"Hosmer and Lemeshow Goodness-of-Fit Test: Chi-Square={hlChiSq:.4f} "
      f"DF={hlDf} Pr>ChiSq={chiSquareSf(hlChiSq, hlDf):.4f}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
#
# Note: PROC PLM leaves pred_prob missing for rows with a missing
# selected predictor; Spark ML cannot score those rows, so they are
# excluded and reported.
# ------------------------------------------------------------------
validScoreable = completeCases(valid, selected)
print("\n" + "=" * 60)
print("Scoring Validation Set (work.valid_scored)")
print("=" * 60)
print(f"Validation rows not scored (missing selected predictor): "
      f"{valid.count() - validScoreable.count()}")

validScored = logitModel.transform(validScoreable) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
).cache()

print("\n" + "=" * 60)
print("Confusion Matrix - Validation Set")
print("=" * 60)
validScored.groupBy("BAD") \
    .pivot("PREDICTED_BAD", [0, 1]) \
    .count() \
    .na.fill(0) \
    .orderBy("BAD") \
    .withColumnRenamed("0", "PREDICTED_BAD=0") \
    .withColumnRenamed("1", "PREDICTED_BAD=1") \
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
# Note: the full (no selection) model is fit on the validation data, as
# in the SAS program. The Association table (concordance, Somers' D,
# Gamma, Tau-a, c) is computed from all event/non-event pairs; SAS bins
# probabilities to 0.002 for this table, so values can differ slightly.
# ------------------------------------------------------------------
validComplete = completeCases(valid, allEffects)
validFullScored = logitPipeline(allEffects).fit(validComplete).transform(validComplete) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# roc: area under the ROC curve
auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(validFullScored)

# Concordant / discordant / tied pairs from event and non-event counts per probability
probCounts = validFullScored.groupBy("pred_prob").agg(
    spark_sum(when(col("BAD") == 1, 1).otherwise(0)).alias("N_EVENT"),
    spark_sum(when(col("BAD") == 0, 1).otherwise(0)).alias("N_NONEVENT")
).withColumn(
    "NONEVENT_BELOW",
    spark_sum("N_NONEVENT").over(
        Window.orderBy("pred_prob").rowsBetween(Window.unboundedPreceding, -1)
    )
).na.fill(0, ["NONEVENT_BELOW"])
pairTotals = probCounts.agg(
    spark_sum(col("N_EVENT") * col("NONEVENT_BELOW")).alias("CONCORDANT"),
    spark_sum(col("N_EVENT") * col("N_NONEVENT")).alias("TIED"),
    spark_sum("N_EVENT").alias("EVENTS"),
    spark_sum("N_NONEVENT").alias("NONEVENTS")
).collect()[0]

nObs = pairTotals["EVENTS"] + pairTotals["NONEVENTS"]
pairs = pairTotals["EVENTS"] * pairTotals["NONEVENTS"]
concordant = pairTotals["CONCORDANT"]
tied = pairTotals["TIED"]
discordant = pairs - concordant - tied

associationStats = spark.createDataFrame(
    [
        ("Percent Concordant", 100.0 * concordant / pairs),
        ("Percent Discordant", 100.0 * discordant / pairs),
        ("Percent Tied", 100.0 * tied / pairs),
        ("Pairs", float(pairs)),
        ("Somers' D", (concordant - discordant) / pairs),
        ("Gamma", (concordant - discordant) / (concordant + discordant)),
        ("Tau-a", (concordant - discordant) / (0.5 * nObs * (nObs - 1))),
        ("c", (concordant + 0.5 * tied) / pairs),
    ],
    "Statistic string, Value double"
)

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Association Statistics (Validation Set, Full Model)")
print("=" * 60)
print(f"Validation rows used: {nObs}")
associationStats.show(truncate=False)
print(f"Area under ROC curve (BinaryClassificationEvaluator): {auc:.4f}")

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
    percentile_approx("pred_prob", 0.25).alias("P25"),
    percentile_approx("pred_prob", 0.50).alias("Median"),
    percentile_approx("pred_prob", 0.75).alias("P75"),
    spark_max("pred_prob").alias("Maximum")
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
