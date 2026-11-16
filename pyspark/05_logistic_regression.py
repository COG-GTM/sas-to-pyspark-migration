"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

import math
import os

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
spark.sparkContext.setLogLevel("WARN")

projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

# SAS: model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON
numericEffects = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                  "DELINQ", "DEROG", "CLAGE", "NINQ"]
# SAS: class JOB(ref='Other') REASON(ref='HomeImp') / param=ref
classEffects = {"JOB": "Other", "REASON": "HomeImp"}
allEffects = numericEffects + list(classEffects)

SLENTRY = 0.05
SLSTAY = 0.05
SEED = 42
TRAIN_FRACTION = 0.7
CUTOFF = 0.5


def chiSquareSf(x, df):
    """Upper-tail chi-square probability (SAS: 1 - PROBCHI(x, df))."""
    if x <= 0:
        return 1.0
    a, z = df / 2.0, x / 2.0
    logPrefix = -z + a * math.log(z) - math.lgamma(a)
    if z < a + 1:
        term = total = 1.0 / a
        n = a
        for _ in range(500):
            n += 1
            term *= z / n
            total += term
            if abs(term) < abs(total) * 1e-15:
                break
        return max(0.0, 1.0 - total * math.exp(logPrefix))
    b = z + 1 - a
    c, d = 1e300, 1.0 / b
    h = d
    for i in range(1, 500):
        an = -i * (i - a)
        b += 2
        d = an * d + b
        d = 1e-300 if abs(d) < 1e-300 else d
        c = b + an / c
        c = 1e-300 if abs(c) < 1e-300 else c
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1) < 1e-15:
            break
    return math.exp(logPrefix) * h


# ------------------------------------------------------------------
# Upstream: rebuild work.home_equity_risk inline (self-contained script)
# SAS equivalent: sas/01_data_loading.sas -> 02_data_cleaning.sas
#   -> 04_risk_segmentation.sas
# 04 only adds risk-bucket columns (none are model inputs) and does not
# filter rows, so the modeling population equals work.home_equity_final.
# ------------------------------------------------------------------
def loadHomeEquity():
    # SAS: proc import dbms=csv -> spark.read.csv (empty fields -> null)
    return spark.read.csv(dataPath, header=True, inferSchema=True)


def cleanHomeEquity(df):
    # SAS: DATA step IF/THEN -> when().otherwise()
    df = df.withColumn(
        "LTV",
        when(col("VALUE").isNotNull() & col("MORTDUE").isNotNull() &
             (col("VALUE") > 0), col("MORTDUE") / col("VALUE"))
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default"))
    ).withColumn("CITY", initcap(col("CITY")))  # SAS: propcase() -> initcap()

    # SAS: if LOAN ne . and VALUE ne . and BAD ne .; -> filter(isNotNull)
    df = df.filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() &
                   col("BAD").isNotNull())

    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;
    # A missing LTV fails "LTV > 0" in SAS; a null comparison drops it here too.
    return df.filter((col("LTV") > 0) & (col("LTV") < 5) &
                     (col("LOAN") > 0) & (col("VALUE") > 0))


homeEquityRisk = cleanHomeEquity(loadHomeEquity())

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - complete cases for key predictors
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
modelData = homeEquityRisk.filter(
    col("LOAN").isNotNull() & col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() & col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() & col("CLAGE").isNotNull()
).withColumn("label", col("BAD").cast("double"))  # SAS: descending -> P(BAD=1)
modelData.cache()

print("=" * 60)
print("Step 1: Modeling Dataset")
print("=" * 60)
print(f"Rows in work.home_equity_risk: {homeEquityRisk.count()}")
print(f"Rows in work.model_data:       {modelData.count()}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
#   data work.train work.valid; ... if selected = 1 then output ...
#
# Note: randomSplit is Bernoulli sampling, so the train size is ~70%
# rather than SURVEYSELECT's exact 70%; seed 42 is preserved.
# ------------------------------------------------------------------
train, valid = modelData.randomSplit([TRAIN_FRACTION, 1 - TRAIN_FRACTION], seed=SEED)

# SAS: PROC LOGISTIC silently drops rows with any missing model variable
# (listwise deletion); Spark ML would error on nulls, so drop them explicitly.
completeCase = None
for effect in allEffects:
    cond = col(effect).isNotNull()
    completeCase = cond if completeCase is None else completeCase & cond

trainComplete = train.filter(completeCase).cache()
validComplete = valid.filter(completeCase).cache()

print("\n" + "=" * 60)
print("Step 2: Training / Validation Split (seed=42, 70/30)")
print("=" * 60)
print(f"Training rows:   {train.count()}  (used in model: {trainComplete.count()})")
print(f"Validation rows: {valid.count()}  (complete cases: {validComplete.count()})")

# ------------------------------------------------------------------
# CLASS statement with reference-cell coding
# SAS equivalent: class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
# StringIndexer with the reference level indexed last + OneHotEncoder
# (dropLast=True) drops exactly the reference level -> param=ref dummies.
# ------------------------------------------------------------------
classLevels = {}
encoderStagesByEffect = {}
for effect, refLevel in classEffects.items():
    levels = sorted(
        row[effect] for row in
        modelData.filter(col(effect).isNotNull()).select(effect).distinct().collect()
    )
    labels = [lvl for lvl in levels if lvl != refLevel] + [refLevel]
    classLevels[effect] = labels[:-1]
    encoderStagesByEffect[effect] = [
        StringIndexerModel.from_labels(
            labels, inputCol=effect, outputCol=f"{effect}_IDX"),
        OneHotEncoder(
            inputCol=f"{effect}_IDX", outputCol=f"{effect}_VEC", dropLast=True),
    ]
encoderStages = [stage for stages in encoderStagesByEffect.values() for stage in stages]

effectColumn = {e: e for e in numericEffects}
effectColumn.update({e: f"{e}_VEC" for e in classEffects})
effectDf = {e: 1 for e in numericEffects}
effectDf.update({e: len(classLevels[e]) for e in classEffects})


def featureNames(effects):
    names = []
    for e in effects:
        if e in classEffects:
            names += [f"{e} {lvl}" for lvl in classLevels[e]]
        else:
            names.append(e)
    return names


encoderModel = Pipeline(stages=encoderStages).fit(modelData)
trainEncoded = encoderModel.transform(trainComplete).cache()


def fitGlm(effects):
    """Unpenalized binomial/logit GLM (IRLS) - same MLE as PROC LOGISTIC."""
    assembler = VectorAssembler(
        inputCols=[effectColumn[e] for e in effects], outputCol="features")
    glm = GeneralizedLinearRegression(
        family="binomial", link="logit", labelCol="label",
        featuresCol="features", maxIter=50)
    return glm.fit(assembler.transform(trainEncoded))


# ------------------------------------------------------------------
# Step 3: Fit logistic regression with stepwise selection
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON
#             / selection=stepwise slentry=0.05 slstay=0.05 details lackfit;
#       output out=work.train_scored predicted=pred_prob;
#       store work.logit_model;
#   run;
#
# SAS: SELECTION=STEPWISE -> explicit stepwise loop over effects.
# Spark has no p-value-driven stepwise, so each step fits a binomial GLM
# and uses likelihood-ratio chi-square tests (a CLASS effect enters/leaves
# as a whole, df = number of dummies) against SLENTRY/SLSTAY = 0.05.
# SAS uses score (entry) and Wald (removal) chi-squares; LR is the
# asymptotically equivalent test available from Spark model summaries.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 3: Logistic Regression - Stepwise Selection (details)")
print("=" * 60)

selected = []
currentDeviance = None
seenModels = {frozenset()}
for step in range(1, 2 * len(allEffects) + 1):
    changed = False

    # Forward step: enter the candidate with the smallest p-value < SLENTRY
    candidates = [e for e in allEffects if e not in selected]
    best = None
    for effect in candidates:
        summary = fitGlm(selected + [effect]).summary
        baseDeviance = currentDeviance if selected else summary.nullDeviance
        lrStat = baseDeviance - summary.deviance
        pValue = chiSquareSf(lrStat, effectDf[effect])
        print(f"  Step {step} entry test: {effect:8s} "
              f"LR ChiSq={lrStat:9.4f} DF={effectDf[effect]} Pr>ChiSq={pValue:.4g}")
        if best is None or pValue < best[1]:
            best = (effect, pValue, summary.deviance)
    if best and best[1] < SLENTRY and frozenset(selected + [best[0]]) not in seenModels:
        selected.append(best[0])
        currentDeviance = best[2]
        seenModels.add(frozenset(selected))
        changed = True
        print(f"  Step {step}: Effect {best[0]} entered (p={best[1]:.4g})")

    # Backward step: remove the effect with the largest p-value > SLSTAY
    if len(selected) > 1:
        worst = None
        for effect in selected:
            reduced = [e for e in selected if e != effect]
            reducedDeviance = fitGlm(reduced).summary.deviance
            lrStat = reducedDeviance - currentDeviance
            pValue = chiSquareSf(lrStat, effectDf[effect])
            if worst is None or pValue > worst[1]:
                worst = (effect, pValue, reducedDeviance)
        if worst[1] > SLSTAY and frozenset(s for s in selected if s != worst[0]) not in seenModels:
            selected.remove(worst[0])
            currentDeviance = worst[2]
            seenModels.add(frozenset(selected))
            changed = True
            print(f"  Step {step}: Effect {worst[0]} removed (p={worst[1]:.4g})")

    if not changed:
        break

print(f"\nSelected effects: {', '.join(selected)}")

# Maximum likelihood estimates of the selected model
finalGlm = fitGlm(selected)
finalSummary = finalGlm.summary
names = featureNames(selected) + ["Intercept"]
coefs = list(finalGlm.coefficients) + [finalGlm.intercept]
print("\nAnalysis of Maximum Likelihood Estimates")
print(f"  {'Parameter':18s} {'Estimate':>12s} {'StdErr':>10s} "
      f"{'Pr>ChiSq':>10s} {'OddsRatio':>10s}")
for name, est, se, p in zip(names, coefs,
                            finalSummary.coefficientStandardErrors,
                            finalSummary.pValues):
    oddsRatio = "" if name == "Intercept" else f"{math.exp(est):10.4f}"
    print(f"  {name:18s} {est:12.6f} {se:10.6f} {p:10.4g} {oddsRatio:>10s}")
print(f"  -2 Log L: {finalSummary.deviance:.4f}   AIC: {finalSummary.aic:.4f}")

# SAS: MODEL + STORE -> Pipeline(StringIndexer, OneHotEncoder,
#      VectorAssembler, LogisticRegression) fitted once and reused for scoring
# Only the selected CLASS effects are encoded, so scoring does not need the
# dropped ones to be non-missing (PROC PLM uses only the stored model's effects).
selectedEncoderStages = [stage for e in selected if e in classEffects
                         for stage in encoderStagesByEffect[e]]
logitPipeline = Pipeline(stages=selectedEncoderStages + [
    VectorAssembler(inputCols=[effectColumn[e] for e in selected],
                    outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label",
                       regParam=0.0, maxIter=100)
])
logitModel = logitPipeline.fit(trainComplete)


def scoreWithProbability(model, df):
    # SAS: predicted=pred_prob -> probability[1] (P(BAD=1))
    return model.transform(df).withColumn(
        "pred_prob", vector_to_array(col("probability"))[1])


# SAS: output out=work.train_scored predicted=pred_prob;
trainScored = scoreWithProbability(logitModel, trainComplete).cache()

# SAS: lackfit -> Hosmer-Lemeshow goodness-of-fit (10 groups by pred_prob)
hlGroups = trainScored.withColumn(
    "HL_GROUP", ntile(10).over(Window.orderBy("pred_prob"))
).groupBy("HL_GROUP").agg(
    count("*").alias("n"),
    spark_sum("label").alias("observed"),
    spark_sum("pred_prob").alias("expected")
).orderBy("HL_GROUP").collect()
hlStat = sum(
    (r["observed"] - r["expected"]) ** 2 /
    (r["expected"] * (1 - r["expected"] / r["n"]))
    for r in hlGroups
)
hlDf = len(hlGroups) - 2
print("\nHosmer and Lemeshow Goodness-of-Fit Test (lackfit)")
print(f"  ChiSq={hlStat:.4f}  DF={hlDf}  Pr>ChiSq={chiSquareSf(hlStat, hlDf):.4f}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# Rows missing a variable of the selected model get a missing pred_prob,
# as in PLM; rows missing only dropped effects are still scored.
# ------------------------------------------------------------------
scorable = None
for effect in selected:
    cond = col(effect).isNotNull()
    scorable = cond if scorable is None else scorable & cond

validScored = scoreWithProbability(logitModel, valid.filter(scorable)) \
    .select(*valid.columns, "pred_prob") \
    .unionByName(valid.filter(~scorable).withColumn("pred_prob", lit(None).cast("double")))

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored; set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
#   run;
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
#
# Note: SAS treats a missing pred_prob as smaller than 0.5 and would label
# unscorable rows PREDICTED_BAD = 0; here they stay null and are reported as
# "Frequency Missing" (PROC FREQ's treatment of missing table values).
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob").isNull(), lit(None))
    .when(col("pred_prob") >= CUTOFF, lit(1))
    .otherwise(lit(0))
).cache()

print("\n" + "=" * 60)
print("Step 5: Confusion Matrix - Validation Set")
print("=" * 60)
# SAS: PROC FREQ two-way table -> groupBy().pivot().count()
validScored.filter(col("PREDICTED_BAD").isNotNull()) \
    .groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).count() \
    .na.fill(0).orderBy("BAD") \
    .withColumnRenamed("0", "PREDICTED_BAD=0") \
    .withColumnRenamed("1", "PREDICTED_BAD=1") \
    .show()
print(f"Frequency Missing = {validScored.filter(col('PREDICTED_BAD').isNull()).count()}")

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
# As in the SAS program, this fits the full (no selection) model on the
# validation data itself.
# ------------------------------------------------------------------
fullPipeline = Pipeline(stages=encoderStages + [
    VectorAssembler(inputCols=[effectColumn[e] for e in allEffects],
                    outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label",
                       regParam=0.0, maxIter=100)
])
fullValidScored = scoreWithProbability(
    fullPipeline.fit(validComplete), validComplete).cache()

# SAS: ROC statement -> BinaryClassificationEvaluator(areaUnderROC)
auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction",
    metricName="areaUnderROC").evaluate(fullValidScored)

# SAS: Association table -> concordant/discordant/tied pair counts computed
# from per-probability event/non-event counts with a running window sum
pairCounts = fullValidScored.groupBy("pred_prob").agg(
    spark_sum("label").alias("events"),
    spark_sum(1 - col("label")).alias("nonEvents")
).withColumn(
    "nonEventsBelow",
    spark_sum("nonEvents").over(
        Window.orderBy("pred_prob").rowsBetween(Window.unboundedPreceding, -1))
).na.fill(0, ["nonEventsBelow"]).agg(
    spark_sum(col("events") * col("nonEventsBelow")).alias("concordant"),
    spark_sum(col("events") * col("nonEvents")).alias("tied"),
    spark_sum("events").alias("totalEvents"),
    spark_sum("nonEvents").alias("totalNonEvents")
).collect()[0]

nObs = int(pairCounts["totalEvents"] + pairCounts["totalNonEvents"])
pairs = pairCounts["totalEvents"] * pairCounts["totalNonEvents"]
concordant = pairCounts["concordant"]
tied = pairCounts["tied"]
discordant = pairs - concordant - tied
associationStats = [
    ("Percent Concordant", 100 * concordant / pairs),
    ("Percent Discordant", 100 * discordant / pairs),
    ("Percent Tied", 100 * tied / pairs),
    ("Pairs", pairs),
    ("Somers' D", (concordant - discordant) / pairs),
    ("Gamma", (concordant - discordant) / (concordant + discordant)),
    ("Tau-a", (concordant - discordant) / (0.5 * nObs * (nObs - 1))),
    ("c", (concordant + 0.5 * tied) / pairs),
]

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 7: Model Association Statistics")
print("=" * 60)
spark.createDataFrame(
    [(name, float(value)) for name, value in associationStats],
    ["Statistic", "Value"]
).show(truncate=False)
print(f"Area under ROC curve (roc): {auc:.4f}")

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 8: Predicted Probability Distribution by Actual Outcome")
print("=" * 60)
# SAS: PROC MEANS CLASS -> groupBy().agg(); P25/MEDIAN/P75 -> percentile_approx
validScored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("Std_Dev"),
    spark_min("pred_prob").alias("Minimum"),
    percentile_approx("pred_prob", 0.25).alias("P25"),
    percentile_approx("pred_prob", 0.5).alias("Median"),
    percentile_approx("pred_prob", 0.75).alias("P75"),
    spark_max("pred_prob").alias("Maximum")
).orderBy("BAD").show()

# Clean up
spark.stop()
