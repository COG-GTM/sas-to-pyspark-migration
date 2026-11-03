"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Training / validation split (PROC SURVEYSELECT)
         - Logistic regression with CLASS/MODEL and stepwise selection
         - Model evaluation (confusion matrix, concordance, ROC/AUC)
Equivalent SAS Program: sas/05_logistic_regression.sas

The SAS program reads work.home_equity_risk, which is built on
work.home_equity_final from 02_data_cleaning. None of the risk-segmentation
columns are used by the model, so this script reproduces only the 02 cleaning
inline to stay self-contained.
"""

import math

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, initcap
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
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SLENTRY = 0.05
SLSTAY = 0.05
SEED = 42
SAMPRATE = 0.7

NUMERIC_VARS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                "DELINQ", "DEROG", "CLAGE", "NINQ"]
CLASS_REFS = {"JOB": "Other", "REASON": "HomeImp"}
MODEL_EFFECTS = NUMERIC_VARS + list(CLASS_REFS)


def chiSquareSurvival(x, df):
    """Upper-tail probability P(ChiSq(df) > x) for integer df (no scipy)."""
    if x <= 0:
        return 1.0
    half = x / 2.0
    if df % 2 == 0:
        term, total = 1.0, 1.0
        for k in range(1, df // 2):
            term *= half / k
            total += term
        return min(1.0, math.exp(-half) * total)
    total = math.erfc(math.sqrt(half))
    term = math.sqrt(2.0 * x / math.pi) * math.exp(-half)
    for k in range(1, (df + 1) // 2):
        total += term
        term *= x / (2 * k + 1)
    return min(1.0, total)


def banner(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ------------------------------------------------------------------
# Step 0: Reproduce upstream cleaning (02_data_cleaning) inline
# SAS equivalent:
#   data work.home_equity_clean;
#       set work.home_equity;
#       if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#       else LTV = .;
#       if BAD = 0 then LOAN_OUTCOME = 'Paid';
#       else if BAD = 1 then LOAN_OUTCOME = 'Default';
#       else LOAN_OUTCOME = '';
#       CITY = propcase(CITY);
#   run;
#   data work.home_equity_filtered; set ...; if LOAN ne . and VALUE ne . and BAD ne .; run;
#   data work.home_equity_final;    set ...; if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0; run;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)
sourceCount = df.count()

dfClean = df \
    .withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        ).otherwise(lit(None).cast("double"))
    ) \
    .withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
        .otherwise(lit(""))
    ) \
    .withColumn("CITY", initcap(col("CITY")))

dfFiltered = dfClean.filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
)
filteredCount = dfFiltered.count()

dfFinal = dfFiltered.filter(
    (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
)
finalCount = dfFinal.count()

banner("Upstream Cleaning (02_data_cleaning reproduced inline)")
print(f"Source rows:                 {sourceCount}")
print(f"After LOAN/VALUE/BAD filter: {filteredCount}")
print(f"Final clean rows:            {finalCount}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - complete cases for key predictors
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
#
# Note: PROC LOGISTIC also drops (listwise) any observation with a missing
# value in ANY MODEL/CLASS variable, so DEROG, NINQ, JOB and REASON must be
# non-missing too. That deletion is applied here, up front, so the split,
# stepwise fit and scoring all work on the same rows SAS would use.
# ------------------------------------------------------------------
modelDataStep1 = dfFinal.filter(
    col("LOAN").isNotNull() & col("MORTDUE").isNotNull() & col("VALUE").isNotNull()
    & col("DEBTINC").isNotNull() & col("DELINQ").isNotNull() & col("CLAGE").isNotNull()
)
modelData = modelDataStep1.filter(
    col("DEROG").isNotNull() & col("NINQ").isNotNull()
    & col("JOB").isNotNull() & col("REASON").isNotNull()
).withColumn("label", col("BAD").cast("double"))

banner("Modeling Dataset (work.model_data)")
print(f"Rows with key predictors non-missing:     {modelDataStep1.count()}")
print(f"Rows used by PROC LOGISTIC (all effects): {modelData.count()}")

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
# METHOD=SRS draws exactly ceil(0.7 * N) rows without replacement, so rows
# are ranked by a seeded random number and the first n are flagged SELECTED.
# ------------------------------------------------------------------
nTotal = modelData.count()
nSample = int(math.ceil(SAMPRATE * nTotal))
modelSplit = modelData \
    .withColumn("_RAND", F.rand(SEED)) \
    .withColumn("_ROW", F.row_number().over(Window.orderBy("_RAND"))) \
    .withColumn("SELECTED", when(col("_ROW") <= nSample, lit(1)).otherwise(lit(0))) \
    .drop("_RAND", "_ROW") \
    .cache()

train = modelSplit.filter(col("SELECTED") == 1).cache()
valid = modelSplit.filter(col("SELECTED") == 0).cache()

banner("Training / Validation Split (PROC SURVEYSELECT SRS, seed=42)")
print(f"Training rows (selected=1):   {train.count()}")
print(f"Validation rows (selected=0): {valid.count()}")
modelSplit.groupBy("SELECTED", "BAD").count().orderBy("SELECTED", "BAD").show()

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
# CLASS ... / param=ref  -> StringIndexer with the reference level indexed
#                           last + OneHotEncoder(dropLast=True), giving one
#                           0/1 dummy per non-reference level.
# DESCENDING             -> model P(BAD = 1).
# SELECTION=STEPWISE     -> MLlib has no stepwise option, so it is
#                           reproduced explicitly: forward entry at
#                           SLENTRY and backward removal at SLSTAY, using
#                           likelihood-ratio chi-square tests on the
#                           deviance of binomial/logit GLM fits. CLASS
#                           effects enter/leave as a whole (multi-df).
#                           SAS uses score tests for entry and Wald tests
#                           for removal, so p-values can differ slightly.
# STORE / PROC PLM       -> fitted pyspark.ml PipelineModel reused to score.
# ------------------------------------------------------------------
classStages = []
classLevels = {}
for classVar, refLevel in CLASS_REFS.items():
    levels = sorted(
        r[classVar] for r in modelData.select(classVar).distinct().collect()
        if r[classVar] != refLevel
    )
    classLevels[classVar] = levels
    classStages.append(
        StringIndexerModel.from_labels(
            levels + [refLevel],
            inputCol=classVar,
            outputCol=f"{classVar}_IDX"
        )
    )
    classStages.append(
        OneHotEncoder(
            inputCol=f"{classVar}_IDX",
            outputCol=f"{classVar}_VEC",
            dropLast=True
        )
    )

encoder = Pipeline(stages=classStages).fit(modelData)
trainEnc = encoder.transform(train).cache()


def effectColumn(effect):
    return f"{effect}_VEC" if effect in CLASS_REFS else effect


def effectDf(effect):
    return len(classLevels[effect]) if effect in CLASS_REFS else 1


def parameterNames(effects):
    names = []
    for effect in effects:
        if effect in CLASS_REFS:
            names += [f"{effect} {lvl}" for lvl in classLevels[effect]]
        else:
            names.append(effect)
    return names


nTrain = trainEnc.count()
nEvents = trainEnc.filter(col("label") == 1).count()
pBar = nEvents / nTrain
nullDeviance = -2.0 * (nEvents * math.log(pBar) + (nTrain - nEvents) * math.log(1 - pBar))

glmCache = {}


def fitGlm(effects):
    key = tuple(sorted(effects))
    if key not in glmCache:
        assembler = VectorAssembler(
            inputCols=[effectColumn(e) for e in key], outputCol="features"
        )
        glm = GeneralizedLinearRegression(
            family="binomial", link="logit", labelCol="label",
            featuresCol="features", maxIter=50, regParam=0.0
        )
        glmCache[key] = glm.fit(assembler.transform(trainEnc))
    return glmCache[key]


def deviance(effects):
    return nullDeviance if not effects else fitGlm(effects).summary.deviance


banner("Logistic Regression: Loan Default Prediction")
print(f"Training observations used: {nTrain}  (events BAD=1: {nEvents})")
print(f"Stepwise selection: SLENTRY={SLENTRY}  SLSTAY={SLSTAY}")

selected = []
removed = set()
stepNo = 0
summaryRows = []
while True:
    candidates = [e for e in MODEL_EFFECTS if e not in selected and e not in removed]
    if not candidates:
        break
    baseDev = deviance(selected)
    entryTests = []
    for effect in candidates:
        chi = baseDev - deviance(selected + [effect])
        dfE = effectDf(effect)
        entryTests.append((effect, dfE, chi, chiSquareSurvival(chi, dfE)))
    entryTests.sort(key=lambda t: t[3])

    stepNo += 1
    print(f"\nStep {stepNo}. Analysis of Effects Eligible for Entry (LR chi-square)")
    spark.createDataFrame(
        [(e, d, round(c, 4), float(p)) for e, d, c, p in entryTests],
        ["Effect", "DF", "ChiSquare", "PrChiSq"]
    ).show(truncate=False)

    bestEffect, bestDf, bestChi, bestP = entryTests[0]
    if bestP >= SLENTRY:
        print(f"No (additional) effects met the {SLENTRY} significance level for entry.")
        break
    selected.append(bestEffect)
    summaryRows.append((stepNo, bestEffect, "", bestDf, round(bestChi, 4), float(bestP)))
    print(f"Effect {bestEffect} entered.")

    if len(selected) > 1:
        fullDev = deviance(selected)
        removalTests = []
        for effect in selected[:-1]:
            others = [e for e in selected if e != effect]
            chi = deviance(others) - fullDev
            dfE = effectDf(effect)
            removalTests.append((effect, dfE, chi, chiSquareSurvival(chi, dfE)))
        worst = max(removalTests, key=lambda t: t[3])
        if worst[3] > SLSTAY:
            stepNo += 1
            selected.remove(worst[0])
            removed.add(worst[0])
            summaryRows.append(
                (stepNo, "", worst[0], worst[1], round(worst[2], 4), float(worst[3]))
            )
            print(f"Effect {worst[0]} removed.")

banner("Summary of Stepwise Selection")
spark.createDataFrame(
    summaryRows,
    ["Step", "Entered", "Removed", "DF", "ChiSquare", "PrChiSq"]
).show(truncate=False)

finalGlm = fitGlm(selected)
glmOrder = sorted(selected)
glmSummary = finalGlm.summary
paramNames = parameterNames(glmOrder) + ["Intercept"]
coefs = list(finalGlm.coefficients) + [finalGlm.intercept]
paramRows = [
    (name, float(est), float(se), float((est / se) ** 2), float(p))
    for name, est, se, p in zip(
        paramNames, coefs,
        glmSummary.coefficientStandardErrors, glmSummary.pValues
    )
]
banner("Analysis of Maximum Likelihood Estimates")
spark.createDataFrame(
    [paramRows[-1]] + paramRows[:-1],
    ["Parameter", "Estimate", "StdError", "WaldChiSq", "PrChiSq"]
).show(truncate=False)
print(f"-2 Log L: intercept only = {nullDeviance:.3f}, "
      f"intercept and covariates = {glmSummary.deviance:.3f}")

# Fit the final selected model as a reusable pipeline
# (SAS equivalent: output out=work.train_scored predicted=pred_prob;
#                  store work.logit_model;)
logitModel = Pipeline(stages=classStages + [
    VectorAssembler(inputCols=[effectColumn(e) for e in selected], outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label",
                       maxIter=100, regParam=0.0)
]).fit(train)

trainScored = logitModel.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# Hosmer-Lemeshow goodness-of-fit (SAS equivalent: LACKFIT option)
hlGroups = trainScored \
    .withColumn("HL_GROUP", F.ntile(10).over(Window.orderBy("pred_prob"))) \
    .groupBy("HL_GROUP") \
    .agg(F.count("*").alias("N"),
         F.sum("label").alias("OBSERVED"),
         F.sum("pred_prob").alias("EXPECTED")) \
    .orderBy("HL_GROUP")
hlRows = hlGroups.collect()
hlChi = sum(
    (r["OBSERVED"] - r["EXPECTED"]) ** 2 / (r["EXPECTED"] * (1 - r["EXPECTED"] / r["N"]))
    for r in hlRows
)
hlDf = len(hlRows) - 2
banner("Hosmer and Lemeshow Goodness-of-Fit Test")
hlGroups.show()
print(f"Chi-Square = {hlChi:.4f}  DF = {hlDf}  Pr > ChiSq = {chiSquareSurvival(hlChi, hlDf):.4f}")

aucEvaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="pred_prob", metricName="areaUnderROC"
)
trainAuc = aucEvaluator.evaluate(trainScored)
print(f"\nTraining AUC (c statistic): {trainAuc:.4f}")

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
    "PREDICTED_BAD", when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
).cache()

banner("Confusion Matrix - Validation Set")
validScored.crosstab("BAD", "PREDICTED_BAD").orderBy("BAD_PREDICTED_BAD").show()
accuracy = validScored.filter(col("BAD") == col("PREDICTED_BAD")).count() / validScored.count()
print(f"Accuracy at 0.5 cutoff: {accuracy:.4f}")

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
# The full (no selection) model is fit on the validation set, as in SAS.
# Concordance is computed exactly over all event/non-event pairs.
# ------------------------------------------------------------------
validFullModel = Pipeline(stages=classStages + [
    VectorAssembler(inputCols=[effectColumn(e) for e in MODEL_EFFECTS], outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label",
                       maxIter=100, regParam=0.0)
]).fit(valid)
validFullScored = validFullModel.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

validAuc = aucEvaluator.evaluate(validFullScored)

byProb = validFullScored.groupBy("pred_prob").agg(
    F.sum("label").alias("EVENTS"),
    F.sum(1 - col("label")).alias("NONEVENTS")
)
cumWindow = Window.orderBy("pred_prob").rowsBetween(Window.unboundedPreceding, -1)
pairCounts = byProb \
    .withColumn("NONEVENTS_BELOW", F.coalesce(F.sum("NONEVENTS").over(cumWindow), lit(0.0))) \
    .agg(
        F.sum(col("EVENTS") * col("NONEVENTS_BELOW")).alias("CONCORDANT"),
        F.sum(col("EVENTS") * col("NONEVENTS")).alias("TIED"),
        F.sum("EVENTS").alias("N_EVENTS"),
        F.sum("NONEVENTS").alias("N_NONEVENTS")
    ).first()
nPairs = pairCounts["N_EVENTS"] * pairCounts["N_NONEVENTS"]
concordant = pairCounts["CONCORDANT"]
tied = pairCounts["TIED"]
discordant = nPairs - concordant - tied
nValid = pairCounts["N_EVENTS"] + pairCounts["N_NONEVENTS"]

associationStats = spark.createDataFrame([
    ("Percent Concordant", round(100 * concordant / nPairs, 1), "Somers' D", round((concordant - discordant) / nPairs, 3)),
    ("Percent Discordant", round(100 * discordant / nPairs, 1), "Gamma", round((concordant - discordant) / (concordant + discordant), 3)),
    ("Percent Tied", round(100 * tied / nPairs, 1), "Tau-a", round((concordant - discordant) / (0.5 * nValid * (nValid - 1)), 3)),
    ("Pairs", float(nPairs), "c", round((concordant + 0.5 * tied) / nPairs, 3)),
], ["Label1", "nValue1", "Label2", "nValue2"])

banner("Model Performance - Concordance and AUC")
print(f"Validation observations: {int(nValid)}  (events BAD=1: {int(pairCounts['N_EVENTS'])})")
print(f"ROC: Area Under Curve (AUC) = {validAuc:.4f}")

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
banner("Model Association Statistics")
associationStats.show(truncate=False)

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
banner("Predicted Probability Distribution by Actual Outcome")
validScored.groupBy("BAD").agg(
    F.count("pred_prob").alias("N"),
    F.round(F.mean("pred_prob"), 4).alias("Mean"),
    F.round(F.stddev("pred_prob"), 4).alias("Std Dev"),
    F.round(F.min("pred_prob"), 4).alias("Minimum"),
    F.round(F.expr("percentile(pred_prob, 0.25)"), 4).alias("25th Pctl"),
    F.round(F.median("pred_prob"), 4).alias("Median"),
    F.round(F.expr("percentile(pred_prob, 0.75)"), 4).alias("75th Pctl"),
    F.round(F.max("pred_prob"), 4).alias("Maximum")
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
