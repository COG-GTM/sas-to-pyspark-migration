"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - PROC LOGISTIC with stepwise selection
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

import math
import shutil
import tempfile

import numpy as np
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Column labels (SAS-style variable labels) for the modeling variables.
# PySpark has no native labels, so they are documented here as in 01.
columnLabels = {
    "BAD": "Loan Status (1=Default, 0=Paid)",
    "LOAN": "Amount of Loan Request",
    "MORTDUE": "Amount Due on Existing Mortgage",
    "VALUE": "Value of Current Property",
    "DEBTINC": "Debt to Income Ratio",
    "DELINQ": "Number of Delinquent Credit Lines",
    "DEROG": "Number of Derogatory Reports",
    "CLAGE": "Age of Oldest Credit Line (months)",
    "NINQ": "Number of Recent Credit Inquiries",
    "JOB": "Job Category",
    "REASON": "Loan Purpose (HomeImp or DebtCon)",
    "SELECTED": "Selection Indicator (PROC SURVEYSELECT)",
    "PRED_PROB": "Estimated Probability BAD=1",
    "PREDICTED_BAD": "Predicted Default (PRED_PROB >= 0.5)",
}

# Display formats used when printing results (SAS w.d formats)
columnFormats = {
    "PRED_PROB": "8.4",
    "CHI_SQUARE": "10.4",
    "ESTIMATE": "12.6",
}

SLENTRY = 0.05
SLSTAY = 0.05

# CLASS JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
# Reference-cell coding: one 0/1 design column per non-reference level,
# with levels in SAS's sorted order.
CLASS_LEVELS = {
    "JOB": ["Mgr", "Office", "ProfExe", "Sales", "Self"],
    "REASON": ["DebtCon"],
}
CLASS_REFS = {"JOB": "Other", "REASON": "HomeImp"}

# Effects in MODEL statement order, mapped to their design columns
MODEL_EFFECTS = {
    name: [name] for name in
    ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
}
for classVar, levels in CLASS_LEVELS.items():
    MODEL_EFFECTS[classVar] = [f"{classVar}_{lvl.upper()}" for lvl in levels]
ALL_DESIGN_COLS = [c for cols in MODEL_EFFECTS.values() for c in cols]
MODEL_VARS = list(MODEL_EFFECTS.keys())


# ------------------------------------------------------------------
# Helpers (PROC LOGISTIC / PROC FREQ / PROC MEANS building blocks)
# ------------------------------------------------------------------
def chi2_sf(x, df):
    """Upper-tail chi-square probability (Pr > ChiSq) for integer df."""
    if x <= 0:
        return 1.0
    if df % 2 == 0:
        term = total = math.exp(-x / 2)
        for i in range(1, df // 2):
            term *= (x / 2) / i
            total += term
        return min(total, 1.0)
    total = math.erfc(math.sqrt(x / 2))
    term = math.sqrt(2 * x / math.pi) * math.exp(-x / 2)
    for i in range(1, (df + 1) // 2):
        total += term
        term *= x / (2 * i + 1)
    return min(total, 1.0)


def fmt_p(p):
    """SAS PVALUE6.4 style."""
    return "<.0001" if p < 0.0001 else f"{p:.4f}"


def add_design_columns(frame):
    """Reference-coded design columns. A missing class value gives null
    design columns (SAS treats the observation as missing), not the
    reference level."""
    for classVar, levels in CLASS_LEVELS.items():
        for lvl in levels:
            frame = frame.withColumn(
                f"{classVar}_{lvl.upper()}",
                when(col(classVar).isNull(), lit(None).cast("double"))
                .when(col(classVar) == lvl, lit(1.0))
                .otherwise(lit(0.0))
            )
    return frame.withColumn("LABEL", col("BAD").cast("double"))


def fit_logistic(frame, designCols):
    """Maximum likelihood logistic fit (binomial GLM, logit link, IRLS,
    no penalty). Returns (PipelineModel or None, frame with _P)."""
    if not designCols:
        p0 = frame.agg(F.avg("LABEL")).collect()[0][0]
        return None, frame.withColumn("_P", lit(p0))
    assembler = VectorAssembler(
        inputCols=designCols, outputCol="FEATURES", handleInvalid="keep"
    )
    glm = GeneralizedLinearRegression(
        family="binomial", link="logit", featuresCol="FEATURES",
        labelCol="LABEL", predictionCol="_P", maxIter=100, tol=1e-8,
        regParam=0.0
    )
    model = Pipeline(stages=[assembler, glm]).fit(frame)
    return model, model.transform(frame).drop("FEATURES")


def coefficient_vector(model, designCols):
    """[intercept, b1, ..., bk] in designCols order."""
    if model is None:
        return None
    glm = model.stages[-1]
    return np.array([glm.intercept] + list(glm.coefficients))


def information_and_score(frame, designCols):
    """Fisher information X'WX and score vector X'(y - p) at the fitted
    probabilities in _P, aggregated in Spark (X includes the intercept)."""
    terms = [lit(1.0)] + [col(c).cast("double") for c in designCols]
    w = col("_P") * (lit(1.0) - col("_P"))
    r = col("LABEL") - col("_P")
    k = len(terms)
    aggs = []
    for i in range(k):
        aggs.append(F.sum(terms[i] * r).alias(f"u_{i}"))
        for j in range(i, k):
            aggs.append(F.sum(terms[i] * terms[j] * w).alias(f"i_{i}_{j}"))
    row = frame.agg(*aggs).collect()[0]
    info = np.zeros((k, k))
    score = np.zeros(k)
    for i in range(k):
        score[i] = row[f"u_{i}"]
        for j in range(i, k):
            info[i, j] = info[j, i] = row[f"i_{i}_{j}"]
    return info, score


def neg2_log_l(frame):
    """-2 Log L of the fitted probabilities in _P."""
    return frame.agg(
        (-2 * F.sum(
            col("LABEL") * F.log(col("_P")) +
            (1 - col("LABEL")) * F.log(1 - col("_P"))
        )).alias("m2ll")
    ).collect()[0]["m2ll"]


def scaled_inverse(matrix):
    """Inverse of a symmetric positive definite matrix, computed on the
    unit-diagonal (Jacobi-scaled) matrix so that dollar-scale columns such
    as LOAN and VALUE don't make the inversion ill-conditioned."""
    d = np.sqrt(np.diag(matrix))
    return np.linalg.inv(matrix / np.outer(d, d)) / np.outer(d, d)


def score_test(info, score, inIdx, testIdx):
    """Score chi-square for adding the design columns testIdx to a model
    whose columns are inIdx (index 0 = intercept)."""
    idx = inIdx + testIdx
    u = score[idx]
    return float(u @ scaled_inverse(info[np.ix_(idx, idx)]) @ u)


def wald_test(beta, cov, testIdx):
    """Wald chi-square for the parameters testIdx."""
    b = beta[testIdx]
    return float(b @ scaled_inverse(cov[np.ix_(testIdx, testIdx)]) @ b)


def effect_indices(effect, designCols):
    """Positions (1-based, after the intercept) of an effect's columns."""
    return [designCols.index(c) + 1 for c in MODEL_EFFECTS[effect]]


def association_stats(frame, probCol):
    """Concordant / discordant / tied pairs between events (BAD=1) and
    non-events (BAD=0), plus Somers' D, Gamma, Tau-a and c."""
    byProb = frame.filter(col(probCol).isNotNull()).groupBy(probCol).agg(
        F.sum(when(col("BAD") == 1, 1).otherwise(0)).cast("long").alias("EV"),
        F.sum(when(col("BAD") == 0, 1).otherwise(0)).cast("long").alias("NE"),
    )
    below = Window.orderBy(probCol).rowsBetween(Window.unboundedPreceding, -1)
    byProb = byProb.withColumn(
        "NE_BELOW", F.coalesce(F.sum("NE").over(below), lit(0)).cast("long")
    )
    r = byProb.agg(
        F.sum(col("EV") * col("NE_BELOW")).alias("CONC"),
        F.sum(col("EV") * col("NE")).alias("TIED"),
        F.sum("EV").alias("N_EV"),
        F.sum("NE").alias("N_NE"),
    ).collect()[0]
    pairs = r["N_EV"] * r["N_NE"]
    conc, tied = r["CONC"], r["TIED"]
    disc = pairs - conc - tied
    n = r["N_EV"] + r["N_NE"]
    return {
        "Percent Concordant": 100.0 * conc / pairs,
        "Percent Discordant": 100.0 * disc / pairs,
        "Percent Tied": 100.0 * tied / pairs,
        "Pairs": pairs,
        "Somers' D": (conc - disc) / pairs,
        "Gamma": (conc - disc) / (conc + disc),
        "Tau-a": (conc - disc) / (0.5 * n * (n - 1)),
        "c": (conc + 0.5 * tied) / pairs,
    }


def association_table(stats):
    """ODS OUTPUT Association layout (Label1/cValue1/Label2/cValue2)."""
    somersD = stats["Somers' D"]
    rows = [
        ("Percent Concordant", f"{stats['Percent Concordant']:.1f}",
         "Somers' D", f"{somersD:.3f}"),
        ("Percent Discordant", f"{stats['Percent Discordant']:.1f}",
         "Gamma", f"{stats['Gamma']:.3f}"),
        ("Percent Tied", f"{stats['Percent Tied']:.1f}",
         "Tau-a", f"{stats['Tau-a']:.3f}"),
        ("Pairs", f"{stats['Pairs']}", "c", f"{stats['c']:.3f}"),
    ]
    return spark.createDataFrame(
        rows, ["LABEL1", "CVALUE1", "LABEL2", "CVALUE2"]
    )


def print_logistic_report(used, model, designCols, effects):
    """Default PROC LOGISTIC output for a fitted model: response profile,
    model fit statistics, global null tests, Type 3 effects, maximum
    likelihood estimates and odds ratios."""
    _, nullScored = fit_logistic(used, [])
    scored = model.transform(used).drop("FEATURES")
    k = len(designCols) + 1
    nUsed = used.count()

    print("\nResponse Profile (Probability modeled is BAD=1)")
    used.groupBy("BAD").count().orderBy(col("BAD").desc()) \
        .withColumnRenamed("count", "TOTAL_FREQUENCY").show()

    m2llNull = neg2_log_l(nullScored)
    m2llFull = neg2_log_l(scored)
    print("Model Fit Statistics")
    spark.createDataFrame([
        ("AIC", round(m2llNull + 2, 3), round(m2llFull + 2 * k, 3)),
        ("SC", round(m2llNull + math.log(nUsed), 3),
         round(m2llFull + k * math.log(nUsed), 3)),
        ("-2 Log L", round(m2llNull, 3), round(m2llFull, 3)),
    ], ["CRITERION", "INTERCEPT_ONLY", "INTERCEPT_AND_COVARIATES"]).show()

    beta = coefficient_vector(model, designCols)
    info, _ = information_and_score(scored, designCols)
    cov = scaled_inverse(info)
    infoNull, scoreNull = information_and_score(nullScored, designCols)
    allIdx = list(range(1, k))
    lr = m2llNull - m2llFull
    sc = score_test(infoNull, scoreNull, [0], allIdx)
    wd = wald_test(beta, cov, allIdx)
    print("Testing Global Null Hypothesis: BETA=0")
    spark.createDataFrame([
        ("Likelihood Ratio", round(lr, 4), k - 1, fmt_p(chi2_sf(lr, k - 1))),
        ("Score", round(sc, 4), k - 1, fmt_p(chi2_sf(sc, k - 1))),
        ("Wald", round(wd, 4), k - 1, fmt_p(chi2_sf(wd, k - 1))),
    ], ["TEST", "CHI_SQUARE", "DF", "PR_GT_CHISQ"]).show()

    print("Type 3 Analysis of Effects")
    type3 = []
    for eff in effects:
        idx = effect_indices(eff, designCols)
        w = wald_test(beta, cov, idx)
        type3.append((eff, len(idx), round(w, 4), fmt_p(chi2_sf(w, len(idx)))))
    spark.createDataFrame(
        type3, ["EFFECT", "DF", "WALD_CHI_SQUARE", "PR_GT_CHISQ"]
    ).show()

    print("Analysis of Maximum Likelihood Estimates")
    se = np.sqrt(np.diag(cov))
    names = ["Intercept"] + designCols
    mle = []
    for i, name in enumerate(names):
        w = float((beta[i] / se[i]) ** 2)
        mle.append((name, 1, f"{beta[i]:.6g}", f"{se[i]:.6g}",
                    round(w, 4), fmt_p(chi2_sf(w, 1))))
    spark.createDataFrame(mle, [
        "PARAMETER", "DF", "ESTIMATE", "STANDARD_ERROR",
        "WALD_CHI_SQUARE", "PR_GT_CHISQ"
    ]).show(truncate=False)

    print("Odds Ratio Estimates (95% Wald confidence limits)")
    z = 1.959963984540054
    odds = []
    for i, name in enumerate(designCols, start=1):
        label = name
        for classVar, levels in CLASS_LEVELS.items():
            for lvl in levels:
                if name == f"{classVar}_{lvl.upper()}":
                    label = f"{classVar} {lvl} vs {CLASS_REFS[classVar]}"
        odds.append((label, round(math.exp(beta[i]), 3),
                     round(math.exp(beta[i] - z * se[i]), 3),
                     round(math.exp(beta[i] + z * se[i]), 3)))
    spark.createDataFrame(
        odds, ["EFFECT", "POINT_ESTIMATE", "LOWER_95_CL", "UPPER_95_CL"]
    ).show(truncate=False)

    print("Association of Predicted Probabilities and Observed Responses")
    association_table(association_stats(scored, "_P")).show(truncate=False)
    return scored


def to_pred_prob(frame):
    """_P from the GLM (NaN when a model predictor is missing) -> PRED_PROB
    (null, i.e. SAS missing)."""
    return frame.withColumn(
        "PRED_PROB", when(F.isnan(col("_P")), lit(None)).otherwise(col("_P"))
    ).drop("_P", "FEATURES")


# ------------------------------------------------------------------
# Step 0: Rebuild the input dataset work.home_equity_risk from the CSV
# SAS equivalent (sas/02_data_cleaning.sas, the row filters that feed
# work.home_equity_final; sas/04_risk_segmentation.sas only adds risk
# columns that this program does not use):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   if LOAN ne . and VALUE ne . and BAD ne .;
#   if LTV > 0 and LTV < 5;
#   if LOAN > 0;
#   if VALUE > 0;
# ------------------------------------------------------------------
df = spark.read.csv(
    "data/home_equity.csv",
    header=True,
    inferSchema=True
)

home_equity_risk = df.withColumn(
    "LTV",
    when(
        col("VALUE").isNotNull() & col("MORTDUE").isNotNull() &
        (col("VALUE") > 0),
        col("MORTDUE") / col("VALUE")
    )
).filter(
    col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
).filter(
    (col("LTV") > 0) & (col("LTV") < 5)
).filter(
    (col("LOAN") > 0) & (col("VALUE") > 0)
)

print("=" * 60)
print("Column Labels (SAS-style variable labels)")
print("=" * 60)
for colName, label in columnLabels.items():
    print(f"  {colName:14s} -> {label}")

print("\n" + "=" * 60)
print("Step 0: Input dataset (work.home_equity_risk)")
print("=" * 60)
print(f"Observations in work.home_equity_risk: {home_equity_risk.count()}")

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - remove records with excessive missing
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
model_data = home_equity_risk.filter(
    col("LOAN").isNotNull() & col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() & col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() & col("CLAGE").isNotNull()
)

print("\n" + "=" * 60)
print("Step 1: Modeling dataset (work.model_data)")
print("=" * 60)
nModel = model_data.count()
print(f"Observations in work.model_data: {nModel}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data
#       out=work.model_split
#       method=srs
#       samprate=0.7
#       seed=42;
#   run;
#
#   data work.train work.valid;
#       set work.model_split;
#       if selected = 1 then output work.train;
#       else output work.valid;
#   run;
#
# Note: METHOD=SRS draws exactly ceil(samprate * N) rows without
# replacement, so rows are ranked by rand(42) and the first n are
# selected (randomSplit would only give approximately 70%). The Spark
# and SAS random streams differ, so the selected rows differ from SAS.
# The DATA step reads the SELECTED flag, which SURVEYSELECT only writes
# with OUTALL; the flag is written for every row here so that the
# 70/30 split the program intends is produced.
# ------------------------------------------------------------------
sampleSize = math.ceil(round(0.7 * nModel, 9))
model_split = model_data.coalesce(1) \
    .withColumn("_U", F.rand(42)) \
    .withColumn("_ROW", F.row_number().over(Window.orderBy("_U"))) \
    .withColumn("SELECTED", when(col("_ROW") <= sampleSize, 1).otherwise(0)) \
    .withColumn("SELECTIONPROB", lit(sampleSize / nModel)) \
    .withColumn("SAMPLINGWEIGHT", lit(nModel / sampleSize)) \
    .drop("_U", "_ROW") \
    .cache()

train = model_split.filter(col("SELECTED") == 1)
valid = model_split.filter(~(col("SELECTED") == 1))

print("\n" + "=" * 60)
print("Step 2: Simple random sample (PROC SURVEYSELECT method=srs)")
print("=" * 60)
print(f"Sample size (ceil(0.7 * {nModel})): {sampleSize}")
print(f"Training rows (work.train): {train.count()}")
print(f"Validation rows (work.valid): {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression with stepwise selection
# SAS equivalent:
#   title "Logistic Regression: Loan Default Prediction";
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON
#                 / selection=stepwise
#                   slentry=0.05
#                   slstay=0.05
#                   details
#                   lackfit;
#       output out=work.train_scored predicted=pred_prob;
#       store work.logit_model;
#   run;
#
# Note: Spark ML has no stepwise selection, so it is implemented here
# the way PROC LOGISTIC does it: effects enter on the score chi-square
# (p < SLENTRY) and leave on the Wald chi-square (p > SLSTAY), checked
# after every entry. Each candidate model is an unpenalized maximum
# likelihood fit (GeneralizedLinearRegression, binomial/logit, IRLS).
# DESCENDING -> the modeled probability is P(BAD=1). PROC LOGISTIC fits
# on rows with no missing value in any MODEL variable.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 3: Logistic Regression: Loan Default Prediction")
print("=" * 60)

trainDesign = add_design_columns(train)
train_used = trainDesign.filter(
    F.expr(" AND ".join(f"{v} IS NOT NULL" for v in MODEL_VARS + ["BAD"]))
).cache()
print(f"Number of Observations Read: {train.count()}")
print(f"Number of Observations Used: {train_used.count()}")

print("\nClass Level Information (param=ref)")
classInfo = []
for classVar, levels in CLASS_LEVELS.items():
    allLevels = sorted(levels + [CLASS_REFS[classVar]])
    for lvl in allLevels:
        design = [1 if lvl == d else 0 for d in levels]
        classInfo.append((classVar, lvl, " ".join(str(d) for d in design)))
spark.createDataFrame(
    classInfo, ["CLASS", "VALUE", "DESIGN_VARIABLES"]
).show(truncate=False)

inModel = []
stepSummary = []
lastEntered = None
lastRemoved = None
for stepNo in range(1, 2 * len(MODEL_EFFECTS) + 1):
    designCols = [c for e in inModel for c in MODEL_EFFECTS[e]]
    stepModel, stepScored = fit_logistic(train_used, designCols)
    stepCols = designCols + [c for c in ALL_DESIGN_COLS if c not in designCols]
    info, score = information_and_score(stepScored, stepCols)
    inIdx = list(range(len(designCols) + 1))

    # Removal check (Wald chi-square, SLSTAY)
    if inModel:
        beta = coefficient_vector(stepModel, designCols)
        cov = scaled_inverse(info[np.ix_(inIdx, inIdx)])
        removal = []
        for eff in inModel:
            idx = effect_indices(eff, designCols)
            w = wald_test(beta, cov, idx)
            removal.append((eff, len(idx), w, chi2_sf(w, len(idx))))
        print(f"Step {stepNo}: Analysis of Effects Eligible for Removal")
        spark.createDataFrame(
            [(e, d, round(w, 4), fmt_p(p)) for e, d, w, p in removal],
            ["EFFECT", "DF", "WALD_CHI_SQUARE", "PR_GT_CHISQ"]
        ).show()
        worst = max(removal, key=lambda t: t[3])
        if worst[3] > SLSTAY:
            inModel.remove(worst[0])
            stepSummary.append((len(stepSummary) + 1, None, worst[0], worst[1],
                                len(inModel), None, round(worst[2], 4),
                                fmt_p(worst[3])))
            print(f"  Effect {worst[0]} is removed.")
            if worst[0] == lastEntered:
                break
            lastRemoved = worst[0]
            continue

    # Entry check (score chi-square, SLENTRY)
    candidates = [e for e in MODEL_EFFECTS if e not in inModel]
    if not candidates:
        break
    entry = []
    for eff in candidates:
        idx = effect_indices(eff, stepCols)
        s = score_test(info, score, inIdx, idx)
        entry.append((eff, len(idx), s, chi2_sf(s, len(idx))))
    print(f"Step {stepNo}: Analysis of Effects Eligible for Entry")
    spark.createDataFrame(
        [(e, d, round(s, 4), fmt_p(p)) for e, d, s, p in entry],
        ["EFFECT", "DF", "SCORE_CHI_SQUARE", "PR_GT_CHISQ"]
    ).show()
    best = min(entry, key=lambda t: t[3])
    if best[3] >= SLENTRY or best[0] == lastRemoved:
        print(f"  No (additional) effects met the {SLENTRY} significance "
              "level for entry into the model.")
        break
    inModel.append(best[0])
    lastEntered = best[0]
    stepSummary.append((len(stepSummary) + 1, best[0], None, best[1],
                        len(inModel), round(best[2], 4), None, fmt_p(best[3])))
    print(f"  Effect {best[0]} is entered.")

selectedEffects = [e for e in MODEL_EFFECTS if e in inModel]
selectedCols = [c for e in selectedEffects for c in MODEL_EFFECTS[e]]
print("\nSummary of Stepwise Selection")
spark.createDataFrame(
    stepSummary,
    "STEP int, ENTERED string, REMOVED string, DF int, NUMBER_IN int, "
    "SCORE_CHI_SQUARE double, WALD_CHI_SQUARE double, PR_GT_CHISQ string"
).show(truncate=False)
print(f"Selected effects: {', '.join(selectedEffects)}")

finalModel, _ = fit_logistic(train_used, selectedCols)
trainUsedScored = print_logistic_report(
    train_used, finalModel, selectedCols, selectedEffects
)

# LACKFIT: Hosmer-Lemeshow goodness-of-fit test (10 groups on sorted
# predicted probability; tied probabilities stay in the same group).
hlGroups = trainUsedScored.groupBy("_P").agg(
    F.count(lit(1)).alias("N"),
    F.sum("LABEL").alias("O1"),
)
nUsedTrain = train_used.count()
hlWindow = Window.orderBy("_P").rowsBetween(Window.unboundedPreceding, -1)
hlGroups = hlGroups.withColumn(
    "N_BELOW", F.coalesce(F.sum("N").over(hlWindow), lit(0))
).withColumn(
    "GROUP", F.floor(lit(10) * col("N_BELOW") / lit(nUsedTrain)) + 1
)
hosmer_lemeshow = hlGroups.groupBy("GROUP").agg(
    F.sum("N").alias("TOTAL"),
    F.sum("O1").cast("long").alias("OBSERVED_BAD1"),
    F.round(F.sum(col("N") * col("_P")), 2).alias("EXPECTED_BAD1"),
    (F.sum("N") - F.sum("O1")).cast("long").alias("OBSERVED_BAD0"),
    F.round(F.sum(col("N") * (1 - col("_P"))), 2).alias("EXPECTED_BAD0"),
    F.sum(col("N") * col("_P")).alias("_E1"),
    F.sum(col("N") * (1 - col("_P"))).alias("_E0"),
).orderBy("GROUP")
print("Partition for the Hosmer and Lemeshow Test")
hosmer_lemeshow.drop("_E1", "_E0").show()
hlRows = hosmer_lemeshow.collect()
hlChi = sum(
    (r["OBSERVED_BAD1"] - r["_E1"]) ** 2 / r["_E1"] +
    (r["OBSERVED_BAD0"] - r["_E0"]) ** 2 / r["_E0"]
    for r in hlRows
)
hlDf = len(hlRows) - 2
print("Hosmer and Lemeshow Goodness-of-Fit Test")
print(f"  Chi-Square = {hlChi:.4f}  DF = {hlDf}  "
      f"Pr > ChiSq = {fmt_p(chi2_sf(hlChi, hlDf))}")

# OUTPUT OUT=work.train_scored PREDICTED=pred_prob: every training row
# is scored; PRED_PROB is missing only where a final-model predictor is.
train_scored = to_pred_prob(finalModel.transform(trainDesign))

# STORE work.logit_model: persist the fitted model (item store)
modelStorePath = tempfile.mkdtemp(prefix="logit_model_")
finalModel.write().overwrite().save(modelStorePath)
print(f"\nModel stored (STORE work.logit_model) at: {modelStorePath}")
print(f"Scored training rows (work.train_scored): {train_scored.count()}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
#
# Note: ILINK -> probability scale; GLM predictions are already on the
# inverse-link (probability) scale. Rows with a missing final-model
# predictor get a missing PRED_PROB, as in PROC PLM.
# ------------------------------------------------------------------
logit_model = PipelineModel.load(modelStorePath)
valid_scored = to_pred_prob(logit_model.transform(add_design_columns(valid)))
shutil.rmtree(modelStorePath, ignore_errors=True)

print("\n" + "=" * 60)
print("Step 4: Score validation data (PROC PLM)")
print("=" * 60)
nMissingProb = valid_scored.filter(col("PRED_PROB").isNull()).count()
print(f"Scored validation rows (work.valid_scored): {valid_scored.count()}")
print(f"Validation rows with missing PRED_PROB: {nMissingProb}")
valid_scored.select(
    "BAD", *selectedEffects, F.round("PRED_PROB", 4).alias("PRED_PROB")
).show(10)

# ------------------------------------------------------------------
# Step 5: Create predicted classes and confusion matrix
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#       else PREDICTED_BAD = 0;
#   run;
#
#   title "Confusion Matrix - Validation Set";
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
#
# Note: a missing PRED_PROB is not >= 0.5 in SAS, so it becomes
# PREDICTED_BAD = 0; when() with a null condition falls through to
# otherwise(0), which matches.
# ------------------------------------------------------------------
valid_scored = valid_scored.withColumn(
    "PREDICTED_BAD",
    when(col("PRED_PROB") >= 0.5, lit(1)).otherwise(lit(0))
)

print("\n" + "=" * 60)
print("Step 5: Confusion Matrix - Validation Set")
print("=" * 60)
confusion = valid_scored.groupBy("BAD") \
    .pivot("PREDICTED_BAD", [0, 1]).count().na.fill(0) \
    .withColumnRenamed("0", "PREDICTED_BAD_0") \
    .withColumnRenamed("1", "PREDICTED_BAD_1") \
    .withColumn("TOTAL", col("PREDICTED_BAD_0") + col("PREDICTED_BAD_1")) \
    .withColumn("BAD", col("BAD").cast("string"))
confusionTotal = confusion.agg(
    lit("Total").alias("BAD"),
    F.sum("PREDICTED_BAD_0").alias("PREDICTED_BAD_0"),
    F.sum("PREDICTED_BAD_1").alias("PREDICTED_BAD_1"),
    F.sum("TOTAL").alias("TOTAL"),
)
confusion.orderBy("BAD").unionByName(confusionTotal).show()
print(f"Confusion matrix total: {confusionTotal.collect()[0]['TOTAL']}")

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
# Note: as in the SAS program, this refits the full (no selection) model
# on the validation rows, so these statistics describe that refit, not
# the stepwise model scored in Step 4. Concordance is computed exactly
# over all event/non-event pairs.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 6: Model Performance - Concordance and AUC")
print("=" * 60)
valid_used = add_design_columns(valid).filter(
    F.expr(" AND ".join(f"{v} IS NOT NULL" for v in MODEL_VARS + ["BAD"]))
).cache()
print(f"Number of Observations Read: {valid.count()}")
print(f"Number of Observations Used: {valid_used.count()}")
validModel, _ = fit_logistic(valid_used, ALL_DESIGN_COLS)
validRefit = print_logistic_report(
    valid_used, validModel, ALL_DESIGN_COLS, MODEL_VARS
)
validStats = association_stats(validRefit, "_P")
association_stats_df = association_table(validStats)

# ROC statement: area under the ROC curve (equals c)
rocAuc = BinaryClassificationEvaluator(
    labelCol="LABEL", rawPredictionCol="_P",
    metricName="areaUnderROC", numBins=0
).evaluate(validRefit)
print(f"ROC area (c) - validation refit: {validStats['c']:.4f}")
print(f"BinaryClassificationEvaluator areaUnderROC: {rocAuc:.4f}")

# ------------------------------------------------------------------
# Step 7: Display key metrics
# SAS equivalent:
#   title "Model Association Statistics";
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 7: Model Association Statistics")
print("=" * 60)
association_stats_df.show(truncate=False)

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   title "Predicted Probability Distribution by Actual Outcome";
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
#
# Note: percentiles use the PROC MEANS default definition (QNTLDEF=5):
# with j = floor(n*p) and g = n*p - j, the percentile is the mean of
# x(j) and x(j+1) when g = 0, otherwise x(j+1). This is exact, unlike
# percentile_approx. std is the sample standard deviation.
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 8: Predicted Probability Distribution by Actual Outcome")
print("=" * 60)
probRanked = valid_scored.filter(col("PRED_PROB").isNotNull()) \
    .withColumn("_R", F.row_number().over(
        Window.partitionBy("BAD").orderBy("PRED_PROB"))) \
    .withColumn("_N", F.count(lit(1)).over(Window.partitionBy("BAD")))


def qntldef5(p):
    np_ = col("_N") * lit(p)
    j = F.floor(np_)
    g = np_ - j
    return F.coalesce(
        F.avg(when((g == 0) & ((col("_R") == j) | (col("_R") == j + 1)),
                   col("PRED_PROB"))),
        F.max(when((g > 0) & (col("_R") == j + 1), col("PRED_PROB")))
    )


nobs = valid_scored.groupBy("BAD").agg(F.count(lit(1)).alias("N_OBS"))
probStats = probRanked.groupBy("BAD").agg(
    F.count("PRED_PROB").alias("N"),
    F.round(F.mean("PRED_PROB"), 4).alias("MEAN"),
    F.round(F.stddev("PRED_PROB"), 4).alias("STD_DEV"),
    F.round(F.min("PRED_PROB"), 4).alias("MINIMUM"),
    F.round(qntldef5(0.25), 4).alias("P25"),
    F.round(qntldef5(0.50), 4).alias("MEDIAN"),
    F.round(qntldef5(0.75), 4).alias("P75"),
    F.round(F.max("PRED_PROB"), 4).alias("MAXIMUM"),
)
nobs.join(probStats, on="BAD", how="left").orderBy("BAD").show()

# Clean up
model_split.unpersist()
train_used.unpersist()
valid_used.unpersist()
spark.stop()
