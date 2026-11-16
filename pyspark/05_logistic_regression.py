"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

import math
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexerModel, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

NUM_VARS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]

# SAS propcase() default delimiters: blank, slash, hyphen, open paren, period, tab
PROPCASE_DELIMS = " /-(.\t"


def propcase(colName):
    """SAS: propcase(x) -> capitalise the first letter of every delimited word.

    initcap() only splits on whitespace, so "winston-salem" would become
    "Winston-salem"; SAS gives "Winston-Salem".
    """
    lowered = F.lower(F.col(colName))
    delims = F.array(*[F.lit(c) for c in PROPCASE_DELIMS])
    chars = F.split(lowered, "")
    return F.when(lowered.isNull(), F.lit(None)).otherwise(
        F.array_join(
            F.transform(
                chars,
                lambda c, i: F.when(
                    (i == 0) | F.array_contains(delims, F.element_at(chars, i)),
                    F.upper(c),
                ).otherwise(c),
            ),
            "",
        )
    )


def print_title(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def proc_means(df, varCols, stats, classCols=()):
    """SAS: PROC MEANS <stats>; CLASS ...; VAR ...; -> groupBy().agg() per variable.

    Percentiles use percentile_approx (accuracy 10000, exact at this data size);
    SAS's default QNTLDEF=5 may average two neighbouring values instead.
    Rows with a missing CLASS value are excluded, as in SAS (no MISSING option).
    """
    statExprs = {
        "N": lambda c: F.count(c),
        "NMISS": lambda c: F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)),
        "MEAN": lambda c: F.mean(c),
        "STD": lambda c: F.stddev_samp(c),
        "MIN": lambda c: F.min(c),
        "MAX": lambda c: F.max(c),
        "MEDIAN": lambda c: F.percentile_approx(c, 0.5, 10000),
    }
    for pct in (1, 5, 25, 75, 95, 99):
        statExprs[f"P{pct}"] = lambda c, q=pct / 100: F.percentile_approx(c, q, 10000)

    source = df
    for classCol in classCols:
        source = source.filter(F.col(classCol).isNotNull())

    result = None
    for varIdx, varCol in enumerate(varCols):
        part = source.groupBy(*classCols).agg(
            *[(statExprs[s](varCol) if s in ("N", "NMISS")
               else F.round(statExprs[s](varCol).cast("double"), 4)).alias(s) for s in stats]
        ).withColumn("Variable", F.lit(varCol)).withColumn("_varIdx", F.lit(varIdx))
        result = part if result is None else result.unionByName(part)
    return result.orderBy(*classCols, "_varIdx").select(*classCols, "Variable", *stats)


def load_home_equity(spark):
    # SAS: PROC IMPORT dbms=csv guessingrows=max -> spark.read.csv(inferSchema=True)
    # Empty CSV fields are read as null, the PySpark equivalent of SAS missing (.)
    return spark.read.csv(DATA_PATH, header=True, inferSchema=True)




def clean_home_equity(df):
    """Rebuild work.home_equity_final from sas/02_data_cleaning.sas (steps 1-3, 5)."""
    # SAS: DATA step IF/THEN -> when(); CITY = propcase(CITY) -> propcase()
    clean = df.withColumn("CITY", propcase("CITY")) \
        .withColumn(
            "LTV",
            F.when(
                F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
                F.col("MORTDUE") / F.col("VALUE"),
            ),
        ) \
        .withColumn(
            "LOAN_OUTCOME",
            F.when(F.col("BAD") == 0, F.lit("Paid")).when(F.col("BAD") == 1, F.lit("Default")),
        )
    # SAS: ARRAY num_vars / num_flags + DO loop -> loop over NUM_VARS
    for varName in NUM_VARS:
        clean = clean.withColumn(
            f"{varName}_MISS", F.when(F.col(varName).isNull(), 1).otherwise(0)
        )
    # SAS: if LOAN ne . and VALUE ne . and BAD ne .; if LTV > 0 and LTV < 5;
    #      if LOAN > 0; if VALUE > 0;  -> filter()
    return clean.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
        & (F.col("LTV") > 0) & (F.col("LTV") < 5)
        & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )



NUMERIC_EFFECTS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
# SAS: CLASS JOB(ref='Other') REASON(ref='HomeImp') / param=ref
CLASS_EFFECTS = {"JOB": "Other", "REASON": "HomeImp"}
MODEL_EFFECTS = NUMERIC_EFFECTS + list(CLASS_EFFECTS)
SAMPRATE = 0.7
SEED = 42
SLENTRY = 0.05
SLSTAY = 0.05
BINWIDTH = 0.002  # SAS default bin width for the Association statistics


def chi2_sf(x, df):
    """Upper-tail chi-square probability for integer df (no SciPy dependency)."""
    if x <= 0:
        return 1.0
    if df % 2 == 0:
        term, total = 1.0, 1.0
        for k in range(1, df // 2):
            term *= (x / 2) / k
            total += term
        return math.exp(-x / 2) * total
    p = math.erfc(math.sqrt(x / 2))
    term = math.sqrt(2 * x / math.pi) * math.exp(-x / 2)
    for k in range(1, (df - 1) // 2 + 1):
        p += term
        term *= x / (2 * k + 1)
    return p


def srs_split(df, rate, seed):
    """SAS: PROC SURVEYSELECT method=srs samprate=rate seed=seed + selected flag.

    Draws exactly ceil(rate * N) rows without replacement (randomSplit only
    gives an approximate size). SAS's random stream can't be reproduced, so the
    selected rows differ from a SAS run.
    """
    total = df.count()
    sampleSize = math.ceil(rate * total)
    ranked = df.coalesce(1) \
        .withColumn("_u", F.rand(seed)) \
        .withColumn("_rn", F.row_number().over(Window.orderBy("_u"))) \
        .withColumn("Selected", (F.col("_rn") <= sampleSize).cast("int")) \
        .drop("_u", "_rn")
    return ranked.filter(F.col("Selected") == 1), ranked.filter(F.col("Selected") == 0)


def reference_coding_stages(df):
    """SAS: CLASS var(ref='x') / param=ref -> StringIndexer + OneHotEncoder(dropLast).

    Levels are ordered alphabetically (SAS ORDER=FORMATTED) with the reference
    level moved last, so OneHotEncoder's dropped last slot is the SAS reference.
    """
    indexers, levelNames = [], {}
    for classCol, refLevel in CLASS_EFFECTS.items():
        levels = sorted(r[0] for r in df.select(classCol).distinct().collect()
                        if r[0] is not None and r[0] != refLevel)
        indexers.append(StringIndexerModel.from_labels(
            levels + [refLevel], inputCol=classCol, outputCol=f"{classCol}_IDX"))
        levelNames[classCol] = levels
    encoder = OneHotEncoder(
        inputCols=[f"{c}_IDX" for c in CLASS_EFFECTS],
        outputCols=[f"{c}_VEC" for c in CLASS_EFFECTS],
        dropLast=True,
    )
    return indexers, encoder, levelNames


def effect_column(effect):
    return f"{effect}_VEC" if effect in CLASS_EFFECTS else effect


def effect_features(effect, levelNames):
    if effect in CLASS_EFFECTS:
        return [f"{effect} {level}" for level in levelNames[effect]]
    return [effect]


def fit_glm(encoded, effects):
    """Unpenalised binomial/logit fit -> (model, -2 Log L)."""
    assembled = VectorAssembler(
        inputCols=[effect_column(e) for e in effects], outputCol="features"
    ).transform(encoded)
    model = GeneralizedLinearRegression(
        family="binomial", link="logit", labelCol="label", featuresCol="features",
        maxIter=100, regParam=0.0,
    ).fit(assembled)
    return model, model.summary.deviance


def stepwise_select(encoded, levelNames, interceptDev):
    """SAS: MODEL ... / selection=stepwise slentry=0.05 slstay=0.05 details.

    Each CLASS variable is one multi-df effect. Entry and stay both use
    likelihood-ratio chi-square tests (SAS uses score to enter, Wald to stay;
    the three are asymptotically equivalent).
    """
    dof = {e: len(effect_features(e, levelNames)) for e in MODEL_EFFECTS}
    selected, currentDev, lastRemoved = [], interceptDev, None
    for step in range(1, 2 * len(MODEL_EFFECTS) + 1):
        candidates = []
        for effect in [e for e in MODEL_EFFECTS if e not in selected]:
            _, dev = fit_glm(encoded, selected + [effect])
            chi = currentDev - dev
            candidates.append((effect, dof[effect], chi, chi2_sf(chi, dof[effect]), dev))
        if not candidates:
            break
        print(f"\nStep {step}: Analysis of Effects Eligible for Entry")
        for effect, df_, chi, p, _ in sorted(candidates, key=lambda c: c[3]):
            print(f"  {effect:8s} DF={df_}  LR Chi-Sq={chi:10.4f}  Pr > ChiSq={p:.4f}")
        best = min(candidates, key=lambda c: c[3])
        if best[3] >= SLENTRY or best[0] == lastRemoved:
            print("  No (additional) effects met the 0.05 significance level for entry.")
            break
        selected.append(best[0])
        currentDev = best[4]
        print(f"  Effect {best[0]} entered.")

        if len(selected) > 1:
            stay = []
            for effect in selected:
                _, dev = fit_glm(encoded, [e for e in selected if e != effect])
                chi = dev - currentDev
                stay.append((effect, chi2_sf(chi, dof[effect]), dev))
            worst = max(stay, key=lambda s: s[1])
            if worst[1] > SLSTAY:
                selected.remove(worst[0])
                currentDev, lastRemoved = worst[2], worst[0]
                print(f"  Effect {worst[0]} removed (Pr > ChiSq={worst[1]:.4f}).")
    return selected


def association_stats(scored, probCol="pred_prob", labelCol="label"):
    """SAS: ods output Association (concordant/discordant/tied, Somers' D, Gamma, Tau-a, c).

    Probabilities are binned to BINWIDTH like SAS; bins are small enough to
    count pairs on the driver.
    """
    bins = scored.withColumn("_bin", F.round(F.col(probCol) / BINWIDTH).cast("long")) \
        .groupBy("_bin") \
        .agg(F.sum(F.col(labelCol)).alias("n1"), F.sum(1 - F.col(labelCol)).alias("n0")) \
        .orderBy("_bin").collect()
    concordant = tied = 0.0
    nonEventsBelow = 0.0
    for row in bins:
        concordant += row["n1"] * nonEventsBelow
        tied += row["n1"] * row["n0"]
        nonEventsBelow += row["n0"]
    events = sum(r["n1"] for r in bins)
    pairs = events * nonEventsBelow
    discordant = pairs - concordant - tied
    n = events + nonEventsBelow
    return {
        "Percent Concordant": round(100 * concordant / pairs, 1),
        "Percent Discordant": round(100 * discordant / pairs, 1),
        "Percent Tied": round(100 * tied / pairs, 1),
        "Pairs": int(pairs),
        "Somers' D": round((concordant - discordant) / pairs, 3),
        "Gamma": round((concordant - discordant) / (concordant + discordant), 3),
        "Tau-a": round((concordant - discordant) / (0.5 * n * (n - 1)), 3),
        "c": round((concordant + 0.5 * tied) / pairs, 3),
    }


def logistic_pipeline(effects, indexers, encoder):
    """SAS: PROC LOGISTIC model -> Pipeline(StringIndexer, OneHotEncoder,
    VectorAssembler, LogisticRegression). regParam=0 gives the plain MLE."""
    return Pipeline(stages=indexers + [
        encoder,
        VectorAssembler(inputCols=[effect_column(e) for e in effects], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label",
                           maxIter=100, regParam=0.0, threshold=0.5),
    ])


def with_pred_prob(predictions):
    # SAS: predicted=pred_prob / ilink -> P(BAD=1) = probability[1]
    return predictions.withColumn("pred_prob", vector_to_array("probability")[1])


def main():
    # SAS: session start -> SparkSession
    spark = SparkSession.builder \
        .appName("HomeEquity_LogisticRegression") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # SAS: work.home_equity_risk (02 + 04). The 04 risk columns are not model
    # inputs and 04 filters no rows, so only the 02 cleaning is rebuilt here.
    homeEquityRisk = clean_home_equity(load_home_equity(spark))

    # ------------------------------------------------------------------
    # Step 1: Prepare modeling dataset
    # SAS equivalent:
    #   data work.model_data;
    #       set work.home_equity_risk;
    #       if LOAN ne . and MORTDUE ne . and VALUE ne .
    #          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
    #   run;
    # ------------------------------------------------------------------
    modelData = homeEquityRisk.filter(
        F.col("LOAN").isNotNull() & F.col("MORTDUE").isNotNull() & F.col("VALUE").isNotNull()
        & F.col("DEBTINC").isNotNull() & F.col("DELINQ").isNotNull() & F.col("CLAGE").isNotNull()
    ).withColumn("label", F.col("BAD").cast("double"))  # SAS: descending -> model P(BAD=1)

    # ------------------------------------------------------------------
    # Step 2: Split into training (70%) and validation (30%)
    # SAS equivalent:
    #   proc surveyselect data=work.model_data out=work.model_split
    #       method=srs samprate=0.7 seed=42;
    #   run;
    #   data work.train work.valid; ... if selected = 1 then output work.train; ...
    # ------------------------------------------------------------------
    train, valid = srs_split(modelData, SAMPRATE, SEED)
    train, valid = train.cache(), valid.cache()

    # SAS: PROC LOGISTIC drops rows with a missing value in any MODEL variable
    # (listwise deletion), so DEROG/NINQ/JOB/REASON nulls are removed here too.
    completeCase = F.lit(True)
    for effect in MODEL_EFFECTS:
        completeCase = completeCase & F.col(effect).isNotNull()
    trainUsed = train.filter(completeCase).cache()

    print_title("Modeling Data")
    print(f"  work.home_equity_risk : {homeEquityRisk.count()}")
    print(f"  work.model_data       : {modelData.count()}")
    print(f"  work.train            : {train.count()} read, {trainUsed.count()} used")
    print(f"  work.valid            : {valid.count()}")

    # ------------------------------------------------------------------
    # Step 3: Fit logistic regression with stepwise selection
    # SAS equivalent:
    #   proc logistic data=work.train descending;
    #       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    #       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON
    #             / selection=stepwise slentry=0.05 slstay=0.05 details lackfit;
    #       output out=work.train_scored predicted=pred_prob;
    #       store work.logit_model;
    #   run;
    # ------------------------------------------------------------------
    indexers, encoder, levelNames = reference_coding_stages(trainUsed)
    encodedTrain = Pipeline(stages=indexers + [encoder]).fit(trainUsed).transform(trainUsed).cache()

    events = trainUsed.filter(F.col("label") == 1).count()
    nonEvents = trainUsed.count() - events
    pEvent = events / (events + nonEvents)
    interceptDev = -2 * (events * math.log(pEvent) + nonEvents * math.log(1 - pEvent))

    print_title("Logistic Regression: Loan Default Prediction (stepwise selection)")
    selected = stepwise_select(encodedTrain, levelNames, interceptDev)
    print(f"\nSelected effects: {', '.join(selected)}")
    print(f"Not selected:     {', '.join(e for e in MODEL_EFFECTS if e not in selected)}")

    glm, finalDev = fit_glm(encodedTrain, selected)
    featureNames = [f for e in selected for f in effect_features(e, levelNames)]
    summary = glm.summary
    print_title("Model Fit Statistics")
    print(f"  -2 Log L  Intercept Only: {interceptDev:.3f}   Intercept and Covariates: {finalDev:.3f}")
    print(f"  AIC       Intercept Only: {interceptDev + 2:.3f}   "
          f"Intercept and Covariates: {finalDev + 2 * (len(featureNames) + 1):.3f}")

    # SAS: Analysis of Maximum Likelihood Estimates + Odds Ratio Estimates
    # GLM summary lists the intercept last.
    print_title("Analysis of Maximum Likelihood Estimates")
    names = featureNames + ["Intercept"]
    estimates = list(glm.coefficients) + [glm.intercept]
    print(f"  {'Parameter':16s} {'Estimate':>12s} {'Std Error':>11s} {'Wald ChiSq':>11s} "
          f"{'Pr > ChiSq':>11s} {'Odds Ratio':>11s}")
    for name, est, se, z, p in zip(names, estimates, summary.coefficientStandardErrors,
                                   summary.tValues, summary.pValues):
        oddsRatio = "" if name == "Intercept" else f"{math.exp(est):11.4f}"
        print(f"  {name:16s} {est:12.6f} {se:11.6f} {z * z:11.4f} {p:11.4f} {oddsRatio}")

    # SAS: store work.logit_model -> fitted PipelineModel
    logitModel = logistic_pipeline(selected, indexers, encoder).fit(trainUsed)

    # SAS: lackfit -> Hosmer-Lemeshow test over 10 groups (ntile deciles of pred_prob)
    trainScored = with_pred_prob(logitModel.transform(trainUsed))
    hlGroups = trainScored \
        .withColumn("_grp", F.ntile(10).over(Window.orderBy("pred_prob"))) \
        .groupBy("_grp") \
        .agg(F.count("*").alias("n"), F.sum("label").alias("obs"), F.sum("pred_prob").alias("exp")) \
        .collect()
    hlChi = sum((g["obs"] - g["exp"]) ** 2 / (g["exp"] * (1 - g["exp"] / g["n"])) for g in hlGroups)
    print_title("Hosmer and Lemeshow Goodness-of-Fit Test")
    print(f"  Chi-Square={hlChi:.4f}  DF={len(hlGroups) - 2}  "
          f"Pr > ChiSq={chi2_sf(hlChi, len(hlGroups) - 2):.4f}")

    # ------------------------------------------------------------------
    # Step 4: Score the validation dataset
    # SAS equivalent:
    #   proc plm restore=work.logit_model;
    #       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
    #   run;
    # ------------------------------------------------------------------
    # PLM leaves pred_prob missing when a selected predictor is missing.
    scorable = F.lit(True)
    for effect in selected:
        scorable = scorable & F.col(effect).isNotNull()
    scored = with_pred_prob(logitModel.transform(valid.filter(scorable))).select(*valid.columns, "pred_prob")
    unscored = valid.filter(~scorable).withColumn("pred_prob", F.lit(None).cast("double"))
    validScored = scored.unionByName(unscored)

    # ------------------------------------------------------------------
    # Step 5: Predicted classes and confusion matrix
    # SAS equivalent:
    #   data work.valid_scored; set work.valid_scored;
    #       if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
    #   run;
    #   proc freq data=work.valid_scored; tables BAD * PREDICTED_BAD / nopercent norow nocol; run;
    # ------------------------------------------------------------------
    # A missing pred_prob fails ">= 0.5" in SAS and falls to 0; when() with a
    # null condition also takes otherwise(0).
    validScored = validScored.withColumn(
        "PREDICTED_BAD", F.when(F.col("pred_prob") >= 0.5, 1).otherwise(0)
    ).cache()
    print_title("Confusion Matrix - Validation Set")
    print(f"  scored rows: {scored.count()}   missing pred_prob: {unscored.count()}")
    # SAS: PROC FREQ 2-way table -> groupBy().pivot().count()
    validScored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).count() \
        .na.fill(0).withColumnRenamed("0", "PREDICTED_BAD_0").withColumnRenamed("1", "PREDICTED_BAD_1") \
        .orderBy("BAD").show()

    holdoutAuc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(logitModel.transform(valid.filter(scorable)))
    print(f"Holdout AUC (stepwise model on work.valid): {holdoutAuc:.4f}")

    # ------------------------------------------------------------------
    # Step 6: Model performance - concordance and AUC
    # SAS equivalent:
    #   proc logistic data=work.valid descending;
    #       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    #       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON;
    #       roc;
    #       ods output Association=work.association_stats;
    #   run;
    # ------------------------------------------------------------------
    # As in the SAS source, this refits the full model (no selection) on work.valid.
    validUsed = valid.filter(completeCase)
    validIndexers, validEncoder, _ = reference_coding_stages(validUsed)
    validFit = with_pred_prob(
        logistic_pipeline(MODEL_EFFECTS, validIndexers, validEncoder).fit(validUsed).transform(validUsed)
    ).cache()
    associationStats = association_stats(validFit)
    rocAuc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(validFit)

    # ------------------------------------------------------------------
    # Step 7: Display key metrics
    # SAS equivalent:
    #   proc print data=work.association_stats; run;
    # ------------------------------------------------------------------
    print_title(f"Model Association Statistics (full model refit on work.valid, n={validFit.count()})")
    for name, value in associationStats.items():
        print(f"  {name:20s} {value}")
    print(f"  {'ROC AUC (unbinned)':20s} {rocAuc:.4f}")

    # ------------------------------------------------------------------
    # Step 8: Score distribution by actual outcome
    # SAS equivalent:
    #   proc means data=work.valid_scored n mean std min p25 median p75 max;
    #       class BAD;
    #       var pred_prob;
    #   run;
    # ------------------------------------------------------------------
    print_title("Predicted Probability Distribution by Actual Outcome")
    proc_means(
        validScored, ["pred_prob"],
        ["N", "MEAN", "STD", "MIN", "P25", "MEDIAN", "P75", "MAX"],
        classCols=["BAD"],
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
