"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
         - Stepwise selection (slentry=0.05, slstay=0.05)
         - Model evaluation (concordance, AUC)
         - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas

Run from the repository root:
    python pyspark/05_logistic_regression.py
"""

import math

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DATA_PATH = "data/home_equity.csv"

NUMERIC_EFFECTS = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"
]
# CLASS JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
CLASS_EFFECTS = {"JOB": "Other", "REASON": "HomeImp"}
MODEL_VARS = NUMERIC_EFFECTS + list(CLASS_EFFECTS)

SAMPRATE = 0.7
SEED = 42
SLENTRY = 0.05
SLSTAY = 0.05


# ------------------------------------------------------------------
# Upstream steps (self-contained): rebuild work.home_equity_risk
# SAS equivalent (sas/01, sas/02, sas/04):
#   proc import ... out=work.home_equity;
#   data work.home_equity_final;  /* LTV, filters */
#       if LOAN ne . and VALUE ne . and BAD ne .;
#       if LTV > 0 and LTV < 5;  if LOAN > 0;  if VALUE > 0;
#   run;
#   data work.home_equity_risk; set work.home_equity_final; ... run;
#
# Program 04 only adds risk-segment columns (no row filters) and none of
# them are model inputs, so only the 02 row filters affect this program.
# ------------------------------------------------------------------
def load_home_equity_risk(spark, path=DATA_PATH):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull()
            & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    )
    # SAS: a missing value in a comparison is false, Spark null -> row dropped
    return df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull()
        & F.col("BAD").isNotNull()
        & (F.col("LTV") > 0) & (F.col("LTV") < 5)
        & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
def prepare_model_data(df):
    keep = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]
    cond = F.lit(True)
    for c in keep:
        cond = cond & F.col(c).isNotNull()
    return df.filter(cond)


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
# METHOD=SRS draws exactly n = ceil(samprate * N) rows without
# replacement. randomSplit() is a per-row Bernoulli draw (size varies),
# so instead rank rows by a seeded hash of their content and take the
# first n. The hash is partition-independent, so the split is
# reproducible; it cannot reproduce SAS's random number stream.
# ------------------------------------------------------------------
def srs_split(df, samprate=SAMPRATE, seed=SEED):
    n_total = df.count()
    n_sample = math.ceil(samprate * n_total)
    rank_key = F.xxhash64(F.lit(seed), *[F.col(c) for c in df.columns])
    w = Window.orderBy(rank_key, *df.columns)
    model_split = df.withColumn(
        "selected", (F.row_number().over(w) <= n_sample).cast("int")
    )
    train = model_split.filter(F.col("selected") == 1).drop("selected")
    valid = model_split.filter(F.col("selected") == 0).drop("selected")
    return model_split, train, valid


# ------------------------------------------------------------------
# CLASS statement with param=ref (reference-cell coding)
# SAS equivalent:
#   class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#
# One 0/1 design column per non-reference level, levels sorted
# ascending like SAS. A missing class value stays missing (SAS treats it
# as a missing explanatory value, not as the reference level).
# Built as a SQLTransformer so it is a stage of the stored pipeline.
# ------------------------------------------------------------------
def class_design(train_df, class_effects=CLASS_EFFECTS):
    design_cols = {}
    exprs = []
    for var, ref in class_effects.items():
        levels = sorted(
            r[0] for r in train_df.select(var).distinct().collect()
            if r[0] is not None and r[0] != ref
        )
        design_cols[var] = []
        for lvl in levels:
            name = f"{var}_{lvl}"
            design_cols[var].append(name)
            exprs.append(
                f"CASE WHEN {var} IS NULL THEN NULL "
                f"WHEN {var} = '{lvl}' THEN 1.0 ELSE 0.0 END AS {name}"
            )
    stmt = "SELECT *, " + ", ".join(exprs) + " FROM __THIS__"
    return SQLTransformer(statement=stmt), design_cols


def effect_columns(effects, design_cols):
    cols = []
    for e in effects:
        cols.extend(design_cols.get(e, [e]))
    return cols


def complete_cases(df, cols):
    cond = F.lit(True)
    for c in cols:
        cond = cond & F.col(c).isNotNull()
    return df.filter(cond)


def chi2_sf(x, dof):
    """Upper-tail chi-square probability, Q(dof/2, x/2)."""
    if x <= 0:
        return 1.0
    a, z = dof / 2.0, x / 2.0
    gln = math.lgamma(a)
    if z < a + 1:
        term = total = 1.0 / a
        ap = a
        for _ in range(1000):
            ap += 1
            term *= z / ap
            total += term
            if abs(term) < abs(total) * 1e-15:
                break
        return max(0.0, 1.0 - total * math.exp(-z + a * math.log(z) - gln))
    b = z + 1 - a
    c = 1.0 / 1e-300
    d = 1.0 / b
    h = d
    for i in range(1, 1000):
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
    return math.exp(-z + a * math.log(z) - gln) * h


def intercept_only_deviance(df):
    row = df.agg(F.count("label").alias("n"), F.sum("label").alias("k")).first()
    n, k = row["n"], row["k"]
    if k in (0, n):
        return 0.0
    p = k / n
    return -2.0 * (k * math.log(p) + (n - k) * math.log(1 - p))


def fit_glm(df, cols):
    """Unpenalized binomial/logit GLM (IRLS) -> MLE with Wald statistics."""
    assembled = VectorAssembler(inputCols=cols, outputCol="features").transform(df)
    glr = GeneralizedLinearRegression(
        family="binomial", link="logit", labelCol="label",
        featuresCol="features", maxIter=50, tol=1e-8,
    )
    return glr.fit(assembled)


def deviance(df, cols):
    return fit_glm(df, cols).summary.deviance if cols else intercept_only_deviance(df)


# ------------------------------------------------------------------
# selection=stepwise slentry=0.05 slstay=0.05 details
#
# Spark MLlib has no stepwise selection, so it is implemented here on
# top of unpenalized GLM fits. Effects (a CLASS variable is one
# multi-df effect, as in SAS) enter/leave on likelihood-ratio
# chi-square tests. SAS uses the score chi-square for entry and the Wald
# chi-square for removal; all three are asymptotically equivalent, but
# borderline effects (p near 0.05) can be decided differently.
# ------------------------------------------------------------------
def stepwise_select(df, effects, design_cols, slentry=SLENTRY, slstay=SLSTAY,
                    details=True, max_steps=50):
    selected = []
    history = []
    current_dev = deviance(df, [])
    last_removed = None
    for step in range(1, max_steps + 1):
        candidates = []
        for e in effects:
            if e in selected:
                continue
            dev = deviance(df, effect_columns(selected + [e], design_cols))
            dof = len(design_cols.get(e, [e]))
            chi2 = current_dev - dev
            candidates.append((chi2_sf(chi2, dof), e, chi2, dof, dev))
        if not candidates:
            break
        candidates.sort()
        if details:
            print(f"\n  Step {step}: entry candidates (LR chi-square)")
            for p, e, chi2, dof, _ in candidates:
                print(f"    {e:10s} chi2={chi2:10.4f} df={dof}  p={p:.4g}")
        p, e, chi2, dof, dev = candidates[0]
        if p >= slentry or e == last_removed:
            break
        selected.append(e)
        current_dev = dev
        history.append(("entered", e, chi2, dof, p))
        print(f"  Step {step}: effect {e} entered (p={p:.4g})")

        while len(selected) > 1:
            removal = []
            for r in selected:
                rest = [x for x in selected if x != r]
                dev_r = deviance(df, effect_columns(rest, design_cols))
                dof_r = len(design_cols.get(r, [r]))
                chi2_r = dev_r - current_dev
                removal.append((chi2_sf(chi2_r, dof_r), r, chi2_r, dof_r, dev_r))
            p_r, r, chi2_r, dof_r, dev_r = max(removal)
            if p_r <= slstay:
                break
            selected.remove(r)
            current_dev = dev_r
            last_removed = r
            history.append(("removed", r, chi2_r, dof_r, p_r))
            print(f"  Step {step}: effect {r} removed (p={p_r:.4g})")
    return selected, history


def with_pred_prob(scored):
    """P(BAD=1) as pred_prob (SAS `descending`); NaN (missing input) -> null."""
    p = vector_to_array("probability")[1]
    return scored.withColumn("pred_prob", F.when(~F.isnan(p), p))


def compose_model(design_stage, fitted):
    """`store` equivalent: design stage + fitted assembler/model as one PipelineModel."""
    return PipelineModel(stages=[design_stage] + fitted.stages)


def hosmer_lemeshow(scored, groups=10):
    """lackfit: Hosmer-Lemeshow goodness-of-fit on decile groups."""
    w = Window.orderBy("pred_prob")
    g = scored.filter(F.col("pred_prob").isNotNull()) \
        .withColumn("grp", F.ntile(groups).over(w)) \
        .groupBy("grp").agg(
            F.count("*").alias("n"),
            F.sum("label").alias("obs1"),
            F.sum("pred_prob").alias("exp1"),
        ).collect()
    chi2 = 0.0
    for r in g:
        obs0, exp0 = r["n"] - r["obs1"], r["n"] - r["exp1"]
        chi2 += (r["obs1"] - r["exp1"]) ** 2 / r["exp1"]
        chi2 += (obs0 - exp0) ** 2 / exp0
    dof = len(g) - 2
    return chi2, dof, chi2_sf(chi2, dof)


# ------------------------------------------------------------------
# Association of Predicted Probabilities and Observed Responses
# SAS equivalent: ods output Association=work.association_stats;
#
# Every (event, non-event) pair is concordant if the event has the
# higher predicted probability. SAS bins probabilities (BINWIDTH=0.002
# by default) before counting; binwidth=0 gives exact values.
# ------------------------------------------------------------------
def association_stats(scored, binwidth=0.002):
    p = F.col("pred_prob")
    if binwidth:
        p = F.round(p / binwidth) * binwidth
    s = scored.filter(F.col("pred_prob").isNotNull()).withColumn("p", p)
    n = s.count()
    ev = s.filter(F.col("label") == 1).groupBy("p").agg(F.count("*").alias("ne"))
    nev = s.filter(F.col("label") == 0).groupBy("p").agg(F.count("*").alias("nn")) \
        .withColumnRenamed("p", "q")
    pairs = ev.crossJoin(nev).withColumn("w", F.col("ne") * F.col("nn"))
    agg = pairs.agg(
        F.sum(F.when(F.col("p") > F.col("q"), F.col("w")).otherwise(0)).alias("nc"),
        F.sum(F.when(F.col("p") < F.col("q"), F.col("w")).otherwise(0)).alias("nd"),
        F.sum(F.when(F.col("p") == F.col("q"), F.col("w")).otherwise(0)).alias("nt"),
        F.sum("w").alias("t"),
    ).first()
    nc, nd, nt, t = (int(agg[k] or 0) for k in ("nc", "nd", "nt", "t"))
    return {
        "Percent Concordant": 100.0 * nc / t,
        "Percent Discordant": 100.0 * nd / t,
        "Percent Tied": 100.0 * nt / t,
        "Pairs": t,
        "Somers' D": (nc - nd) / t,
        "Gamma": (nc - nd) / (nc + nd) if nc + nd else 0.0,
        "Tau-a": (nc - nd) / (0.5 * n * (n - 1)),
        "c": (nc + 0.5 * nt) / t,
    }


def _sas_quantile(arr, p):
    """PROC MEANS default percentile definition (QNTLDEF=5) on a sorted array."""
    n = F.size(arr)
    np_ = n * F.lit(p)
    j = F.floor(np_).cast("int")
    at = lambda i: F.element_at(arr, i)  # 1-based
    return F.when(
        np_ == j, (at(F.greatest(j, F.lit(1))) + at(j + 1)) / 2
    ).otherwise(at(j + 1))


# ------------------------------------------------------------------
# Step 8 helper: PROC MEANS n mean std min p25 median p75 max, class BAD
# Percentiles are exact and follow SAS's default QNTLDEF=5 rule
# (Spark's percentile_approx would only be approximate).
# ------------------------------------------------------------------
def score_distribution(scored):
    arr = F.sort_array(F.collect_list("pred_prob"))
    return scored.groupBy("BAD").agg(
        F.count("pred_prob").alias("N"),
        F.mean("pred_prob").alias("Mean"),
        F.stddev("pred_prob").alias("Std Dev"),
        F.min("pred_prob").alias("Minimum"),
        arr.alias("_v"),
        F.max("pred_prob").alias("Maximum"),
    ).select(
        "BAD", "N", "Mean", "Std Dev", "Minimum",
        _sas_quantile(F.col("_v"), 0.25).alias("25th Pctl"),
        _sas_quantile(F.col("_v"), 0.50).alias("Median"),
        _sas_quantile(F.col("_v"), 0.75).alias("75th Pctl"),
        "Maximum",
    ).orderBy("BAD")


def format_p(p):
    """SAS PVALUE6.4 style."""
    return "<.0001" if p < 1e-4 else f"{p:.4f}"


def banner(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main():
    # Initialize SparkSession
    # SAS equivalent: Starting a SAS session
    spark = SparkSession.builder \
        .appName("HomeEquity_LogisticRegression") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    homeEquityRisk = load_home_equity_risk(spark)
    print(f"work.home_equity_risk rows: {homeEquityRisk.count()}")

    # Step 1
    modelData = prepare_model_data(homeEquityRisk) \
        .withColumn("label", F.col("BAD").cast("double")).cache()
    banner("Step 1: Modeling dataset (equivalent to work.model_data)")
    print(f"Rows: {modelData.count()}")

    # Step 2
    _, train, valid = srs_split(modelData)
    train, valid = train.cache(), valid.cache()
    banner("Step 2: SRS split (equivalent to PROC SURVEYSELECT samprate=0.7)")
    print(f"Training set:   {train.count()} rows")
    print(f"Validation set: {valid.count()} rows")

    # ------------------------------------------------------------------
    # Step 3: Fit logistic regression with stepwise selection
    # SAS equivalent:
    #   proc logistic data=work.train descending;
    #       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    #       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
    #                   JOB REASON
    #             / selection=stepwise slentry=0.05 slstay=0.05
    #               details lackfit;
    #       output out=work.train_scored predicted=pred_prob;
    #       store work.logit_model;
    #   run;
    #
    # `descending` -> model P(BAD=1). PROC LOGISTIC drops rows with a
    # missing value in any MODEL variable (listwise deletion), so the
    # fit uses complete cases on all ten effects.
    # ------------------------------------------------------------------
    banner("Step 3: Logistic Regression: Loan Default Prediction")
    designStage, designCols = class_design(train)
    allCols = effect_columns(MODEL_VARS, designCols)
    trainDesign = designStage.transform(train)
    trainUsed = complete_cases(trainDesign, allCols).cache()
    print(f"Number of Observations Read: {train.count()}")
    print(f"Number of Observations Used: {trainUsed.count()}")
    print("Class level design columns (param=ref):")
    for var, cols in designCols.items():
        print(f"  {var} (ref='{CLASS_EFFECTS[var]}'): {cols}")

    selectedEffects, _ = stepwise_select(trainUsed, MODEL_VARS, designCols)
    selectedCols = effect_columns(selectedEffects, designCols)
    print(f"\nSelected effects: {selectedEffects}")

    glm = fit_glm(trainUsed, selectedCols)
    s = glm.summary
    print("\nModel Fit Statistics")
    nullDev = intercept_only_deviance(trainUsed)
    print(f"  -2 Log L  intercept only: {nullDev:.3f}  "
          f"intercept and covariates: {s.deviance:.3f}")
    print(f"  AIC: {s.aic:.3f}")
    print("\nAnalysis of Maximum Likelihood Estimates")
    print(f"  {'Parameter':12s} {'Estimate':>12s} {'Std Error':>11s} "
          f"{'Wald ChiSq':>11s} {'Pr>ChiSq':>9s} {'OddsRatio':>10s}")
    names = selectedCols + ["Intercept"]
    estimates = list(glm.coefficients) + [glm.intercept]
    for nm, est, se, t, p in zip(names, estimates,
                                 s.coefficientStandardErrors, s.tValues, s.pValues):
        odds = f"{math.exp(est):10.4f}" if nm != "Intercept" else ""
        print(f"  {nm:12s} {est:12.6g} {se:11.6g} {t * t:11.4f} "
              f"{format_p(p):>9s} {odds}")

    logitModel = compose_model(designStage, Pipeline(stages=[
        VectorAssembler(inputCols=selectedCols, outputCol="features",
                        handleInvalid="keep"),
        LogisticRegression(featuresCol="features", labelCol="label",
                           regParam=0.0, maxIter=100, tol=1e-10),
    ]).fit(complete_cases(trainDesign, selectedCols)))

    trainScored = with_pred_prob(logitModel.transform(train))
    hlChi2, hlDf, hlP = hosmer_lemeshow(complete_cases(trainScored, allCols))
    print("\nHosmer and Lemeshow Goodness-of-Fit Test (lackfit)")
    print(f"  Chi-Square={hlChi2:.4f}  DF={hlDf}  Pr>ChiSq={hlP:.4f}")

    # ------------------------------------------------------------------
    # Step 4: Score the validation dataset
    # SAS equivalent:
    #   proc plm restore=work.logit_model;
    #       score data=work.valid out=work.valid_scored
    #             predicted=pred_prob / ilink;
    #   run;
    #
    # The stored PipelineModel (design stage + assembler + model) is the
    # item store. Rows missing a selected predictor get pred_prob = null.
    # ------------------------------------------------------------------
    validScored = with_pred_prob(logitModel.transform(valid))
    banner("Step 4: Validation scoring (equivalent to PROC PLM SCORE)")
    unscored = validScored.filter(F.col("pred_prob").isNull()).count()
    print(f"Scored rows: {validScored.count() - unscored}  "
          f"(missing pred_prob: {unscored})")

    # ------------------------------------------------------------------
    # Step 5: Create predicted classes and confusion matrix
    # SAS equivalent:
    #   data work.valid_scored; set work.valid_scored;
    #       if pred_prob >= 0.5 then PREDICTED_BAD = 1;
    #       else PREDICTED_BAD = 0;
    #   run;
    #   proc freq data=work.valid_scored;
    #       tables BAD * PREDICTED_BAD / nopercent norow nocol;
    #   run;
    #
    # As in SAS, a missing pred_prob falls to the ELSE branch (0).
    # ------------------------------------------------------------------
    validScored = validScored.withColumn(
        "PREDICTED_BAD",
        F.when(F.col("pred_prob") >= 0.5, 1).otherwise(0),
    ).cache()
    banner("Step 5: Confusion Matrix - Validation Set")
    confusion = validScored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]) \
        .count().na.fill(0).orderBy("BAD") \
        .withColumn("BAD", F.col("BAD").cast("string")) \
        .withColumn("Total", F.col("0") + F.col("1"))
    totals = confusion.agg(*[F.sum(c).alias(c) for c in ["0", "1", "Total"]]) \
        .select(F.lit("Total").alias("BAD"), "0", "1", "Total")
    confusion.unionByName(totals).withColumnRenamed("BAD", "BAD \\ PREDICTED_BAD") \
        .show(truncate=False)
    cm = {(r["BAD"], r["PREDICTED_BAD"]): r["count"]
          for r in validScored.groupBy("BAD", "PREDICTED_BAD").count().collect()}
    tp, tn = cm.get((1, 1), 0), cm.get((0, 0), 0)
    fp, fn = cm.get((0, 1), 0), cm.get((1, 0), 0)
    print(f"Accuracy={(tp + tn) / (tp + tn + fp + fn):.4f}  "
          f"Sensitivity={tp / max(tp + fn, 1):.4f}  "
          f"Specificity={tn / max(tn + fp, 1):.4f}")
    holdoutAuc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="pred_prob", metricName="areaUnderROC"
    ).evaluate(validScored.filter(F.col("pred_prob").isNotNull()))
    print(f"Holdout AUC of the stepwise model: {holdoutAuc:.4f}")

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
    # Note: as in the SAS source, this refits the full (no selection)
    # model on the validation data itself.
    # ------------------------------------------------------------------
    banner("Step 6: Model Performance - Concordance and AUC")
    validDesign = complete_cases(designStage.transform(valid), allCols)
    fullValidModel = Pipeline(stages=[
        VectorAssembler(inputCols=allCols, outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label",
                           regParam=0.0, maxIter=100, tol=1e-10),
    ]).fit(validDesign)
    validRefit = with_pred_prob(fullValidModel.transform(validDesign))
    associationStats = association_stats(validRefit)
    rocAuc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="pred_prob", metricName="areaUnderROC"
    ).evaluate(validRefit)

    # ------------------------------------------------------------------
    # Step 7: Display key metrics
    # SAS equivalent:
    #   proc print data=work.association_stats; run;
    # ------------------------------------------------------------------
    print(f"Full model refit on validation, observations used: {validDesign.count()}")
    banner("Step 7: Model Association Statistics")
    spark.createDataFrame(
        [(k, float(v)) for k, v in associationStats.items()], ["Statistic", "Value"]
    ).show(truncate=False)
    print(f"ROC AUC (unbinned, `roc` statement): {rocAuc:.4f}")

    # ------------------------------------------------------------------
    # Step 8: Score distribution by actual outcome
    # SAS equivalent:
    #   proc means data=work.valid_scored n mean std min p25 median p75 max;
    #       class BAD;
    #       var pred_prob;
    #   run;
    # ------------------------------------------------------------------
    banner("Step 8: Predicted Probability Distribution by Actual Outcome")
    score_distribution(validScored).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
