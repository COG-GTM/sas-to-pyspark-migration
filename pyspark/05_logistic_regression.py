"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
Equivalent SAS Program: sas/05_logistic_regression.sas

  - PROC LOGISTIC with stepwise selection -> Spark ML Pipeline + L1 (lasso)
    LogisticRegression tuned by CrossValidator
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix

Self-contained: the upstream steps that build work.home_equity_risk
(sas/02_data_cleaning.sas and sas/04_risk_segmentation.sas) are replicated
inline so this script runs on its own.
"""

import math
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexerModel, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

SEED = 42
TRAIN_RATIO = 0.7
CUTOFF = 0.5

NUMERIC_PREDICTORS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
# SAS: class JOB(ref='Other') REASON(ref='HomeImp') / param=ref
CLASS_PREDICTORS = {"JOB": "Other", "REASON": "HomeImp"}
MODEL_VARS = NUMERIC_PREDICTORS + list(CLASS_PREDICTORS)


def banner(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


# ------------------------------------------------------------------
# Upstream helpers (replicate work.home_equity_final / work.home_equity_risk)
# ------------------------------------------------------------------
def clean_home_equity(df):
    """sas/02_data_cleaning.sas: derived columns + row filters."""
    # SAS: if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE; else LTV = .;
    df = df.withColumn(
        "LTV",
        when(col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
             col("MORTDUE") / col("VALUE")),
    )
    # SAS: IF/ELSE IF on BAD -> when().otherwise()
    df = df.withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")).otherwise(lit("")),
    )
    # SAS: subsetting IF (LOAN ne . and VALUE ne . and BAD ne .) -> filter(isNotNull)
    df = df.filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull())
    # SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;  (null LTV fails the IF)
    return df.filter((col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0))


def add_risk_segments(df):
    """sas/04_risk_segmentation.sas: composite risk score (null component contributes 0)."""
    ltv = when(col("LTV") >= 0.80, 3.0).when(col("LTV") >= 0.60, 1.5).otherwise(0.0)
    dti = (when(col("DEBTINC") >= 50, 3.0).when(col("DEBTINC") >= 40, 2.0)
           .when(col("DEBTINC") >= 30, 1.0).otherwise(0.0))
    delinq = (when(col("DELINQ") >= 4, 2.0).when(col("DELINQ") >= 2, 1.5)
              .when(col("DELINQ") == 1, 0.5).otherwise(0.0))
    derog = when(col("DEROG") >= 3, 2.0).when(col("DEROG") >= 1, 1.0).otherwise(0.0)
    df = df.withColumn("RISK_SCORE", ltv + dti + delinq + derog)
    # SAS: PROC FORMAT risk_score ranges -> when() chain
    return df.withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, "Low Risk").when(col("RISK_SCORE") < 5, "Medium Risk")
        .when(col("RISK_SCORE") < 7, "High Risk").otherwise("Very High Risk"),
    )


# ------------------------------------------------------------------
# Model helpers
# ------------------------------------------------------------------
def reference_coded_stages(df):
    """SAS: CLASS var(ref='x') / param=ref -> StringIndexer with the reference level
    indexed last + OneHotEncoder(dropLast=True), so the reference level is the
    all-zero dummy vector exactly as in SAS reference-cell coding."""
    stages, vec_cols, feature_names = [], [], list(NUMERIC_PREDICTORS)
    for name, ref in CLASS_PREDICTORS.items():
        levels = sorted(r[0] for r in df.select(name).distinct().collect() if r[0] is not None)
        non_ref = [lvl for lvl in levels if lvl != ref]
        stages.append(StringIndexerModel.from_labels(
            non_ref + [ref], inputCol=name, outputCol=f"{name}_IDX", handleInvalid="error"))
        stages.append(OneHotEncoder(inputCol=f"{name}_IDX", outputCol=f"{name}_VEC", dropLast=True))
        vec_cols.append(f"{name}_VEC")
        feature_names += [f"{name}={lvl}" for lvl in non_ref]
    return stages, vec_cols, feature_names


def with_pred_prob(scored):
    # SAS: predicted=pred_prob (descending -> P(BAD=1)) -> probability[1]
    return scored.withColumn("pred_prob", vector_to_array(col("probability"))[1])


def chi2_sf_even_df(x, df):
    """Chi-square survival function for even df (closed form, no scipy needed)."""
    half = x / 2.0
    return math.exp(-half) * sum(half ** k / math.factorial(k) for k in range(df // 2))


def hosmer_lemeshow(scored, groups=10):
    """SAS: MODEL ... / lackfit -> Hosmer-Lemeshow test over deciles of pred_prob."""
    w = Window.orderBy("pred_prob")
    deciles = (scored.withColumn("HL_GROUP", F.ntile(groups).over(w))
               .groupBy("HL_GROUP")
               .agg(F.count("*").alias("n"), F.sum("BAD").alias("obs"), F.sum("pred_prob").alias("exp"))
               .collect())
    stat = 0.0
    for r in deciles:
        exp1, exp0 = r["exp"], r["n"] - r["exp"]
        obs1, obs0 = r["obs"], r["n"] - r["obs"]
        stat += (obs1 - exp1) ** 2 / exp1 + (obs0 - exp0) ** 2 / exp0
    df = len(deciles) - 2
    return stat, df, chi2_sf_even_df(stat, df) if df % 2 == 0 else float("nan")


def association_stats(scored):
    """SAS: ODS OUTPUT Association -> concordant/discordant/tied pairs, Somers' D,
    Gamma, Tau-a and c, computed over all (event, non-event) pairs."""
    events = scored.filter(col("BAD") == 1).select(col("pred_prob").alias("p1"))
    nonevents = scored.filter(col("BAD") == 0).select(col("pred_prob").alias("p0"))
    pairs = events.crossJoin(nonevents).agg(
        F.sum(when(col("p1") > col("p0"), 1).otherwise(0)).alias("conc"),
        F.sum(when(col("p1") < col("p0"), 1).otherwise(0)).alias("disc"),
        F.count("*").alias("total"),
    ).collect()[0]
    n = scored.count()
    conc, disc, total = pairs["conc"], pairs["disc"], pairs["total"]
    tied = total - conc - disc
    return [
        ("Percent Concordant", 100.0 * conc / total),
        ("Percent Discordant", 100.0 * disc / total),
        ("Percent Tied", 100.0 * tied / total),
        ("Pairs", float(total)),
        ("Somers' D", (conc - disc) / total),
        ("Gamma", (conc - disc) / (conc + disc)),
        ("Tau-a", (conc - disc) / (0.5 * n * (n - 1))),
        ("c", (conc + 0.5 * tied) / total),
    ]


def main():
    spark = (SparkSession.builder
             .appName("HomeEquity_LogisticRegression")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # SAS: PROC IMPORT -> spark.read.csv (empty fields -> null, SAS missing '.')
    raw = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    home_equity_risk = add_risk_segments(clean_home_equity(raw))

    # ------------------------------------------------------------------
    # Step 1: Modeling dataset - complete cases for key predictors
    # SAS: if LOAN ne . and MORTDUE ne . and VALUE ne . and DEBTINC ne .
    #         and DELINQ ne . and CLAGE ne .;  -> filter(isNotNull)
    # ------------------------------------------------------------------
    key_cols = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]
    model_data = home_equity_risk.filter(
        F.expr(" AND ".join(f"{c} IS NOT NULL" for c in key_cols))
    ).withColumn("label", col("BAD").cast("double"))
    model_data.cache()

    # ------------------------------------------------------------------
    # Step 2: 70/30 train/validation split
    # SAS: PROC SURVEYSELECT method=srs samprate=0.7 seed=42 + DATA step OUTPUT
    #      -> randomSplit([0.7, 0.3], seed=42)
    # ------------------------------------------------------------------
    train_all, valid_all = model_data.randomSplit([TRAIN_RATIO, 1 - TRAIN_RATIO], seed=SEED)

    # SAS PROC LOGISTIC drops observations with a missing value in any MODEL
    # variable (listwise deletion), so DEROG/NINQ/JOB/REASON nulls are removed
    # here before fitting and scoring.
    complete = F.expr(" AND ".join(f"{c} IS NOT NULL" for c in MODEL_VARS))
    train = train_all.filter(complete).cache()
    valid = valid_all.filter(complete).cache()

    banner("Modeling Dataset")
    print(f"  Complete cases (Step 1):        {model_data.count()}")
    print(f"  Train (selected=1):             {train_all.count()}  -> {train.count()} used in fit")
    print(f"  Validation (selected=0):        {valid_all.count()}  -> {valid.count()} scored")

    # ------------------------------------------------------------------
    # Step 3: Fit logistic regression with variable selection
    # SAS: PROC LOGISTIC descending; CLASS ...; MODEL BAD = ... / selection=stepwise
    #      slentry=0.05 slstay=0.05 details lackfit;
    #   CLASS          -> StringIndexer + OneHotEncoder (reference coding)
    #   MODEL          -> VectorAssembler + LogisticRegression
    #   descending     -> label = BAD, model P(BAD=1)
    #   stepwise       -> L1 (lasso, elasticNetParam=1.0) regularization; regParam
    #                     chosen by 3-fold CrossValidator on AUC. Spark ML has no
    #                     p-value-based stepwise, so slentry/slstay have no direct
    #                     equivalent; predictors with non-zero coefficients are the
    #                     "selected" effects.
    # ------------------------------------------------------------------
    class_stages, vec_cols, feature_names = reference_coded_stages(train)
    assembler = VectorAssembler(inputCols=NUMERIC_PREDICTORS + vec_cols, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100,
                            elasticNetParam=1.0, standardization=True)
    pipeline = Pipeline(stages=class_stages + [assembler, lr])

    auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                             metricName="areaUnderROC")
    grid = ParamGridBuilder().addGrid(lr.regParam, [0.0005, 0.001, 0.005, 0.01, 0.02]).build()
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=auc_eval,
                        numFolds=3, seed=SEED)
    cv_model = cv.fit(train)
    model = cv_model.bestModel
    lr_model = model.stages[-1]

    banner("Logistic Regression: Loan Default Prediction")
    best_reg = lr_model.getRegParam()
    print(f"  Selection: L1 lasso, regParam={best_reg} (CV AUC={max(cv_model.avgMetrics):.4f})")
    print(f"  Intercept: {lr_model.intercept:.6f}")
    print(f"  {'Effect':20s} {'Estimate':>14s} {'Odds Ratio':>12s}  Selected")
    for name, beta in zip(feature_names, lr_model.coefficients.toArray()):
        print(f"  {name:20s} {beta:14.6g} {math.exp(beta):12.4f}  {'yes' if beta != 0 else 'no'}")

    # SAS: output out=work.train_scored predicted=pred_prob -> model.transform()
    train_scored = with_pred_prob(model.transform(train))
    hl_stat, hl_df, hl_p = hosmer_lemeshow(train_scored)
    print(f"\n  Hosmer-Lemeshow (lackfit): Chi-Sq={hl_stat:.4f}  DF={hl_df}  Pr>ChiSq={hl_p:.4f}")
    print(f"  Training AUC: {auc_eval.evaluate(train_scored):.4f}")

    # ------------------------------------------------------------------
    # Step 4: Score the validation dataset
    # SAS: PROC PLM restore=work.logit_model; score ... predicted=pred_prob / ilink;
    #      -> fitted PipelineModel.transform() (probability already on ilink scale)
    # ------------------------------------------------------------------
    valid_scored = with_pred_prob(model.transform(valid))

    # ------------------------------------------------------------------
    # Step 5: Predicted classes and confusion matrix
    # SAS: if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
    #      -> when().otherwise()
    # SAS: PROC FREQ tables BAD * PREDICTED_BAD -> groupBy().pivot().count()
    # ------------------------------------------------------------------
    valid_scored = valid_scored.withColumn(
        "PREDICTED_BAD", when(col("pred_prob") >= CUTOFF, 1).otherwise(0)
    ).cache()

    banner("Confusion Matrix - Validation Set")
    (valid_scored.groupBy("BAD").pivot("PREDICTED_BAD", [0, 1]).count()
     .na.fill(0).orderBy("BAD")
     .withColumnRenamed("0", "PREDICTED_BAD=0").withColumnRenamed("1", "PREDICTED_BAD=1")
     .show())
    cm = {(r["BAD"], r["PREDICTED_BAD"]): r["count"]
          for r in valid_scored.groupBy("BAD", "PREDICTED_BAD").count().collect()}
    tp, tn = cm.get((1, 1), 0), cm.get((0, 0), 0)
    fp, fn = cm.get((0, 1), 0), cm.get((1, 0), 0)
    total = tp + tn + fp + fn
    print(f"  Accuracy:    {(tp + tn) / total:.4f}")
    print(f"  Sensitivity: {tp / (tp + fn) if tp + fn else float('nan'):.4f}")
    print(f"  Specificity: {tn / (tn + fp) if tn + fp else float('nan'):.4f}")
    print(f"  Validation AUC (selected model): {auc_eval.evaluate(valid_scored):.4f}")

    # ------------------------------------------------------------------
    # Step 6: Model performance on the validation set
    # SAS: PROC LOGISTIC data=work.valid (full model, no selection); roc;
    #      ods output Association=work.association_stats;
    #      -> unregularized Pipeline refit on valid + BinaryClassificationEvaluator
    #         + pairwise concordance computed with a crossJoin
    # ------------------------------------------------------------------
    full_lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=0.0)
    valid_stages, valid_vecs, _ = reference_coded_stages(valid)
    full_model = Pipeline(stages=valid_stages + [
        VectorAssembler(inputCols=NUMERIC_PREDICTORS + valid_vecs, outputCol="features"), full_lr,
    ]).fit(valid)
    valid_full_scored = with_pred_prob(full_model.transform(valid))
    full_auc = auc_eval.evaluate(valid_full_scored)

    # ------------------------------------------------------------------
    # Step 7: Display key metrics
    # SAS: PROC PRINT data=work.association_stats -> createDataFrame().show()
    # ------------------------------------------------------------------
    banner("Model Association Statistics (validation, full model)")
    stats = association_stats(valid_full_scored) + [("ROC AUC", full_auc)]
    spark.createDataFrame(stats, ["Statistic", "Value"]).show(truncate=False)

    # ------------------------------------------------------------------
    # Step 8: Score distribution by actual outcome
    # SAS: PROC MEANS n mean std min p25 median p75 max; class BAD; var pred_prob;
    #      -> groupBy().agg() with percentile_approx for P25/median/P75
    # ------------------------------------------------------------------
    banner("Predicted Probability Distribution by Actual Outcome")
    p = col("pred_prob")
    (valid_scored.groupBy("BAD").agg(
        F.count(p).alias("N"),
        F.round(F.mean(p), 4).alias("Mean"),
        F.round(F.stddev(p), 4).alias("Std"),
        F.round(F.min(p), 4).alias("Min"),
        F.round(F.percentile_approx(p, 0.25), 4).alias("P25"),
        F.round(F.percentile_approx(p, 0.5), 4).alias("Median"),
        F.round(F.percentile_approx(p, 0.75), 4).alias("P75"),
        F.round(F.max(p), 4).alias("Max"),
    ).orderBy("BAD").show())

    spark.stop()


if __name__ == "__main__":
    main()
