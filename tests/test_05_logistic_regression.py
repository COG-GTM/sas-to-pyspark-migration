"""
Parity tests for pyspark/05_logistic_regression.py
(equivalent SAS program: sas/05_logistic_regression.sas).
"""

import importlib.util
import math
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(PROJECT_ROOT, "pyspark", "05_logistic_regression.py")
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")

_spec = importlib.util.spec_from_file_location("logreg05", SCRIPT)
logreg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(logreg)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("HomeEquity_LogReg_Tests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def model_data(spark):
    df = logreg.load_home_equity_risk(spark, DATA_PATH)
    return logreg.prepare_model_data(df) \
        .withColumn("label", F.col("BAD").cast("double")).cache()


@pytest.fixture(scope="module")
def fitted(model_data):
    _, train, valid = logreg.srs_split(model_data)
    design, design_cols = logreg.class_design(train)
    all_cols = logreg.effect_columns(logreg.MODEL_VARS, design_cols)
    train_design = design.transform(train)
    train_used = logreg.complete_cases(train_design, all_cols).cache()
    selected, history = logreg.stepwise_select(
        train_used, logreg.MODEL_VARS, design_cols, details=False
    )
    return {
        "train": train, "valid": valid, "design": design,
        "design_cols": design_cols, "train_design": train_design,
        "train_used": train_used, "selected": selected, "history": history,
    }


def test_chi2_sf_known_values():
    assert logreg.chi2_sf(3.841458820694124, 1) == pytest.approx(0.05, abs=1e-9)
    assert logreg.chi2_sf(11.070497693516351, 5) == pytest.approx(0.05, abs=1e-9)
    assert logreg.chi2_sf(0.5, 2) == pytest.approx(math.exp(-0.25), abs=1e-12)
    assert logreg.chi2_sf(0.0, 3) == 1.0


def test_model_data_complete_cases(model_data):
    # work.home_equity_final -> work.model_data
    assert model_data.count() == 3881
    for c in ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]:
        assert model_data.filter(F.col(c).isNull()).count() == 0


def test_srs_split_exact_size_disjoint_and_reproducible(model_data):
    model_split, train, valid = logreg.srs_split(model_data)
    n = model_data.count()
    assert train.count() == math.ceil(0.7 * n) == 2717
    assert valid.count() == n - train.count()
    assert model_split.filter("selected = 1").count() == train.count()
    assert train.intersect(valid).count() == 0
    _, train2, _ = logreg.srs_split(model_data)
    assert train.exceptAll(train2).count() == 0


def test_class_design_reference_coding(spark):
    df = spark.createDataFrame(
        [("Other", "HomeImp"), ("Mgr", "DebtCon"), ("Sales", "HomeImp"),
         (None, "DebtCon")],
        "JOB string, REASON string",
    )
    design, cols = logreg.class_design(df)
    assert cols == {"JOB": ["JOB_Mgr", "JOB_Sales"], "REASON": ["REASON_DebtCon"]}
    rows = {(r["JOB"], r["REASON"]): r for r in design.transform(df).collect()}
    ref = rows[("Other", "HomeImp")]
    assert (ref["JOB_Mgr"], ref["JOB_Sales"], ref["REASON_DebtCon"]) == (0.0, 0.0, 0.0)
    assert rows[("Mgr", "DebtCon")]["JOB_Mgr"] == 1.0
    assert rows[("Mgr", "DebtCon")]["REASON_DebtCon"] == 1.0
    assert rows[(None, "DebtCon")]["JOB_Mgr"] is None


def test_association_stats_hand_computed(spark):
    # events 0.9, 0.4 vs non-events 0.5, 0.4, 0.1 -> nc=4, nd=1, nt=1
    df = spark.createDataFrame(
        [(1.0, 0.9), (1.0, 0.4), (0.0, 0.5), (0.0, 0.4), (0.0, 0.1)],
        "label double, pred_prob double",
    )
    s = logreg.association_stats(df, binwidth=0)
    assert s["Pairs"] == 6
    assert s["Percent Concordant"] == pytest.approx(100 * 4 / 6)
    assert s["Percent Discordant"] == pytest.approx(100 * 1 / 6)
    assert s["Percent Tied"] == pytest.approx(100 * 1 / 6)
    assert s["c"] == pytest.approx(0.75)
    assert s["Somers' D"] == pytest.approx(0.5)
    assert s["Gamma"] == pytest.approx(0.6)
    assert s["Tau-a"] == pytest.approx(0.3)


def test_score_distribution_uses_sas_qntldef5(spark):
    rows = [(0, float(v)) for v in range(1, 11)] + \
           [(1, float(v)) for v in range(1, 5)] + [(1, None)]
    df = spark.createDataFrame(rows, "BAD int, pred_prob double")
    out = {r["BAD"]: r for r in logreg.score_distribution(df).collect()}
    assert (out[0]["25th Pctl"], out[0]["Median"], out[0]["75th Pctl"]) == (3.0, 5.5, 8.0)
    assert out[1]["N"] == 4
    assert (out[1]["25th Pctl"], out[1]["Median"], out[1]["75th Pctl"]) == (1.5, 2.5, 3.5)


def test_stepwise_selection(fitted):
    selected = fitted["selected"]
    for core in ["DELINQ", "DEBTINC", "DEROG"]:
        assert core in selected
    for weak in ["MORTDUE", "VALUE", "REASON"]:
        assert weak not in selected
    for action, _, _, _, p in fitted["history"]:
        if action == "entered":
            assert p < logreg.SLENTRY
    # every retained effect still meets slstay
    cols = logreg.effect_columns(selected, fitted["design_cols"])
    full_dev = logreg.deviance(fitted["train_used"], cols)
    for e in selected:
        rest = logreg.effect_columns([x for x in selected if x != e],
                                     fitted["design_cols"])
        dof = len(fitted["design_cols"].get(e, [e]))
        chi2 = logreg.deviance(fitted["train_used"], rest) - full_dev
        assert logreg.chi2_sf(chi2, dof) <= logreg.SLSTAY


def test_mllib_fit_matches_glm_mle_and_scores_holdout(fitted):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler

    cols = logreg.effect_columns(fitted["selected"], fitted["design_cols"])
    fit_rows = fitted["train_used"]
    lr = Pipeline(stages=[
        VectorAssembler(inputCols=cols, outputCol="features", handleInvalid="keep"),
        LogisticRegression(featuresCol="features", labelCol="label",
                           regParam=0.0, maxIter=100, tol=1e-10),
    ]).fit(fit_rows)
    glm = logreg.fit_glm(fit_rows, cols)
    lr_model = lr.stages[-1]
    assert lr_model.intercept == pytest.approx(glm.intercept, rel=1e-3)
    for a, b in zip(lr_model.coefficients, glm.coefficients):
        assert a == pytest.approx(b, rel=1e-3, abs=1e-7)

    model = logreg.compose_model(fitted["design"], lr)
    scored = logreg.with_pred_prob(model.transform(fitted["valid"]))
    assert scored.count() == fitted["valid"].count()
    probs = scored.filter(F.col("pred_prob").isNotNull())
    assert probs.filter((F.col("pred_prob") < 0) | (F.col("pred_prob") > 1)).count() == 0
    # rows missing a selected predictor are left unscored (PROC PLM behaviour)
    missing = logreg.complete_cases(fitted["valid"], fitted["selected"])
    assert probs.count() == missing.count()
    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="pred_prob"
    ).evaluate(probs)
    assert auc > 0.75
