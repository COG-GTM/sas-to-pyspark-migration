"""Generate the SAS-parity golden/expected files under ``tests/expected/``.

No live SAS environment is available, so the expected values are derived by
faithfully replicating the logic of the programs in ``sas/*.sas`` with PySpark
(the SAS DATA-step / PROC logic, not the current ``pyspark/*.py`` scripts, which
have known divergences the later per-stage PRs will fix). Re-run from the repo
root with:

    python tests/expected/generate_expected.py

Each emitted JSON file records the exact filters/derivations it came from so the
values are reproducible and auditable. See ``tests/expected/README.md`` for the
per-file provenance and the tolerances that apply to the stage-5 model metrics.
"""

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

EXPECTED_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EXPECTED_DIR.parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "home_equity.csv"

ROUND = 6


def _round(value):
    return round(float(value), ROUND) if value is not None else None


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GenerateExpected")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_raw(spark: SparkSession) -> DataFrame:
    return spark.read.csv(str(DATA_PATH), header=True, inferSchema=True)


# ----------------------------------------------------------------------
# Shared SAS pipeline (sas/02_data_cleaning.sas)
# ----------------------------------------------------------------------
def with_derived(df: DataFrame) -> DataFrame:
    """Replicate the derived columns from the sas/02 DATA step."""
    return df.withColumn(
        "LTV",
        F.when(
            F.col("VALUE").isNotNull() & F.col("MORTDUE").isNotNull() & (F.col("VALUE") > 0),
            F.col("MORTDUE") / F.col("VALUE"),
        ),
    ).withColumn(
        "LOAN_OUTCOME",
        F.when(F.col("BAD") == 0, F.lit("Paid")).when(F.col("BAD") == 1, F.lit("Default")),
    )


def filtered(df: DataFrame) -> DataFrame:
    """sas/02 work.home_equity_filtered: LOAN, VALUE, BAD non-missing."""
    return df.filter(
        F.col("LOAN").isNotNull() & F.col("VALUE").isNotNull() & F.col("BAD").isNotNull()
    )


def final(df: DataFrame) -> DataFrame:
    """sas/02 work.home_equity_final: 0 < LTV < 5, LOAN > 0, VALUE > 0.

    In SAS a missing LTV fails ``LTV > 0`` (missing sorts below 0), so those
    rows are dropped; the null-safe Spark comparison drops them identically.
    """
    return df.filter(
        (F.col("LTV") > 0) & (F.col("LTV") < 5) & (F.col("LOAN") > 0) & (F.col("VALUE") > 0)
    )


def risk(df: DataFrame) -> DataFrame:
    """Replicate risk columns from sas/04_risk_segmentation.sas."""
    df = (
        df.withColumn(
            "LTV_RISK_CAT",
            F.when(F.col("LTV").isNull(), F.lit(None))
            .when(F.col("LTV") < 0.60, F.lit("Low"))
            .when(F.col("LTV") < 0.80, F.lit("Medium"))
            .otherwise(F.lit("High")),
        )
        .withColumn(
            "DTI_RISK_CAT",
            F.when(F.col("DEBTINC").isNull(), F.lit(None))
            .when(F.col("DEBTINC") < 30, F.lit("Low"))
            .when(F.col("DEBTINC") < 40, F.lit("Medium"))
            .when(F.col("DEBTINC") < 50, F.lit("High"))
            .otherwise(F.lit("Very High")),
        )
        .withColumn(
            "DELINQ_RISK_CAT",
            F.when(F.col("DELINQ").isNull(), F.lit(None))
            .when(F.col("DELINQ") == 0, F.lit("None"))
            .when(F.col("DELINQ") == 1, F.lit("Low"))
            .when(F.col("DELINQ") <= 3, F.lit("Medium"))
            .otherwise(F.lit("High")),
        )
    )

    ltv_score = (
        F.when(F.col("LTV").isNull(), F.lit(0.0))
        .when(F.col("LTV") >= 0.80, F.lit(3.0))
        .when(F.col("LTV") >= 0.60, F.lit(1.5))
        .otherwise(F.lit(0.0))
    )
    dti_score = (
        F.when(F.col("DEBTINC").isNull(), F.lit(0.0))
        .when(F.col("DEBTINC") >= 50, F.lit(3.0))
        .when(F.col("DEBTINC") >= 40, F.lit(2.0))
        .when(F.col("DEBTINC") >= 30, F.lit(1.0))
        .otherwise(F.lit(0.0))
    )
    delinq_score = (
        F.when(F.col("DELINQ").isNull(), F.lit(0.0))
        .when(F.col("DELINQ") >= 4, F.lit(2.0))
        .when(F.col("DELINQ") >= 2, F.lit(1.5))
        .when(F.col("DELINQ") == 1, F.lit(0.5))
        .otherwise(F.lit(0.0))
    )
    derog_score = (
        F.when(F.col("DEROG").isNull(), F.lit(0.0))
        .when(F.col("DEROG") >= 3, F.lit(2.0))
        .when(F.col("DEROG") >= 1, F.lit(1.0))
        .otherwise(F.lit(0.0))
    )

    return df.withColumn(
        "RISK_SCORE", ltv_score + dti_score + delinq_score + derog_score
    ).withColumn(
        "RISK_SEGMENT",
        F.when(F.col("RISK_SCORE") < 3, F.lit("Low Risk"))
        .when(F.col("RISK_SCORE") < 5, F.lit("Medium Risk"))
        .when(F.col("RISK_SCORE") < 7, F.lit("High Risk"))
        .otherwise(F.lit("Very High Risk")),
    )


# ----------------------------------------------------------------------
# Stage collectors
# ----------------------------------------------------------------------
def stage1(raw: DataFrame) -> dict:
    return {
        "row_count": raw.count(),
        "column_count": len(raw.columns),
        "columns": [
            {"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
            for f in raw.schema.fields
        ],
    }


def _group_counts(df: DataFrame, column: str) -> list:
    rows = (
        df.groupBy(column)
        .count()
        .orderBy(F.col("count").desc(), F.col(column).asc_nulls_last())
        .collect()
    )
    return [
        {"value": r[column] if r[column] is not None else "(missing)", "count": r["count"]}
        for r in rows
    ]


def stage2(raw: DataFrame, df_final: DataFrame) -> dict:
    df_derived = with_derived(raw)
    df_filtered = filtered(df_derived)

    sample_rows = (
        df_derived.filter(F.col("LTV").isNotNull())
        .select("MORTDUE", "VALUE", "LTV")
        .orderBy("MORTDUE", "VALUE")
        .limit(10)
        .collect()
    )
    return {
        "counts": {
            "raw": raw.count(),
            "after_filter_loan_value_bad_notnull": df_filtered.count(),
            "final_after_ltv_loan_value_positive": df_final.count(),
        },
        "loan_outcome_counts": _group_counts(df_final, "LOAN_OUTCOME"),
        "sample_ltv": [
            {
                "MORTDUE": _round(r["MORTDUE"]),
                "VALUE": _round(r["VALUE"]),
                "LTV": _round(r["LTV"]),
            }
            for r in sample_rows
        ],
    }


def _summary(df: DataFrame, group_cols: list, value_cols: list) -> list:
    aggs = []
    for c in value_cols:
        aggs += [
            F.count(F.col(c)).alias(f"{c}__n"),
            F.mean(F.col(c)).alias(f"{c}__mean"),
            F.stddev(F.col(c)).alias(f"{c}__std"),
            F.min(F.col(c)).alias(f"{c}__min"),
            F.expr(f"percentile({c}, 0.5)").alias(f"{c}__median"),
            F.max(F.col(c)).alias(f"{c}__max"),
        ]
    rows = df.groupBy(*group_cols).agg(*aggs).orderBy(*group_cols).collect()
    out = []
    for r in rows:
        entry = {gc: r[gc] for gc in group_cols}
        entry["stats"] = {
            c: {
                "n": r[f"{c}__n"],
                "mean": _round(r[f"{c}__mean"]),
                "std": _round(r[f"{c}__std"]),
                "min": _round(r[f"{c}__min"]),
                "median": _round(r[f"{c}__median"]),
                "max": _round(r[f"{c}__max"]),
            }
            for c in value_cols
        }
        out.append(entry)
    return out


def stage3(df_final: DataFrame) -> dict:
    top_states = (
        df_final.groupBy("STATE")
        .agg(
            F.count("*").alias("num_loans"),
            F.mean("LOAN").alias("avg_loan"),
            F.mean("VALUE").alias("avg_property_value"),
            F.mean("BAD").alias("default_rate"),
        )
        .filter(F.col("num_loans") >= 10)
        .orderBy(F.col("avg_loan").desc())
        .limit(10)
        .collect()
    )
    return {
        "top_10_states_by_avg_loan": [
            {
                "STATE": r["STATE"],
                "num_loans": r["num_loans"],
                "avg_loan": _round(r["avg_loan"]),
                "avg_property_value": _round(r["avg_property_value"]),
                "default_rate": _round(r["default_rate"]),
            }
            for r in top_states
        ],
        "summary_by_loan_outcome": _summary(
            df_final, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]
        ),
        "summary_by_reason_outcome": _summary(
            df_final, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"]
        ),
        "frequency_tables": {
            col: _group_counts(df_final, col) for col in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]
        },
    }


def stage4(df_risk: DataFrame) -> dict:
    score_bounds = df_risk.agg(
        F.min("RISK_SCORE").alias("min"), F.max("RISK_SCORE").alias("max")
    ).collect()[0]
    default_by_segment = (
        df_risk.groupBy("RISK_SEGMENT")
        .agg(
            F.count("*").alias("n"),
            F.mean("BAD").alias("default_rate"),
            F.mean("LOAN").alias("avg_loan"),
            F.mean("LTV").alias("avg_ltv"),
            F.mean("DEBTINC").alias("avg_debtinc"),
        )
        .orderBy("RISK_SEGMENT")
        .collect()
    )
    return {
        "risk_score_range": {
            "min": _round(score_bounds["min"]),
            "max": _round(score_bounds["max"]),
        },
        "risk_segment_counts": _group_counts(df_risk, "RISK_SEGMENT"),
        "ltv_risk_cat_counts": _group_counts(df_risk, "LTV_RISK_CAT"),
        "dti_risk_cat_counts": _group_counts(df_risk, "DTI_RISK_CAT"),
        "delinq_risk_cat_counts": _group_counts(df_risk, "DELINQ_RISK_CAT"),
        "default_rate_by_segment": [
            {
                "RISK_SEGMENT": r["RISK_SEGMENT"],
                "n": r["n"],
                "default_rate": _round(r["default_rate"]),
                "avg_loan": _round(r["avg_loan"]),
                "avg_ltv": _round(r["avg_ltv"]),
                "avg_debtinc": _round(r["avg_debtinc"]),
            }
            for r in default_by_segment
        ],
    }


def stage5(df_risk: DataFrame) -> dict:
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

    # sas/05 work.model_data: complete cases on the 6 model-critical vars.
    sas_model_data = df_risk.filter(
        F.col("LOAN").isNotNull()
        & F.col("MORTDUE").isNotNull()
        & F.col("VALUE").isNotNull()
        & F.col("DEBTINC").isNotNull()
        & F.col("DELINQ").isNotNull()
        & F.col("CLAGE").isNotNull()
    )

    # PySpark reference model additionally needs categorical predictors present.
    model_data = sas_model_data.filter(
        F.col("DEROG").isNotNull()
        & F.col("NINQ").isNotNull()
        & F.col("JOB").isNotNull()
        & F.col("REASON").isNotNull()
    ).withColumn("label", F.col("BAD").cast("double"))

    train, valid = model_data.randomSplit([0.7, 0.3], seed=42)

    job_idx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
    reason_idx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
    job_enc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
    reason_enc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")
    assembler = VectorAssembler(
        inputCols=[
            "LOAN",
            "MORTDUE",
            "VALUE",
            "DEBTINC",
            "DELINQ",
            "DEROG",
            "CLAGE",
            "NINQ",
            "JOB_VEC",
            "REASON_VEC",
        ],
        outputCol="features",
    )
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.8,
    )
    model = Pipeline(stages=[job_idx, reason_idx, job_enc, reason_enc, assembler, lr]).fit(train)
    predictions = model.transform(valid)

    auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(predictions)

    cm_rows = (
        predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").collect()
    )
    confusion_matrix = {
        f"actual_{int(r['label'])}_pred_{int(r['prediction'])}": r["count"] for r in cm_rows
    }

    return {
        "sas_model_data_count": sas_model_data.count(),
        "pyspark_reference_model_data_count": model_data.count(),
        "train_count": train.count(),
        "valid_count": valid.count(),
        "reference_auc": _round(auc),
        "reference_confusion_matrix": confusion_matrix,
        "tolerances": {
            "auc_abs": 0.05,
            "note": (
                "SAS SELECTION=STEPWISE maps to PySpark elasticNet, so metrics "
                "are not bit-identical. AUC within +/-0.05; confusion-matrix "
                "cell counts are approximate (depend on split/Spark version)."
            ),
        },
    }


def write_json(name: str, payload: dict) -> None:
    path = EXPECTED_DIR / name
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
        fh.write("\n")
    print(f"wrote {path.relative_to(PROJECT_ROOT)}")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        raw = load_raw(spark).cache()
        df_final = final(with_derived(raw)).cache()
        df_risk = risk(df_final).cache()

        write_json("stage1_data_loading.json", stage1(raw))
        write_json("stage2_data_cleaning.json", stage2(raw, df_final))
        write_json("stage3_aggregation_reporting.json", stage3(df_final))
        write_json("stage4_risk_segmentation.json", stage4(df_risk))
        write_json("stage5_logistic_regression.json", stage5(df_risk))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
