"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - PROC LOGISTIC -> pyspark.ml LogisticRegression in a Pipeline
  - CLASS statement -> StringIndexer + OneHotEncoder
  - Model evaluation (confusion matrix, AUC)

Migrated from: sas/05_logistic_regression.sas

Run with: python pyspark/05_logistic_regression.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, mean, stddev, min as smin, max as smax, percentile_approx
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# Median via percentile_approx (works on PySpark 3.1+; functions.median needs 3.4+).
def median(c):
    return percentile_approx(col(c), 0.5)


def get_data_path():
    """Resolve data/home_equity.csv relative to the project root."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "home_equity.csv")


def load_clean(spark):
    """Load CSV and apply the cleaning transformations from script 02."""
    df = spark.read.csv(get_data_path(), header=True, inferSchema=True)

    df = df.withColumn(
        "LTV",
        when(
            col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ).otherwise(lit(None)),
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
    )

    df = df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    ).filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )
    return df


def add_risk_columns(df):
    """Replicate the SAS risk segmentation from script 04."""
    ltv_score = (
        when(col("LTV").isNull(), lit(0))
        .when(col("LTV") >= 0.80, lit(3))
        .when(col("LTV") >= 0.60, lit(1.5))
        .otherwise(lit(0))
    )
    dti_score = (
        when(col("DEBTINC").isNull(), lit(0))
        .when(col("DEBTINC") >= 50, lit(3))
        .when(col("DEBTINC") >= 40, lit(2))
        .when(col("DEBTINC") >= 30, lit(1))
        .otherwise(lit(0))
    )
    delinq_score = (
        when(col("DELINQ").isNull(), lit(0))
        .when(col("DELINQ") >= 4, lit(2))
        .when(col("DELINQ") >= 2, lit(1.5))
        .when(col("DELINQ") == 1, lit(0.5))
        .otherwise(lit(0))
    )
    derog_score = (
        when(col("DEROG").isNull(), lit(0))
        .when(col("DEROG") >= 3, lit(2))
        .when(col("DEROG") >= 1, lit(1))
        .otherwise(lit(0))
    )
    return df.withColumn(
        "RISK_SCORE", ltv_score + dti_score + delinq_score + derog_score
    ).withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk")),
    )


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_LogisticRegression") \
        .master("local[*]") \
        .getOrCreate()

    df = add_risk_columns(load_clean(spark))

    # Step 1: Prepare modeling dataset - complete cases only.
    # SAS: DATA work.model_data; if LOAN ne . and ... ;
    model_data = df.filter(
        col("LOAN").isNotNull()
        & col("MORTDUE").isNotNull()
        & col("VALUE").isNotNull()
        & col("DEBTINC").isNotNull()
        & col("DELINQ").isNotNull()
        & col("CLAGE").isNotNull()
        & col("DEROG").isNotNull()
        & col("NINQ").isNotNull()
        & col("JOB").isNotNull()
        & col("REASON").isNotNull()
    ).withColumn("label", col("BAD").cast("double"))

    print(f"Complete cases for modeling: {model_data.count()}")

    # Step 2: Train/test split (70/30).
    # SAS: PROC SURVEYSELECT samprate=0.7 seed=42 -> randomSplit.
    train, valid = model_data.randomSplit([0.7, 0.3], seed=42)

    # Step 3: Build the ML pipeline.
    # SAS: CLASS JOB REASON -> StringIndexer + OneHotEncoder.
    job_idx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
    reason_idx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
    job_enc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
    reason_enc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

    # SAS: MODEL statement predictors -> VectorAssembler.
    assembler = VectorAssembler(
        inputCols=[
            "LOAN", "MORTDUE", "VALUE", "DEBTINC",
            "DELINQ", "DEROG", "CLAGE", "NINQ",
            "JOB_VEC", "REASON_VEC",
        ],
        outputCol="features",
    )

    # SAS: PROC LOGISTIC (stepwise) -> LogisticRegression with regularization.
    lr = LogisticRegression(
        featuresCol="features", labelCol="label", maxIter=50, regParam=0.01
    )

    pipeline = Pipeline(stages=[job_idx, reason_idx, job_enc, reason_enc, assembler, lr])

    # Step 4: Fit on training, score validation.
    # SAS: output out=... predicted=; PROC PLM score ... -> model.transform().
    model = pipeline.fit(train)
    predictions = model.transform(valid)

    # Step 5: Predicted classes and confusion matrix.
    # LogisticRegression already produces a 0/1 'prediction' column.
    predictions = predictions.withColumn("PREDICTED_BAD", col("prediction").cast("int"))

    print("\nConfusion Matrix - Validation Set (rows=actual label, cols=prediction):")
    predictions.stat.crosstab("label", "prediction").show(truncate=False)

    # Step 6: Model performance - AUC.
    # SAS: ods output Association (concordance/AUC) -> BinaryClassificationEvaluator.
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(predictions)
    print(f"\nModel Performance - Area Under ROC (AUC): {auc:.4f}")

    # Model internals (parameter estimates and training fit).
    # SAS: PROC LOGISTIC parameter estimates + Association statistics.
    lr_model = model.stages[-1]
    print("\nModel Parameter Estimates:")
    print(f"  Intercept: {lr_model.intercept:.6f}")
    for i, coef in enumerate(lr_model.coefficients):
        print(f"  feature[{i}]: {coef:.6f}")

    training_summary = lr_model.summary
    print(f"  Training accuracy:      {training_summary.accuracy:.4f}")
    print(f"  Training areaUnderROC:  {training_summary.areaUnderROC:.4f}")

    # Step 7: Score distribution by actual outcome.
    # SAS: PROC MEANS class BAD; var pred_prob;
    # Extract P(default) (index 1) from the probability vector.
    prob_default = predictions.rdd.map(
        lambda r: (int(r["BAD"]), float(r["probability"][1]))
    ).toDF(["BAD", "pred_prob"])

    print("\nPredicted Probability Distribution by Actual Outcome:")
    prob_default.groupBy("BAD").agg(
        mean("pred_prob").alias("Mean_Prob"),
        stddev("pred_prob").alias("Std_Prob"),
        smin("pred_prob").alias("Min_Prob"),
        median("pred_prob").alias("Median_Prob"),
        smax("pred_prob").alias("Max_Prob"),
    ).orderBy("BAD").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
