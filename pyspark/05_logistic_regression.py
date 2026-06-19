"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - Feature engineering with StringIndexer / OneHotEncoder / VectorAssembler
  - ML Pipeline with LogisticRegression
  - Model evaluation (AUC, confusion matrix, coefficients)

Migrated from: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, initcap, count
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def clean_home_equity(df):
    """Replicate the cleaning pipeline from 02_data_cleaning.py (work.home_equity_final)."""
    df = df.withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) &
            (col("MORTDUE").isNotNull()) &
            (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ).withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid"))
        .when(col("BAD") == 1, lit("Default"))
    ).withColumn(
        "CITY", initcap(col("CITY"))
    )

    missCols = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
    for c in missCols:
        df = df.withColumn(
            f"{c}_MISS",
            when(col(c).isNull(), lit(1)).otherwise(lit(0))
        )

    df = df.filter(
        col("LOAN").isNotNull() &
        col("VALUE").isNotNull() &
        col("BAD").isNotNull()
    ).filter(
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) &
        (col("VALUE") > 0)
    )
    return df


def add_risk_segments(df):
    """Risk scoring from 04_risk_segmentation.py (home_equity_risk)."""
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
    return df.withColumn("RISK_SCORE", ltvScore + dtiScore + delinqScore + derogScore)


def main():
    spark = SparkSession.builder \
        .appName("HomeEquity_LogisticRegression") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
    homeEquityRisk = add_risk_segments(clean_home_equity(df))

    # Step 1: Keep complete cases for key predictors.
    # SAS equivalent: DATA work.model_data; if LOAN ne . and MORTDUE ne . ... ;
    modelData = homeEquityRisk.filter(
        col("LOAN").isNotNull() &
        col("MORTDUE").isNotNull() &
        col("VALUE").isNotNull() &
        col("DEBTINC").isNotNull() &
        col("DELINQ").isNotNull() &
        col("CLAGE").isNotNull() &
        col("DEROG").isNotNull() &
        col("NINQ").isNotNull() &
        col("JOB").isNotNull() &
        col("REASON").isNotNull()
    )

    print(f"Complete-case modeling rows: {modelData.count()}")

    # Step 2: Train/test split (70/30).
    # SAS equivalent: PROC SURVEYSELECT samprate=0.7 seed=42 -> train / valid.
    train, valid = modelData.randomSplit([0.7, 0.3], seed=42)

    # Step 3: Build the ML pipeline.
    # SAS equivalent: CLASS JOB REASON / param=ref  ->  StringIndexer + OneHotEncoder.
    jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
    reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
    jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
    reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

    # SAS equivalent: the MODEL statement's list of effects.
    assembler = VectorAssembler(
        inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                   "DELINQ", "DEROG", "CLAGE", "NINQ",
                   "JOB_VEC", "REASON_VEC"],
        outputCol="features"
    )

    # SAS equivalent: PROC LOGISTIC ... model BAD = ...;
    lr = LogisticRegression(
        featuresCol="features", labelCol="BAD",
        maxIter=50, regParam=0.01
    )

    pipeline = Pipeline(stages=[
        jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
    ])

    # Fit on training data.
    # SAS equivalent: PROC LOGISTIC data=work.train.
    model = pipeline.fit(train)

    # Step 4: Score the validation set.
    # SAS equivalent: PROC PLM score data=work.valid / ilink.
    predictions = model.transform(valid)

    # Evaluate AUC.
    # SAS equivalent: PROC LOGISTIC ROC / Association concordance statistics.
    evaluator = BinaryClassificationEvaluator(
        labelCol="BAD", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(predictions)

    # Step 5: Confusion matrix.
    # SAS equivalent: PROC FREQ tables BAD * PREDICTED_BAD.
    print("\nConfusion Matrix (rows=BAD actual, cols=prediction):")
    predictions.groupBy("BAD").pivot("prediction").agg(count(lit(1))).orderBy("BAD").show()

    # Model coefficients and performance metrics.
    # SAS equivalent: PROC LOGISTIC parameter estimates + Association statistics.
    lrModel = model.stages[-1]
    print("\nModel Performance:")
    print(f"  AUC (areaUnderROC): {auc:.4f}")
    print(f"  Intercept: {lrModel.intercept:.6f}")
    print("  Coefficients:")
    for i, coef in enumerate(lrModel.coefficients):
        print(f"    feature[{i}]: {coef:.6f}")

    summary = lrModel.summary
    print(f"  Training accuracy: {summary.accuracy:.4f}")
    print(f"  Training areaUnderROC: {summary.areaUnderROC:.4f}")

    spark.stop()


if __name__ == "__main__":
    main()
