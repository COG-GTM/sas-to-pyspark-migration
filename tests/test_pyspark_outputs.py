"""Validate PySpark transformations against expected outputs.

Ensures correctness of the migrated SAS-to-PySpark logic. The SparkSession and
dataset are now provided by the shared fixtures in ``conftest.py`` (``spark`` and
``home_equity_df``) instead of a per-file ``setUpClass``.
"""

from pyspark.sql.functions import col, count, lit, mean, when

# ----------------------------------------------------------------------
# Test 01: Data Loading
# ----------------------------------------------------------------------


def test_data_loads_successfully(home_equity_df):
    """Verify the CSV loads without errors."""
    assert home_equity_df is not None
    assert home_equity_df.count() > 0


def test_row_count(home_equity_df):
    """Verify expected row count from home_equity.csv."""
    row_count = home_equity_df.count()
    assert row_count == 5960, f"Expected 5960 rows, got {row_count}"


def test_expected_columns_exist(home_equity_df):
    """Verify all expected columns are present."""
    expected_columns = [
        "BAD",
        "LOAN",
        "MORTDUE",
        "VALUE",
        "REASON",
        "JOB",
        "YOJ",
        "DEROG",
        "DELINQ",
        "CLAGE",
        "NINQ",
        "CLNO",
        "DEBTINC",
        "APPDATE",
        "CITY",
        "STATE",
        "DIVISION",
        "REGION",
    ]
    actual_columns = home_equity_df.columns
    for col_name in expected_columns:
        assert col_name in actual_columns, f"Missing expected column: {col_name}"


def test_column_count(home_equity_df):
    """Verify the number of columns."""
    assert len(home_equity_df.columns) == 18


# ----------------------------------------------------------------------
# Test 02: Data Cleaning - Derived Columns
# ----------------------------------------------------------------------


def test_ltv_calculation(home_equity_df):
    """Verify LTV (Loan-to-Value) is computed correctly."""
    df_clean = home_equity_df.withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ),
    )
    assert "LTV" in df_clean.columns

    valid_ltv = df_clean.filter(col("LTV").isNotNull())
    assert valid_ltv.count() > 0, "No valid LTV values computed"

    # Known calculation: row where MORTDUE=25860, VALUE=39025 -> ~0.6627
    sample = (
        df_clean.filter((col("MORTDUE") == 25860) & (col("VALUE") == 39025)).select("LTV").collect()
    )
    if sample:
        ltv = sample[0]["LTV"]
        assert abs(ltv - 25860 / 39025) < 1e-4


def test_loan_outcome_derivation(home_equity_df):
    """Verify LOAN_OUTCOME is derived correctly from BAD."""
    df_clean = home_equity_df.withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
    )
    assert "LOAN_OUTCOME" in df_clean.columns

    paid_count = df_clean.filter((col("BAD") == 0) & (col("LOAN_OUTCOME") == "Paid")).count()
    total_paid = home_equity_df.filter(col("BAD") == 0).count()
    assert paid_count == total_paid

    default_count = df_clean.filter((col("BAD") == 1) & (col("LOAN_OUTCOME") == "Default")).count()
    total_default = home_equity_df.filter(col("BAD") == 1).count()
    assert default_count == total_default


def test_filtering_removes_nulls(home_equity_df):
    """Verify filtering removes records with null critical fields."""
    df_filtered = home_equity_df.filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )
    assert df_filtered.count() <= home_equity_df.count()
    assert df_filtered.filter(col("LOAN").isNull()).count() == 0
    assert df_filtered.filter(col("VALUE").isNull()).count() == 0
    assert df_filtered.filter(col("BAD").isNull()).count() == 0


# ----------------------------------------------------------------------
# Test 03: Aggregation and Reporting
# ----------------------------------------------------------------------


def test_frequency_counts(home_equity_df):
    """Verify groupBy counts produce valid results.

    ``groupBy("JOB").count()`` includes a group for null JOB values, so we sum
    only the non-null groups when comparing against the non-null JOB count.
    """
    job_counts = home_equity_df.groupBy("JOB").count().collect()
    assert len(job_counts) > 0, "No job categories found"

    total_from_non_null_groups = sum(row["count"] for row in job_counts if row["JOB"] is not None)
    non_null_jobs = home_equity_df.filter(col("JOB").isNotNull()).count()
    assert total_from_non_null_groups == non_null_jobs


def test_summary_stats_by_group(home_equity_df):
    """Verify grouped summary statistics are computed."""
    df_clean = home_equity_df.withColumn(
        "LOAN_OUTCOME",
        when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
    )

    stats = (
        df_clean.groupBy("LOAN_OUTCOME")
        .agg(count("LOAN").alias("N"), mean("LOAN").alias("Mean_LOAN"))
        .collect()
    )

    assert len(stats) > 0
    for row in stats:
        if row["LOAN_OUTCOME"] is not None:
            assert row["N"] > 0
            assert row["Mean_LOAN"] > 0


def test_sql_query_top_states(spark, home_equity_df):
    """Verify Spark SQL query returns valid results."""
    home_equity_df.createOrReplaceTempView("home_equity_test")

    result = spark.sql(
        """
        SELECT STATE, COUNT(*) AS num_loans, AVG(LOAN) AS avg_loan
        FROM home_equity_test
        GROUP BY STATE
        HAVING COUNT(*) >= 10
        ORDER BY avg_loan DESC
        LIMIT 10
        """
    ).collect()

    assert len(result) > 0, "SQL query returned no results"
    assert len(result) <= 10
    for i in range(len(result) - 1):
        assert result[i]["avg_loan"] >= result[i + 1]["avg_loan"]


# ----------------------------------------------------------------------
# Test 04: Risk Segmentation
# ----------------------------------------------------------------------


def _with_ltv(df):
    return df.withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ),
    )


def _risk_score_column():
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
    return ltv_score + dti_score + delinq_score + derog_score


def test_risk_categories_created(home_equity_df):
    """Verify risk category columns are computed."""
    df_risk = (
        _with_ltv(home_equity_df)
        .withColumn(
            "LTV_RISK_CAT",
            when(col("LTV").isNull(), lit(None))
            .when(col("LTV") < 0.60, lit("Low"))
            .when(col("LTV") < 0.80, lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC").isNull(), lit(None))
            .when(col("DEBTINC") < 30, lit("Low"))
            .when(col("DEBTINC") < 40, lit("Medium"))
            .when(col("DEBTINC") < 50, lit("High"))
            .otherwise(lit("Very High")),
        )
    )

    assert "LTV_RISK_CAT" in df_risk.columns
    assert "DTI_RISK_CAT" in df_risk.columns

    valid_ltv_cats = {"Low", "Medium", "High", None}
    actual_ltv_cats = {
        row["LTV_RISK_CAT"] for row in df_risk.select("LTV_RISK_CAT").distinct().collect()
    }
    assert actual_ltv_cats.issubset(valid_ltv_cats)


def test_risk_score_range(home_equity_df):
    """Verify composite risk score is within expected range (0-10)."""
    df_risk = _with_ltv(home_equity_df).withColumn("RISK_SCORE", _risk_score_column())

    min_score = df_risk.agg({"RISK_SCORE": "min"}).collect()[0][0]
    max_score = df_risk.agg({"RISK_SCORE": "max"}).collect()[0][0]
    assert min_score >= 0
    assert max_score <= 10


def test_risk_segment_labels(home_equity_df):
    """Verify risk segment labels are correct."""
    df_risk = (
        _with_ltv(home_equity_df)
        .withColumn("RISK_SCORE", _risk_score_column())
        .withColumn(
            "RISK_SEGMENT",
            when(col("RISK_SCORE") < 3, lit("Low Risk"))
            .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
            .when(col("RISK_SCORE") < 7, lit("High Risk"))
            .otherwise(lit("Very High Risk")),
        )
    )

    valid_segments = {"Low Risk", "Medium Risk", "High Risk", "Very High Risk"}
    actual_segments = {
        row["RISK_SEGMENT"] for row in df_risk.select("RISK_SEGMENT").distinct().collect()
    }
    assert actual_segments.issubset(valid_segments)


# ----------------------------------------------------------------------
# Test 05: Logistic Regression
# ----------------------------------------------------------------------


def test_logistic_regression_model(home_equity_df):
    """Verify logistic regression pipeline produces predictions."""
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

    model_data = home_equity_df.filter(
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

    assert model_data.count() > 100, "Not enough complete cases for modeling"

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

    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50, regParam=0.01)

    pipeline = Pipeline(stages=[job_idx, reason_idx, job_enc, reason_enc, assembler, lr])

    model = pipeline.fit(train)
    predictions = model.transform(valid)

    assert "prediction" in predictions.columns
    assert "probability" in predictions.columns

    pred_values = {
        row["prediction"] for row in predictions.select("prediction").distinct().collect()
    }
    assert pred_values.issubset({0.0, 1.0})

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = evaluator.evaluate(predictions)
    assert auc > 0.5, f"AUC ({auc:.4f}) should be > 0.5"
