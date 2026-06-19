"""
Test Suite: test_pyspark_outputs.py
Purpose: Validate PySpark transformations against expected outputs
Ensures correctness of the migrated SAS-to-PySpark logic.
"""

import unittest
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, mean, count, sum as spark_sum,
    percent_rank, ceil as spark_ceil, udf,
    min as spark_min, max as spark_max
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


class TestHomeEquityPySpark(unittest.TestCase):
    """Test suite for HOME_EQUITY PySpark migration scripts."""

    @classmethod
    def setUpClass(cls):
        """Create a local SparkSession and load the dataset."""
        cls.spark = SparkSession.builder \
            .appName("HomeEquity_Tests") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        # Determine the data path relative to the project root
        testDir = os.path.dirname(os.path.abspath(__file__))
        projectRoot = os.path.dirname(testDir)
        dataPath = os.path.join(projectRoot, "data", "home_equity.csv")

        cls.df = cls.spark.read.csv(dataPath, header=True, inferSchema=True)

    @classmethod
    def tearDownClass(cls):
        """Stop the SparkSession."""
        cls.spark.stop()

    # ------------------------------------------------------------------
    # Test 01: Data Loading
    # ------------------------------------------------------------------
    def test_data_loads_successfully(self):
        """Verify the CSV loads without errors."""
        self.assertIsNotNone(self.df)
        self.assertTrue(self.df.count() > 0)

    def test_row_count(self):
        """Verify expected row count from home_equity.csv."""
        rowCount = self.df.count()
        self.assertEqual(rowCount, 5960, f"Expected 5960 rows, got {rowCount}")

    def test_expected_columns_exist(self):
        """Verify all expected columns are present."""
        expectedColumns = [
            "BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "JOB",
            "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ", "CLNO",
            "DEBTINC", "APPDATE", "CITY", "STATE", "DIVISION", "REGION"
        ]
        actualColumns = self.df.columns
        for colName in expectedColumns:
            self.assertIn(
                colName, actualColumns,
                f"Missing expected column: {colName}"
            )

    def test_column_count(self):
        """Verify the number of columns."""
        self.assertEqual(len(self.df.columns), 18)

    # ------------------------------------------------------------------
    # Test 02: Data Cleaning - Derived Columns
    # ------------------------------------------------------------------
    def test_ltv_calculation(self):
        """Verify LTV (Loan-to-Value) is computed correctly."""
        dfClean = self.df.withColumn(
            "LTV",
            when(
                (col("VALUE").isNotNull()) &
                (col("MORTDUE").isNotNull()) &
                (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        )
        self.assertIn("LTV", dfClean.columns)

        # LTV should be positive where computed
        validLtv = dfClean.filter(col("LTV").isNotNull())
        self.assertTrue(validLtv.count() > 0, "No valid LTV values computed")

        # Check a known calculation: row where MORTDUE=25860, VALUE=39025
        # LTV should be approximately 0.6627
        sample = dfClean.filter(
            (col("MORTDUE") == 25860) & (col("VALUE") == 39025)
        ).select("LTV").collect()
        if len(sample) > 0:
            ltv = sample[0]["LTV"]
            self.assertAlmostEqual(ltv, 25860 / 39025, places=4)

    def test_loan_outcome_derivation(self):
        """Verify LOAN_OUTCOME is derived correctly from BAD."""
        dfClean = self.df.withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid"))
            .when(col("BAD") == 1, lit("Default"))
        )
        self.assertIn("LOAN_OUTCOME", dfClean.columns)

        # All BAD=0 should map to 'Paid'
        paidCount = dfClean.filter(
            (col("BAD") == 0) & (col("LOAN_OUTCOME") == "Paid")
        ).count()
        totalPaid = self.df.filter(col("BAD") == 0).count()
        self.assertEqual(paidCount, totalPaid)

        # All BAD=1 should map to 'Default'
        defaultCount = dfClean.filter(
            (col("BAD") == 1) & (col("LOAN_OUTCOME") == "Default")
        ).count()
        totalDefault = self.df.filter(col("BAD") == 1).count()
        self.assertEqual(defaultCount, totalDefault)

    def test_filtering_removes_nulls(self):
        """Verify filtering removes records with null critical fields."""
        dfFiltered = self.df.filter(
            col("LOAN").isNotNull() &
            col("VALUE").isNotNull() &
            col("BAD").isNotNull()
        )
        # Should have fewer or equal rows
        self.assertLessEqual(dfFiltered.count(), self.df.count())
        # No nulls in critical columns
        self.assertEqual(
            dfFiltered.filter(col("LOAN").isNull()).count(), 0
        )
        self.assertEqual(
            dfFiltered.filter(col("VALUE").isNull()).count(), 0
        )
        self.assertEqual(
            dfFiltered.filter(col("BAD").isNull()).count(), 0
        )

    # ------------------------------------------------------------------
    # Test 03: Aggregation and Reporting
    # ------------------------------------------------------------------
    def test_frequency_counts(self):
        """Verify groupBy counts produce valid results."""
        jobCounts = self.df.groupBy("JOB").count().collect()
        self.assertTrue(len(jobCounts) > 0, "No job categories found")

        # Check that total counts sum to dataset size (minus nulls)
        totalFromGroups = sum(row["count"] for row in jobCounts)
        nonNullJobs = self.df.filter(col("JOB").isNotNull()).count()
        self.assertEqual(totalFromGroups, nonNullJobs)

    def test_summary_stats_by_group(self):
        """Verify grouped summary statistics are computed."""
        dfClean = self.df.withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid"))
            .when(col("BAD") == 1, lit("Default"))
        )

        stats = dfClean.groupBy("LOAN_OUTCOME").agg(
            count("LOAN").alias("N"),
            mean("LOAN").alias("Mean_LOAN")
        ).collect()

        self.assertTrue(len(stats) > 0)
        for row in stats:
            if row["LOAN_OUTCOME"] is not None:
                self.assertTrue(row["N"] > 0)
                self.assertTrue(row["Mean_LOAN"] > 0)

    def test_sql_query_top_states(self):
        """Verify Spark SQL query returns valid results."""
        self.df.createOrReplaceTempView("home_equity_test")

        result = self.spark.sql("""
            SELECT STATE, COUNT(*) AS num_loans, AVG(LOAN) AS avg_loan
            FROM home_equity_test
            GROUP BY STATE
            HAVING COUNT(*) >= 10
            ORDER BY avg_loan DESC
            LIMIT 10
        """).collect()

        self.assertTrue(len(result) > 0, "SQL query returned no results")
        self.assertLessEqual(len(result), 10)
        # Verify ordering (descending by avg_loan)
        for i in range(len(result) - 1):
            self.assertGreaterEqual(result[i]["avg_loan"], result[i + 1]["avg_loan"])

    # ------------------------------------------------------------------
    # Test 04: Risk Segmentation
    # ------------------------------------------------------------------
    def test_risk_categories_created(self):
        """Verify risk category columns are computed."""
        dfRisk = self.df.withColumn(
            "LTV",
            when(
                (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ).withColumn(
            "LTV_RISK_CAT",
            when(col("LTV").isNull(), lit(None))
            .when(col("LTV") < 0.60, lit("Low"))
            .when(col("LTV") < 0.80, lit("Medium"))
            .otherwise(lit("High"))
        ).withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC").isNull(), lit(None))
            .when(col("DEBTINC") < 30, lit("Low"))
            .when(col("DEBTINC") < 40, lit("Medium"))
            .when(col("DEBTINC") < 50, lit("High"))
            .otherwise(lit("Very High"))
        )

        self.assertIn("LTV_RISK_CAT", dfRisk.columns)
        self.assertIn("DTI_RISK_CAT", dfRisk.columns)

        # Verify valid categories
        validLtvCats = {"Low", "Medium", "High", None}
        actualLtvCats = set(
            row["LTV_RISK_CAT"]
            for row in dfRisk.select("LTV_RISK_CAT").distinct().collect()
        )
        self.assertTrue(actualLtvCats.issubset(validLtvCats))

    def test_risk_score_range(self):
        """Verify composite risk score is within expected range (0-10)."""
        dfRisk = self.df.withColumn(
            "LTV",
            when(
                (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        )

        # LTV component
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

        dfRisk = dfRisk.withColumn(
            "RISK_SCORE",
            ltvScore + dtiScore + delinqScore + derogScore
        )

        # All scores should be between 0 and 10
        minScore = dfRisk.agg({"RISK_SCORE": "min"}).collect()[0][0]
        maxScore = dfRisk.agg({"RISK_SCORE": "max"}).collect()[0][0]
        self.assertGreaterEqual(minScore, 0)
        self.assertLessEqual(maxScore, 10)

    def test_risk_segment_labels(self):
        """Verify risk segment labels are correct."""
        dfRisk = self.df.withColumn(
            "LTV",
            when(
                (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        )

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

        dfRisk = dfRisk.withColumn(
            "RISK_SCORE",
            ltvScore + dtiScore + delinqScore + derogScore
        ).withColumn(
            "RISK_SEGMENT",
            when(col("RISK_SCORE") < 3, lit("Low Risk"))
            .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
            .when(col("RISK_SCORE") < 7, lit("High Risk"))
            .otherwise(lit("Very High Risk"))
        )

        validSegments = {"Low Risk", "Medium Risk", "High Risk", "Very High Risk"}
        actualSegments = set(
            row["RISK_SEGMENT"]
            for row in dfRisk.select("RISK_SEGMENT").distinct().collect()
        )
        self.assertTrue(actualSegments.issubset(validSegments))

    # ------------------------------------------------------------------
    # Test 05: Logistic Regression
    # ------------------------------------------------------------------
    def test_logistic_regression_model(self):
        """Verify logistic regression pipeline produces predictions."""
        from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        # Prepare data
        modelData = self.df.filter(
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
        ).withColumn("label", col("BAD").cast("double"))

        self.assertTrue(
            modelData.count() > 100,
            "Not enough complete cases for modeling"
        )

        train, valid = modelData.randomSplit([0.7, 0.3], seed=42)

        # Build pipeline
        jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
        reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
        jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
        reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

        assembler = VectorAssembler(
            inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                        "DELINQ", "DEROG", "CLAGE", "NINQ",
                        "JOB_VEC", "REASON_VEC"],
            outputCol="features"
        )

        lr = LogisticRegression(
            featuresCol="features", labelCol="label",
            maxIter=50, regParam=0.01
        )

        pipeline = Pipeline(stages=[
            jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
        ])

        model = pipeline.fit(train)
        predictions = model.transform(valid)

        # Verify predictions exist
        self.assertIn("prediction", predictions.columns)
        self.assertIn("probability", predictions.columns)

        # Verify predictions are 0 or 1
        predValues = set(
            row["prediction"]
            for row in predictions.select("prediction").distinct().collect()
        )
        self.assertTrue(predValues.issubset({0.0, 1.0}))

        # Verify AUC is reasonable (> 0.5 = better than random)
        evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(predictions)
        self.assertGreater(auc, 0.5, f"AUC ({auc:.4f}) should be > 0.5")


    # ------------------------------------------------------------------
    # Test 06: Model Scoring and Evaluation Pipeline
    # ------------------------------------------------------------------
    def _build_scoring_pipeline(self):
        """Helper: prepare data, train model, score holdout set."""
        from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml import Pipeline

        modelData = self.df.filter(
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
        ).withColumn("label", col("BAD").cast("double"))

        train, remaining = modelData.randomSplit([0.6, 0.4], seed=42)
        valid, holdout = remaining.randomSplit([0.5, 0.5], seed=42)

        jobIdx = StringIndexer(inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep")
        reasonIdx = StringIndexer(inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep")
        jobEnc = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
        reasonEnc = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

        assembler = VectorAssembler(
            inputCols=["LOAN", "MORTDUE", "VALUE", "DEBTINC",
                        "DELINQ", "DEROG", "CLAGE", "NINQ",
                        "JOB_VEC", "REASON_VEC"],
            outputCol="features"
        )

        lr = LogisticRegression(
            featuresCol="features", labelCol="label",
            maxIter=100, regParam=0.01, elasticNetParam=0.8
        )

        pipeline = Pipeline(stages=[
            jobIdx, reasonIdx, jobEnc, reasonEnc, assembler, lr
        ])

        model = pipeline.fit(train)

        extractProb = udf(lambda v: float(v[1]), DoubleType())

        holdoutScored = model.transform(holdout)
        holdoutScored = holdoutScored.withColumn(
            "pred_prob", extractProb(col("probability"))
        ).withColumn(
            "PREDICTED_BAD",
            when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
        )

        return model, train, valid, holdout, holdoutScored

    def test_scoring_three_way_split(self):
        """Verify 60/20/20 split produces three non-empty partitions."""
        modelData = self.df.filter(
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
        total = modelData.count()
        self.assertTrue(total > 100, "Not enough complete cases for modeling")

        train, remaining = modelData.randomSplit([0.6, 0.4], seed=42)
        valid, holdout = remaining.randomSplit([0.5, 0.5], seed=42)

        self.assertTrue(train.count() > 0, "Training set is empty")
        self.assertTrue(valid.count() > 0, "Validation set is empty")
        self.assertTrue(holdout.count() > 0, "Holdout set is empty")
        self.assertEqual(
            train.count() + valid.count() + holdout.count(), total,
            "Split sizes do not sum to total"
        )

    def test_scoring_pipeline_produces_predictions(self):
        """Verify scoring pipeline produces valid predictions on holdout."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        self.assertIn("prediction", holdoutScored.columns)
        self.assertIn("probability", holdoutScored.columns)
        self.assertIn("pred_prob", holdoutScored.columns)
        self.assertIn("PREDICTED_BAD", holdoutScored.columns)

        predValues = set(
            row["prediction"]
            for row in holdoutScored.select("prediction").distinct().collect()
        )
        self.assertTrue(predValues.issubset({0.0, 1.0}))

    def test_scoring_predicted_bad_matches_threshold(self):
        """Verify PREDICTED_BAD matches the 0.5 threshold on pred_prob."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        mismatch = holdoutScored.filter(
            ((col("pred_prob") >= 0.5) & (col("PREDICTED_BAD") != 1)) |
            ((col("pred_prob") < 0.5) & (col("PREDICTED_BAD") != 0))
        ).count()
        self.assertEqual(mismatch, 0, "PREDICTED_BAD does not match 0.5 threshold")

    def test_scoring_auc_above_random(self):
        """Verify AUC on holdout is better than random (> 0.5)."""
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(holdoutScored)
        self.assertGreater(auc, 0.5, f"AUC ({auc:.4f}) should be > 0.5")

    def test_scoring_gini_positive(self):
        """Verify Gini coefficient (2*AUC - 1) is positive."""
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(holdoutScored)
        gini = 2 * auc - 1
        self.assertGreater(gini, 0, f"Gini ({gini:.4f}) should be positive")

    def test_scoring_risk_grades(self):
        """Verify risk grade assignment covers all expected grades."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        scorecard = holdoutScored.withColumn(
            "RISK_GRADE",
            when(col("pred_prob") < 0.10, lit("A - Minimal"))
            .when(col("pred_prob") < 0.25, lit("B - Low"))
            .when(col("pred_prob") < 0.50, lit("C - Moderate"))
            .when(col("pred_prob") < 0.75, lit("D - High"))
            .otherwise(lit("E - Critical"))
        )

        validGrades = {
            "A - Minimal", "B - Low", "C - Moderate",
            "D - High", "E - Critical"
        }
        actualGrades = set(
            row["RISK_GRADE"]
            for row in scorecard.select("RISK_GRADE").distinct().collect()
        )
        self.assertTrue(
            actualGrades.issubset(validGrades),
            f"Unexpected risk grades: {actualGrades - validGrades}"
        )
        self.assertTrue(
            len(actualGrades) >= 2,
            "Model should produce at least 2 distinct risk grades"
        )

    def test_scoring_risk_grade_monotonic_default_rate(self):
        """Verify higher risk grades have higher or equal default rates."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        scorecard = holdoutScored.withColumn(
            "RISK_GRADE",
            when(col("pred_prob") < 0.10, lit("A - Minimal"))
            .when(col("pred_prob") < 0.25, lit("B - Low"))
            .when(col("pred_prob") < 0.50, lit("C - Moderate"))
            .when(col("pred_prob") < 0.75, lit("D - High"))
            .otherwise(lit("E - Critical"))
        )

        gradeStats = scorecard.groupBy("RISK_GRADE").agg(
            mean("label").alias("default_rate")
        ).collect()

        gradeOrder = ["A - Minimal", "B - Low", "C - Moderate", "D - High", "E - Critical"]
        ratesByGrade = {row["RISK_GRADE"]: row["default_rate"] for row in gradeStats}

        presentGrades = [g for g in gradeOrder if g in ratesByGrade]
        for i in range(len(presentGrades) - 1):
            rate_curr = ratesByGrade[presentGrades[i]]
            rate_next = ratesByGrade[presentGrades[i + 1]]
            self.assertLessEqual(
                rate_curr, rate_next + 0.01,
                f"Default rate for {presentGrades[i]} ({rate_curr:.3f}) should be "
                f"<= {presentGrades[i + 1]} ({rate_next:.3f})"
            )

    def test_scoring_decile_analysis(self):
        """Verify decile analysis produces 10 buckets with valid stats."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        windowSpec = Window.orderBy(col("pred_prob").desc())
        holdoutDeciles = holdoutScored.withColumn(
            "pct_rank", percent_rank().over(windowSpec)
        ).withColumn(
            "decile", spark_ceil(col("pct_rank") * 10).cast("int")
        ).withColumn(
            "decile", when(col("decile") == 0, lit(1)).otherwise(col("decile"))
        )

        gainsTable = holdoutDeciles.groupBy("decile").agg(
            count("*").alias("N"),
            spark_sum("label").cast("int").alias("Defaults"),
            mean("pred_prob").alias("Avg_Score"),
            spark_min("pred_prob").alias("Min_Score"),
            spark_max("pred_prob").alias("Max_Score")
        ).orderBy("decile").collect()

        self.assertEqual(len(gainsTable), 10, "Should have exactly 10 deciles")

        for row in gainsTable:
            self.assertTrue(row["N"] > 0, f"Decile {row['decile']} is empty")
            self.assertGreaterEqual(row["Avg_Score"], 0.0)
            self.assertLessEqual(row["Avg_Score"], 1.0)

    def test_scoring_ks_statistic(self):
        """Verify KS statistic is between 0 and 1."""
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        totalDefaults = holdoutScored.filter(col("label") == 1.0).count()
        totalN = holdoutScored.count()
        self.assertTrue(totalDefaults > 0, "No defaults in holdout set")

        windowSpec = Window.orderBy(col("pred_prob").desc())
        holdoutDeciles = holdoutScored.withColumn(
            "pct_rank", percent_rank().over(windowSpec)
        ).withColumn(
            "decile", spark_ceil(col("pct_rank") * 10).cast("int")
        ).withColumn(
            "decile", when(col("decile") == 0, lit(1)).otherwise(col("decile"))
        )

        gainsRows = holdoutDeciles.groupBy("decile").agg(
            count("*").alias("N"),
            spark_sum("label").cast("int").alias("Defaults")
        ).orderBy("decile").collect()

        cumDefaults = 0
        cumN = 0
        ksMax = 0.0
        for row in gainsRows:
            cumDefaults += row["Defaults"]
            cumN += row["N"]
            cumDefPct = cumDefaults / totalDefaults
            cumPopPct = cumN / totalN
            ksDiff = abs(cumDefPct - cumPopPct)
            if ksDiff > ksMax:
                ksMax = ksDiff

        self.assertGreater(ksMax, 0.0, "KS statistic should be > 0")
        self.assertLessEqual(ksMax, 1.0, "KS statistic should be <= 1")

    def test_scoring_pred_prob_range(self):
        """Verify predicted probabilities are between 0 and 1."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        minProb = holdoutScored.agg(spark_min("pred_prob")).collect()[0][0]
        maxProb = holdoutScored.agg(spark_max("pred_prob")).collect()[0][0]
        self.assertGreaterEqual(minProb, 0.0, "Min pred_prob should be >= 0")
        self.assertLessEqual(maxProb, 1.0, "Max pred_prob should be <= 1")

    def test_scoring_confusion_matrix_sums(self):
        """Verify confusion matrix entries sum to holdout count."""
        _, _, _, _, holdoutScored = self._build_scoring_pipeline()

        cmRows = holdoutScored.groupBy("label", "PREDICTED_BAD") \
            .count().collect()
        cmTotal = sum(row["count"] for row in cmRows)
        self.assertEqual(
            cmTotal, holdoutScored.count(),
            "Confusion matrix counts do not sum to holdout size"
        )


if __name__ == "__main__":
    unittest.main()
