"""
Test Suite: test_pyspark_outputs.py
Purpose: Validate PySpark transformations against expected outputs
Ensures correctness of the migrated SAS-to-PySpark logic.
"""

import unittest
import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, mean, count, round as spark_round


def build_risk_segments(df):
    """Apply the module-04 risk-segmentation pipeline (mirrors
    pyspark/04_risk_segmentation.py) and return the DataFrame with
    RISK_SCORE and RISK_SEGMENT columns."""
    df = df.withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE"),
        ),
    ).filter(
        col("LOAN").isNotNull()
        & col("VALUE").isNotNull()
        & col("BAD").isNotNull()
        & (col("LOAN") > 0)
        & (col("VALUE") > 0)
    )

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

    def test_default_rate_by_segment_matches_sas_baseline(self):
        """Parity (L8N2-15): PySpark default-rate-by-segment matches the SAS
        baseline (tests/baselines/risk_segment_default_rate.csv) within
        tolerance for the rate and exactly for the per-segment row counts."""
        testDir = os.path.dirname(os.path.abspath(__file__))
        baselinePath = os.path.join(
            testDir, "baselines", "risk_segment_default_rate.csv"
        )
        with open(baselinePath, newline="") as fh:
            baseline = {
                row["RISK_SEGMENT"]: {
                    "N": int(row["N"]),
                    "Default_Rate_Pct": float(row["Default_Rate_Pct"]),
                }
                for row in csv.DictReader(fh)
            }

        dfRisk = build_risk_segments(self.df)
        actual = {
            row["RISK_SEGMENT"]: row
            for row in dfRisk.groupBy("RISK_SEGMENT")
            .agg(
                count("*").alias("N"),
                spark_round(mean("BAD") * 100, 2).alias("Default_Rate_Pct"),
            )
            .collect()
        }

        self.assertEqual(
            set(actual.keys()),
            set(baseline.keys()),
            "Risk segments differ from SAS baseline",
        )
        for segment, expected in baseline.items():
            self.assertEqual(
                actual[segment]["N"],
                expected["N"],
                f"Row count mismatch for segment '{segment}'",
            )
            self.assertAlmostEqual(
                actual[segment]["Default_Rate_Pct"],
                expected["Default_Rate_Pct"],
                delta=0.01,
                msg=f"Default rate mismatch for segment '{segment}'",
            )

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


if __name__ == "__main__":
    unittest.main()
