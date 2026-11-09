"""
Test Suite: test_pyspark_outputs.py
Purpose: Validate PySpark transformations against expected outputs
Ensures correctness of the migrated SAS-to-PySpark logic.
"""

import unittest
import os
import re
import subprocess
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, mean, count


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

        cls.projectRoot = projectRoot
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

    def _buildRiskDataset(self, df):
        """Rebuild work.home_equity_risk as in pyspark/04_risk_segmentation.py."""
        dfFinal = df.withColumn(
            "LTV",
            when(
                (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ).filter(
            col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
        ).filter(
            (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
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
        return dfFinal.withColumn(
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
        ).withColumn(
            "DELINQ_RISK_CAT",
            when(col("DELINQ").isNull(), lit(None))
            .when(col("DELINQ") == 0, lit("None"))
            .when(col("DELINQ") == 1, lit("Low"))
            .when(col("DELINQ") <= 3, lit("Medium"))
            .otherwise(lit("High"))
        ).withColumn(
            "RISK_SCORE",
            (ltvScore + dtiScore + delinqScore + derogScore).cast("double")
        ).withColumn(
            "RISK_SEGMENT",
            when(col("RISK_SCORE") < 3, lit("Low Risk"))
            .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
            .when(col("RISK_SCORE") < 7, lit("High Risk"))
            .otherwise(lit("Very High Risk"))
        )

    def test_risk_segmentation_script_runs(self):
        """Run pyspark/04_risk_segmentation.py and check its printed outputs."""
        result = subprocess.run(
            [sys.executable, "pyspark/04_risk_segmentation.py"],
            cwd=self.projectRoot, capture_output=True, text=True, timeout=600
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        out = result.stdout

        self.assertIn("Rows in work.home_equity_final: 5337", out)
        self.assertIn("Rows in work.home_equity_risk: 5337", out)

        # PROC FREQ one-way tables: Frequency and Percent per level
        expectedFreq = [
            ("High Risk", 427, "8.0"), ("Low Risk", 3118, "58.42"),
            ("Medium Risk", 1776, "33.28"), ("Very High Risk", 16, "0.3"),
        ]
        for level, freq, pct in expectedFreq:
            self.assertRegex(
                out, rf"\|{re.escape(level)}\s*\|{freq}\s*\|{re.escape(pct)}\s*\|"
            )
        self.assertIn("Frequency Missing = 1084", out)  # DTI_RISK_CAT
        self.assertIn("Frequency Missing = 390", out)   # DELINQ_RISK_CAT

        # PROC MEANS: default rate (mean of BAD) per segment
        self.assertRegex(out, r"\|Low Risk\s*\|3118\s*\|BAD\s*\|3118\s*\|0\.1523\s*\|")
        self.assertRegex(out, r"\|Very High Risk\s*\|16\s*\|DEBTINC\s*\|15\s*\|65\.7773\s*\|")

        # PROC FREQ three-way table: one DTI x BAD table per LTV level
        for ltvLevel in ["High", "Low", "Medium"]:
            self.assertIn(f"Controlling for LTV_RISK_CAT={ltvLevel}", out)
        self.assertRegex(out, r"\|Total\s*\|933\s*\|103\s*\|1036\s*\|")
        self.assertRegex(out, r"\|Total\s*\|810\s*\|73\s*\|883\s*\|")
        self.assertRegex(out, r"\|Total\s*\|2160\s*\|174\s*\|2334\s*\|")

    def test_risk_input_filter_matches_sas_missing_semantics(self):
        """SAS 'if LTV > 0 and LTV < 5' drops missing LTV; so must Spark."""
        dfRisk = self._buildRiskDataset(self.df)
        self.assertEqual(dfRisk.count(), 5337)
        self.assertEqual(dfRisk.filter(col("LTV").isNull()).count(), 0)

        # Independent row count from the raw CSV with plain Python
        import csv
        dataPath = os.path.join(self.projectRoot, "data", "home_equity.csv")
        kept = 0
        with open(dataPath, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row["LOAN"] == "" or row["VALUE"] == "" or row["BAD"] == "":
                    continue
                loan, value = float(row["LOAN"]), float(row["VALUE"])
                if row["MORTDUE"] == "" or value <= 0:
                    continue  # LTV missing -> dropped by LTV > 0
                ltv = float(row["MORTDUE"]) / value
                if 0 < ltv < 5 and loan > 0 and value > 0:
                    kept += 1
        self.assertEqual(dfRisk.count(), kept)

    def test_risk_category_boundaries(self):
        """Check range boundaries and missing inputs on synthetic rows."""
        rows = [
            # MORTDUE (VALUE=100000, so LTV = MORTDUE / 100000), DEBTINC, DELINQ, DEROG
            (59990, 29.99, 0, 0),
            (60000, 30.0, 1, 1),
            (70000, None, 4, None),
            (70000, 45.0, None, 0),
            (80000, 40.0, 2, 2),
            (120000, 50.0, 3, 3),
        ]
        dfInput = self.spark.createDataFrame(
            rows, "MORTDUE int, DEBTINC double, DELINQ int, DEROG int"
        ).withColumn("VALUE", lit(100000)) \
         .withColumn("LOAN", lit(10000)) \
         .withColumn("BAD", lit(0))
        result = self._buildRiskDataset(dfInput).orderBy("MORTDUE", "DEBTINC").collect()
        actual = [
            (r["LTV_RISK_CAT"], r["DTI_RISK_CAT"], r["DELINQ_RISK_CAT"],
             r["RISK_SCORE"], r["RISK_SEGMENT"])
            for r in result
        ]
        expected = [
            ("Low", "Low", "None", 0.0, "Low Risk"),
            ("Medium", "Medium", "Low", 4.0, "Medium Risk"),
            ("Medium", None, "High", 3.5, "Medium Risk"),
            ("Medium", "High", None, 3.5, "Medium Risk"),
            ("High", "High", "Medium", 7.5, "Very High Risk"),
            ("High", "Very High", "Medium", 9.5, "Very High Risk"),
        ]
        self.assertEqual(actual, expected)

    def test_risk_freq_excludes_missing_from_percent(self):
        """PROC FREQ percents use only non-missing levels as the denominator."""
        dfRisk = self._buildRiskDataset(self.df)
        nonMissing = dfRisk.filter(col("DTI_RISK_CAT").isNotNull())
        total = nonMissing.count()
        self.assertEqual(dfRisk.count() - total, 1084)
        freq = {
            r["DTI_RISK_CAT"]: r["count"]
            for r in nonMissing.groupBy("DTI_RISK_CAT").count().collect()
        }
        self.assertEqual(freq, {"Low": 1122, "Medium": 2293, "High": 796, "Very High": 42})
        self.assertAlmostEqual(sum(v * 100.0 / total for v in freq.values()), 100.0)
        self.assertAlmostEqual(round(freq["Low"] * 100.0 / total, 2), 26.38)

    def test_risk_three_way_crosstab(self):
        """LTV x DTI x BAD table: per-stratum totals and zero-filled cells."""
        dfRisk = self._buildRiskDataset(self.df).filter(
            col("DTI_RISK_CAT").isNotNull()
        )
        cells = dfRisk.groupBy("LTV_RISK_CAT", "DTI_RISK_CAT") \
            .pivot("BAD", [0, 1]).count().na.fill(0).collect()
        byKey = {(r["LTV_RISK_CAT"], r["DTI_RISK_CAT"]): (r["0"], r["1"]) for r in cells}
        self.assertEqual(len(byKey), 12)
        self.assertEqual(byKey[("High", "Very High")], (0, 13))
        self.assertEqual(byKey[("Medium", "Medium")], (1259, 69))
        self.assertEqual(sum(a + b for a, b in byKey.values()), 5337 - 1084)

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
