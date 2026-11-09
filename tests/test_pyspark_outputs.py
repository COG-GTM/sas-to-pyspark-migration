"""
Test Suite: test_pyspark_outputs.py
Purpose: Validate PySpark transformations against expected outputs
Ensures correctness of the migrated SAS-to-PySpark logic.
"""

import csv
import math
import os
import re
import subprocess
import sys
import unittest
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
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


    def test_logistic_regression_script_runs(self):
        """Run pyspark/05_logistic_regression.py end to end and check its
        printed row counts and AUC."""
        projectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        result = subprocess.run(
            [sys.executable, "pyspark/05_logistic_regression.py"],
            cwd=projectRoot, capture_output=True, text=True, timeout=900
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        out = result.stdout
        for expected in [
            "Observations in work.home_equity_risk: 5337",
            "Observations in work.model_data: 3881",
            "Training rows (work.train): 2717",
            "Validation rows (work.valid): 1164",
            "Scored validation rows (work.valid_scored): 1164",
            "Confusion matrix total: 1164",
            "Summary of Stepwise Selection",
            "Hosmer and Lemeshow Goodness-of-Fit Test",
            "Model Association Statistics",
            "Predicted Probability Distribution by Actual Outcome",
        ]:
            self.assertIn(expected, out)
        self.assertRegex(out, r"Selected effects: \w+")
        c = float(re.search(r"ROC area \(c\) - validation refit: ([0-9.]+)", out).group(1))
        auc = float(re.search(r"BinaryClassificationEvaluator areaUnderROC: ([0-9.]+)", out).group(1))
        self.assertGreater(c, 0.5)
        self.assertAlmostEqual(c, auc, places=3)

    def test_model_data_complete_cases(self):
        """work.home_equity_risk (02 row filters) and work.model_data
        (complete key predictors) match an independent csv count."""
        homeEquityRisk = self.df.withColumn(
            "LTV",
            when(
                col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE")
            )
        ).filter(
            col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
        ).filter((col("LTV") > 0) & (col("LTV") < 5)) \
            .filter((col("LOAN") > 0) & (col("VALUE") > 0))
        keyPredictors = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]
        modelData = homeEquityRisk.filter(
            F.expr(" AND ".join(f"{c} IS NOT NULL" for c in keyPredictors))
        )

        dataPath = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "home_equity.csv"
        )
        riskRows = modelRows = 0
        with open(dataPath, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if not (row["LOAN"] and row["VALUE"] and row["BAD"] and row["MORTDUE"]):
                    continue
                loan, value, mortdue = float(row["LOAN"]), float(row["VALUE"]), float(row["MORTDUE"])
                if not (value > 0 and loan > 0 and 0 < mortdue / value < 5):
                    continue
                riskRows += 1
                if all(row[c] != "" for c in keyPredictors):
                    modelRows += 1

        self.assertEqual(homeEquityRisk.count(), riskRows)
        self.assertEqual(modelData.count(), modelRows)
        self.assertEqual(modelRows, 3881)

    def test_srs_split_exact_sample_size(self):
        """PROC SURVEYSELECT method=srs samprate=0.7 selects exactly
        ceil(0.7 * N) rows; the split is disjoint and covers all rows."""
        self.assertEqual(math.ceil(round(0.7 * 10, 9)), 7)
        self.assertEqual(math.ceil(round(0.7 * 3881, 9)), 2717)

        data = self.df.filter(col("DEBTINC").isNotNull())
        n = data.count()
        sampleSize = math.ceil(round(0.7 * n, 9))
        split = data.coalesce(1).withColumn("_U", F.rand(42)) \
            .withColumn("_ROW", F.row_number().over(Window.orderBy("_U"))) \
            .withColumn("SELECTED", when(col("_ROW") <= sampleSize, 1).otherwise(0)) \
            .cache()
        train = split.filter(col("SELECTED") == 1)
        valid = split.filter(~(col("SELECTED") == 1))
        self.assertEqual(train.count(), sampleSize)
        self.assertEqual(valid.count(), n - sampleSize)
        self.assertEqual(train.select("_ROW").intersect(valid.select("_ROW")).count(), 0)
        split.unpersist()

    def test_class_design_missing_level_is_missing(self):
        """CLASS JOB(ref='Other') / param=ref: a missing JOB gives missing
        design columns, not the reference level."""
        toy = self.spark.createDataFrame(
            [("Mgr",), ("Other",), ("Sales",), (None,)], "JOB string"
        )
        design = toy.withColumn(
            "JOB_MGR",
            when(col("JOB").isNull(), lit(None).cast("double"))
            .when(col("JOB") == "Mgr", lit(1.0))
            .otherwise(lit(0.0))
        ).collect()
        self.assertEqual([r["JOB_MGR"] for r in design], [1.0, 0.0, 0.0, None])

    def test_predicted_bad_missing_probability(self):
        """SAS: a missing pred_prob is not >= 0.5, so PREDICTED_BAD = 0."""
        toy = self.spark.createDataFrame(
            [(0.7,), (0.5,), (0.2,), (None,)], "PRED_PROB double"
        )
        result = toy.withColumn(
            "PREDICTED_BAD",
            when(col("PRED_PROB") >= 0.5, lit(1)).otherwise(lit(0))
        ).collect()
        self.assertEqual([r["PREDICTED_BAD"] for r in result], [1, 1, 0, 0])

    def test_proc_means_percentiles_qntldef5(self):
        """PROC MEANS default percentiles (QNTLDEF=5) by class group."""
        toy = self.spark.createDataFrame(
            [(0, float(v)) for v in [1, 2, 3, 4]] +
            [(1, float(v)) for v in [5, 1, 4, 2, 3]],
            "BAD int, PRED_PROB double"
        )
        ranked = toy.withColumn(
            "_R", F.row_number().over(Window.partitionBy("BAD").orderBy("PRED_PROB"))
        ).withColumn("_N", F.count(lit(1)).over(Window.partitionBy("BAD")))

        def qntldef5(p):
            np_ = col("_N") * lit(p)
            j = F.floor(np_)
            g = np_ - j
            return F.coalesce(
                F.avg(when((g == 0) & ((col("_R") == j) | (col("_R") == j + 1)),
                           col("PRED_PROB"))),
                F.max(when((g > 0) & (col("_R") == j + 1), col("PRED_PROB")))
            )

        rows = {
            r["BAD"]: r for r in ranked.groupBy("BAD").agg(
                qntldef5(0.25).alias("P25"),
                qntldef5(0.50).alias("MEDIAN"),
                qntldef5(0.75).alias("P75"),
            ).collect()
        }
        self.assertEqual((rows[0]["P25"], rows[0]["MEDIAN"], rows[0]["P75"]), (1.5, 2.5, 3.5))
        self.assertEqual((rows[1]["P25"], rows[1]["MEDIAN"], rows[1]["P75"]), (2.0, 3.0, 4.0))

    def test_association_concordance_with_ties(self):
        """Concordant/discordant/tied pairs and c match a hand count and
        BinaryClassificationEvaluator's AUC."""
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        # events at 0.9, 0.6, 0.4; non-events at 0.6, 0.3, 0.1
        # 9 pairs: concordant 7, tied 1 (0.6 vs 0.6), discordant 1 (0.4 vs 0.6)
        toy = self.spark.createDataFrame(
            [(1, 0.9), (1, 0.6), (1, 0.4), (0, 0.6), (0, 0.3), (0, 0.1)],
            "BAD int, PRED_PROB double"
        )
        byProb = toy.groupBy("PRED_PROB").agg(
            F.sum(when(col("BAD") == 1, 1).otherwise(0)).cast("long").alias("EV"),
            F.sum(when(col("BAD") == 0, 1).otherwise(0)).cast("long").alias("NE"),
        )
        below = Window.orderBy("PRED_PROB").rowsBetween(Window.unboundedPreceding, -1)
        byProb = byProb.withColumn(
            "NE_BELOW", F.coalesce(F.sum("NE").over(below), lit(0)).cast("long")
        )
        r = byProb.agg(
            F.sum(col("EV") * col("NE_BELOW")).alias("CONC"),
            F.sum(col("EV") * col("NE")).alias("TIED"),
        ).collect()[0]
        pairs = 9
        disc = pairs - r["CONC"] - r["TIED"]
        self.assertEqual((r["CONC"], r["TIED"], disc), (7, 1, 1))
        c = (r["CONC"] + 0.5 * r["TIED"]) / pairs
        self.assertAlmostEqual(c, 7.5 / 9)

        auc = BinaryClassificationEvaluator(
            labelCol="LABEL", rawPredictionCol="PRED_PROB",
            metricName="areaUnderROC", numBins=0
        ).evaluate(toy.withColumn("LABEL", col("BAD").cast("double")))
        self.assertAlmostEqual(c, auc, places=6)

    def test_chi_square_p_values(self):
        """Pr > ChiSq used by stepwise selection matches known critical values."""
        def chi2_sf(x, df):
            if x <= 0:
                return 1.0
            if df % 2 == 0:
                term = total = math.exp(-x / 2)
                for i in range(1, df // 2):
                    term *= (x / 2) / i
                    total += term
                return min(total, 1.0)
            total = math.erfc(math.sqrt(x / 2))
            term = math.sqrt(2 * x / math.pi) * math.exp(-x / 2)
            for i in range(1, (df + 1) // 2):
                total += term
                term *= x / (2 * i + 1)
            return min(total, 1.0)

        for x, df in [(3.841459, 1), (5.991465, 2), (7.814728, 3),
                      (11.070498, 5), (15.507313, 8), (23.684791, 14)]:
            self.assertAlmostEqual(chi2_sf(x, df), 0.05, places=5)

if __name__ == "__main__":
    unittest.main()
