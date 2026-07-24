"""Sanity checks for the shared SparkSession helper.

Kept separate from ``test_pyspark_outputs.py`` so it exercises only
``pyspark.common.spark_session.get_spark`` without touching the migration
parity tests.
"""

import unittest

from pyspark.sql import SparkSession

from pyspark.common.spark_session import DEFAULT_SHUFFLE_PARTITIONS, get_spark


class TestSparkSessionHelper(unittest.TestCase):
    """Verify get_spark builds a configured, reusable local SparkSession."""

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("SparkSessionHelper_Tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_returns_spark_session(self):
        self.assertIsInstance(self.spark, SparkSession)

    def test_app_name_is_set(self):
        self.assertEqual(
            self.spark.conf.get("spark.app.name"),
            "SparkSessionHelper_Tests",
        )

    def test_shuffle_partitions_default(self):
        self.assertEqual(
            self.spark.conf.get("spark.sql.shuffle.partitions"),
            str(DEFAULT_SHUFFLE_PARTITIONS),
        )

    def test_get_spark_is_idempotent(self):
        """Repeated calls reuse the single active session per JVM."""
        again = get_spark("Ignored_Name")
        self.assertIs(again, self.spark)

    def test_session_can_run_a_query(self):
        df = self.spark.range(5)
        self.assertEqual(df.count(), 5)


if __name__ == "__main__":
    unittest.main()
