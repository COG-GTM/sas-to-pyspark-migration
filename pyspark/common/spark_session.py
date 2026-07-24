"""Reusable SparkSession bootstrap for the migration scripts.

Every migration script needs a local ``SparkSession`` configured the same way.
Rather than repeating the ``SparkSession.builder...getOrCreate()`` block in each
script, import :func:`get_spark` from here.

Example
-------
>>> from pyspark.common.spark_session import get_spark
>>> spark = get_spark("HomeEquity_DataLoading")
>>> df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
>>> spark.stop()
"""

from pyspark.sql import SparkSession

# A small, fixed shuffle-partition count keeps local runs fast; the dataset is
# tiny (~6k rows) so the Spark default of 200 partitions just adds overhead.
DEFAULT_SHUFFLE_PARTITIONS = 8


def get_spark(app_name: str, *, master: str = "local[*]") -> SparkSession:
    """Build (or return the existing) local ``SparkSession``.

    Parameters
    ----------
    app_name:
        Application name shown in the Spark UI and logs.
    master:
        Spark master URL. Defaults to ``"local[*]"`` (use all local cores),
        which is appropriate for running these demo scripts on a single machine.

    Returns
    -------
    pyspark.sql.SparkSession
        A ready-to-use session. Because Spark reuses a single active session per
        JVM, calling this repeatedly returns the same session (with the original
        configuration). Remember to call ``spark.stop()`` when finished.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", DEFAULT_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
