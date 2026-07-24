"""Shared helpers for the SAS-to-PySpark migration scripts.

This package centralizes reusable utilities (such as the SparkSession
bootstrap in :mod:`pyspark.common.spark_session`) so individual migration
scripts do not have to duplicate boilerplate.
"""
