"""
A boilerplate conftest for sample-project
"""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def data(spark_session: SparkSession) -> DataFrame:
    """
    This is a boilerplate fixture to generate mock data.

    :param spark_session: a SparkSession instance injected by pytest-spark.
    :returns a Spark DataFrame
    """
    return spark_session.createDataFrame(
        [("Alice", 1), ("Bob", 2), ("Eve", 3)], ["name", "age"]
    )


def assert_shallow_equals(ddf: DataFrame, other_ddf: DataFrame) -> None:
    """
    Assert equality of two DataFrames.

    This method performs a shallow comparison on column names and
    rows content. Column types are not compared.

    :param ddf: a Spark DataFrame
    :param other_ddf: a Spark DataFrame
    :return None
    """
    assert len(set(ddf.columns).difference(set(other_ddf.columns))) == 0
    assert ddf.subtract(other_ddf).rdd.isEmpty()
    assert other_ddf.subtract(ddf).rdd.isEmpty()
