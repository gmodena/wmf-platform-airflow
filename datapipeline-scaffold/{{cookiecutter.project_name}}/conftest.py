import pytest
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def data(spark_session):
    return spark_session.createDataFrame(
        [("Alice", 1), ("Bob", 2), ("Eve", 3)], ["name", "age"]
    )


def assert_shallow_equals(ddf: DataFrame, other_ddf: DataFrame) -> None:
    assert len(set(ddf.columns).difference(set(other_ddf.columns))) == 0
    assert ddf.subtract(other_ddf).rdd.isEmpty()
    assert other_ddf.subtract(ddf).rdd.isEmpty()
