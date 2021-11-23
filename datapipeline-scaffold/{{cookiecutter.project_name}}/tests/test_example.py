from conftest import assert_shallow_equals
from pyspark import Row


def test_dataframe_equals(spark_session, data):
    expected_df = spark_session.createDataFrame(
        [("Alice", 1), ("Bob", 2), ("Eve", 3)], ["name", "age"]
    )
    assert_shallow_equals(data, expected_df)
