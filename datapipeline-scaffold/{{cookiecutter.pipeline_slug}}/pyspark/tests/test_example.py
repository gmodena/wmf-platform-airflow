"""
A boilerplate test suite for {{cookiecutter.pipeline_name}}
"""
from conftest import assert_shallow_equals


def test_dataframe_equals(spark_session, data):
    """
    This is a boilerplate unit test.

    assert_shallow_equals can be used
    to test that the result of a transformation matches t
    he expected output.
    """
    expected_df = spark_session.createDataFrame(
        [("Alice", 1), ("Bob", 2), ("Eve", 3)], ["name", "age"]
    )
    assert_shallow_equals(data, expected_df)
