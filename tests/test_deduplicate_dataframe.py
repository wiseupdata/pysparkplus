import pytest
from pyspark.sql import SparkSession

from pysparkplus.functions import deduplicate_order_by


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-pyspark-local-testing").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def input_dataframe(spark):
    data = [("A", 1, 10), ("A", 2, 20), ("B", 3, 30), ("B", 4, 40), ("B", 5, 50)]
    columns = ["key_col", "order_col1", "order_col2"]
    return spark.createDataFrame(data, columns)


def test_deduplicate(spark, input_dataframe):
    # Arrange
    expected_output_data = [("A", 2, 20), ("B", 5, 50)]
    expected_output_columns = ["key_col", "order_col1", "order_col2"]
    expected_output_dataframe = spark.createDataFrame(expected_output_data, expected_output_columns)

    by_columns = ["key_col"]
    order_by = ["order_col2"]

    # Act
    actual_output_dataframe = deduplicate_order_by(input_dataframe, by_columns, order_by= order_by)

    # Assert
    assert actual_output_dataframe.collect() == expected_output_dataframe.collect()
