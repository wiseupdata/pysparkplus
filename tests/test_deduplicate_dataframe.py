import pytest
from pyspark.sql import SparkSession

from pysparkplus.dataframe import deduplicate


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

    key_columns = ["key_col"]
    order_columns = ["order_col2"]

    # Act
    actual_output_dataframe = deduplicate(input_dataframe, key_columns, order_columns)

    # Assert
    assert actual_output_dataframe.collect() == expected_output_dataframe.collect()