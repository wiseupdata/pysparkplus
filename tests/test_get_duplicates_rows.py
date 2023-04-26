import pytest
from pyspark.sql import SparkSession

from pysparkplus.functions import get_duplicated_rows


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing"""
    spark = SparkSession.builder.appName("pytest").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def test_data(spark):
    """Create a test DataFrame"""
    df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe"), (3, "John", "Doe"), (4, "John", "Doe"), (5, "Jane", "Doe")], ["id", "first_name", "last_name"])
    return df


def test_get_duplicate_rows(test_data):
    """Test the get_duplicated_rows function"""
    result = get_duplicated_rows(test_data, ["first_name", "last_name"])
    print(result.show())
    assert len(result.collect()) == 2
    assert result.orderBy("first_name").collect()[0]["count"] == 2
    assert result.orderBy("first_name").collect()[1]["count"] == 3


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing"""
    spark = SparkSession.builder.appName("pytest").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def test_data_2(spark):
    """Create a test DataFrame"""
    df = spark.createDataFrame(
        [
            (1, "John", "Doe"),
            (2, "Jane", "Doe"),
            (3, "John", "Doe"),
            (4, "John", "Doe"),
            (5, "Jane", "Doe"),
            (6, "Bob", "Smith"),
            (7, "Bob", "Smith"),
            (8, "Alice", "Jones"),
            (9, "Alice", "Jones"),
            (10, "Eve", "Jackson"),
        ],
        ["id", "first_name", "last_name"],
    )
    return df

def test_get_duplicate_rows_output_type(test_data_2):
    """Test the output type of the get_duplicated_rows function"""
    result = get_duplicated_rows(test_data_2, ["first_name", "last_name"])
    assert isinstance(result, DataFrame)

def test_get_duplicate_rows_output_columns(test_data_2):
    """Test the output columns of the get_duplicated_rows function"""
    result = get_duplicated_rows(test_data_2, ["first_name", "last_name"])
    assert result.columns == ["first_name", "last_name", "count"]

def test_get_duplicate_rows_output_values(test_data_2):
    """Test the output values of the get_duplicated_rows function"""
    result = get_duplicated_rows(test_data_2, ["first_name", "last_name"])
    assert result.collect() == [
        ("John", "Doe", 2),
        ("Alice", "Jones", 2),
        ("Bob", "Smith", 2),
        ("Jane", "Doe", 2),
    ]

def test_get_duplicate_rows_empty_input(test_data_2):
    """Test the get_duplicated_rows function with an empty DataFrame"""
    empty_df = test_data_2.schema.emptyDataFrame
    result = get_duplicated_rows(empty_df, ["first_name", "last_name"])
    assert result.collect() == []

def test_get_duplicate_rows_empty_columns(test_data_2):
    """Test the get_duplicated_rows function with an empty list of columns"""
    result = get_duplicated_rows(test_data_2, [])
    assert result.collect() == []

def test_get_duplicate_rows_single_column(test_data_2):
    """Test the get_duplicated_rows function with a single column"""
    result = get_duplicated_rows(test_data_2, "first_name")
    assert result.collect() == [
        ("Alice", 2),
        ("Bob", 2),
        ("Jane", 2),
        ("John", 2),
        ("Eve", 1),
    ]

def test_get_duplicate_rows_with_null_values(test_data_2):
    """Test the get_duplicated_rows function with null values in the DataFrame"""
    df_with_null = test_data_2.union(spark.createDataFrame([(11, None, None)], ["id", "first_name", "last_name"]))
    result = get_duplicated_rows(df_with_null, ["first_name", "last_name"])
    assert result.collect() == [
        (None, None, 1),
        ("John", "Doe", 2),
        ("Alice", "Jones", 2),
        ("Bob", "Smith", 2),
        ("Jane", "Doe", 2),
    ]

def test_get_duplicate_rows_with_non_string_columns(test_data_2):
    """Test the get_duplicated_rows
function with non-string columns in the DataFrame"""
    df_with_non_string = spark.createDataFrame([(1, 2, "John"), (2, 3, "Jane"), (3, 4, "John")], ["id", "age", "name"])
    result = get_duplicated_rows(df_with_non_string, "name")
    assert result.collect() == [("John", 2), ("Jane", 1)]

def test_get_duplicate_rows_with_invalid_column(test_data_2):
    """Test the get_duplicated_rows function with an invalid column name"""
    with pytest.raises(Exception):
        get_duplicated_rows(test_data_2, "invalid_column")

def test_get_duplicate_rows_with_duplicate_columns(test_data_2):
    """Test the get_duplicated_rows function with duplicate column names"""
    result = get_duplicated_rows(test_data_2, ["first_name", "first_name"])
    assert result.collect() == [("John", "Doe", 2), ("Jane", "Doe", 2), ("Bob", "Smith", 2), ("Alice", "Jones", 2)]

def test_get_duplicate_rows_with_integer_columns(test_data_2):
    """Test the get_duplicated_rows function with integer column names"""
    df_with_int_columns = test_data_2.selectExpr("id as 1", "first_name as 2", "last_name as 3")
    result = get_duplicated_rows(df_with_int_columns, ["2", "3"])
    assert result.collect() == [("John", "Doe", 2), ("Alice", "Jones", 2), ("Bob", "Smith", 2), ("Jane", "Doe", 2)]

def test_get_duplicate_rows_with_boolean_columns(test_data_2):
    """Test the get_duplicated_rows function with boolean column names"""
    df_with_bool_columns = test_data_2.selectExpr("id", "true as is_doe")
    result = get_duplicated_rows(df_with_bool_columns, "is_doe")
    assert result.collect() == [(True, 4), (False, 1)]

def test_get_duplicate_rows_with_nonexistent_columns(test_data_2):
    """Test the get_duplicated_rows function with nonexistent column names"""
    with pytest.raises(Exception):
        get_duplicated_rows(test_data_2, ["nonexistent_column1", "nonexistent_column2"])

def test_get_duplicate_rows_with_non_hashable_columns(test_data_2):
    """Test the get_duplicated_rows function with non-hashable column types"""
    from datetime import datetime
    df_with_non_hashable = spark.createDataFrame([(1, datetime.now()), (2, datetime.now()), (3, datetime.now())], ["id", "timestamp"])
    with pytest.raises(TypeError):
        get_duplicated_rows(df_with_non_hashable, "timestamp")

def test_get_duplicate_rows_with_nonexistent_dataframe(test_data_2):
    """Test the get_duplicated_rows function with nonexistent DataFrame"""
    with pytest.raises(Exception):
        get_duplicated_rows(None, ["first_name", "last_name"])