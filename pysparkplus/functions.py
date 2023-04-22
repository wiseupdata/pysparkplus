from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number
from strplus import Str


def deduplicate(df: DataFrame, by_columns, order_by=None) -> DataFrame:
    """
    
    Returns a DataFrame with duplicate rows removed based on the given columns.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        by_columns (Union[str, List[str]]): A column or list of columns to group by for deduplication.
        order_by (Optional[Union[str, List[str]]]): A column or list of columns to order by before deduplication. If not
            specified, the deduplication will be performed based on the `by_columns` parameter.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with duplicate rows removed.

    !!! Example "Deduplicating a DataFrame"
        This example shows how to use `deduplicate()` to remove duplicate rows from a DataFrame.

        === "Original df"
            ```python
            df = spark.createDataFrame([(1, "a"), (2, "b"), (1, "a"), (3, "c")], ["col1", "col2"])
            df.show()
            ```
            Output:
            ```
            +----+----+
            |col1|col2|
            +----+----+
            |   1|   a|
            |   2|   b|
            |   1|   a|
            |   3|   c|
            +----+----+
            ```

        === "Example 1"
            ```python
            df_dedup = deduplicate(df, "col1")
            df_dedup.show()
            ```
            Output:
            ```
            +----+----+
            |col1|col2|
            +----+----+
            |   1|   a|
            |   2|   b|
            |   3|   c|
            +----+----+
            ```

        === "Example 2"
            ```python
            df_dedup = deduplicate(df, ["col1", "col2"], order_by="col1")
            df_dedup.show()
            ```
            Output:
            ```
            +----+----+
            |col1|col2|
            +----+----+
            |   1|   a|
            |   2|   b|
            |   3|   c|
            +----+----+
            ```

    Info: Important
        - This function preserves the first occurrence of each unique row and removes subsequent duplicates.
        - If there are no duplicate rows in the DataFrame, this function returns the input DataFrame unchanged.
        - The `order_by` parameter can be used to specify a custom order for the deduplication. By default, the function
          orders by the columns specified in the `by_columns` parameter.
        - The input DataFrame should not contain null values, as these may cause unexpected behavior in the deduplication.
    """
    order_by = by_columns if order_by is None else order_by

    order_by = Str(order_by) if isinstance(order_by, str) else order_by

    if len(df.columns) == 1:
        return df.distinct()

    else:
        order_by = ",".join(order_by) if isinstance(order_by, list) else order_by

        # Create a window specification to partition by key columns and order by order columns in descending order
        window_spec = Window.partitionBy(by_columns).orderBy(desc(order_by))

        # Add a new column called "row_num" to the DataFrame based on the window specification
        df_num = df.withColumn("row_num", row_number().over(window_spec))

        # Filter the DataFrame to keep only rows where the "row_num" column equals 1
        df_dedup = df_num.filter(df_num.row_num == 1)

        return df_dedup.drop("row_num")


def count_cols(df: DataFrame):
    return len(df.columns)
