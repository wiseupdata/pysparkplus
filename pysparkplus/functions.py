from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number
from strplus import Str
from strplus.functions import split_by_separator


def count_cols(df: DataFrame):
    return len(df.columns)


def deduplicate(df: DataFrame, by_columns: Optional[List[str] or str] = None, order_by: Optional[List[str] or str] = None, desc_: bool = True) -> DataFrame:
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

    if count_cols(df) == 1:
        # Native spark function!
        return df.distinct()

    elif order_by is None or len(order_by) == 0:
        # Native spark function!
        df.dropDuplicates(subset=columns)

    else:
        columns = split_by_separator(by_columns, type_constraint=False)
        order_by_cols = Str.cast(order_by)

        # Create a window specification to partition by key columns and order by order columns in descending order
        window_spec = Window.partitionBy(columns).orderBy(desc(order_by_cols.separator_as_comma))

        # Add a new column called "row_num" to the DataFrame based on the window specification
        df_num = df.withColumn("row_num", row_number().over(window_spec))

        # Filter the DataFrame to keep only rows where the "row_num" column equals 1
        df_dedup = df_num.filter(df_num.row_num == 1)

        return df_dedup.drop("row_num")


def get_duplicated_rows(df: DataFrame, by_columns: Optional[List[str] or str] = None):
    columns: List[str] = split_by_separator(input_string=by_columns, type_constraint=False)
    columns: List[F.col] = [F.col(c) for c in columns]
    result: DataFrame(jdf, sql_ctx) = df.groupBy(columns).agg(F.count("*").alias("count"))
    return result
