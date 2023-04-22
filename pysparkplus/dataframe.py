from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number

# class WrappedDict(dict):
#     def __init__(self, wrapped_dict):
#         if not isinstance(wrapped_dict, dict):
#             raise TypeError("wrapped_dict must be a dictionary")
#         super().__init__()
#         self.update(wrapped_dict)

#     def __getattr__(self, attr):
#         return getattr(self, attr)

#     def __getitem__(self, key):
#         item = super().__getitem__(key)
#         if isinstance(item, dict):
#             return WrappedDict(item)
#         return item

#     def __instancecheck__(self, instance):
#         return isinstance(instance, dict)

#     def __subclasscheck__(self, subclass):
#         return issubclass(subclass, dict)

#     def get_attributes(self, attribute_regex):
#         return get_attributes(self, attribute_regex)


def deduplicate(dataframe: DataFrame, key_columns: List[str], order_columns: List[str]) -> DataFrame:
    """
    This function removes duplicates from a Spark DataFrame based on a combination of columns,
    while retaining only the rows with the highest values in a specified column.

    Args:
        dataframe (DataFrame): The input Spark DataFrame to be deduplicated.
        key_columns (List[str]): The list of column names to be used as keys for deduplication.
        order_columns (List[str]): The list of column names to be used for sorting rows in descending order.

    Returns:
        DataFrame: A new Spark DataFrame that removes duplicates based on the values in the key_columns
        and keeps only the row with the highest value in the order_columns.
    """
    # Check if input parameters are valid
    if not isinstance(dataframe, DataFrame):
        raise ValueError("Input parameter 'dataframe' should be of type DataFrame.")
    if not isinstance(key_columns, list):
        raise ValueError("Input parameter 'key_columns' should be of type List[str].")
    if not isinstance(order_columns, list):
        raise ValueError("Input parameter 'order_columns' should be of type List[str].")
    if len(key_columns) == 0:
        raise ValueError("Input parameter 'key_columns' should not be an empty list.")
    if len(order_columns) == 0:
        raise ValueError("Input parameter 'order_columns' should not be an empty list.")

    # Create a window specification to partition by key columns and order by order columns in descending order
    window_spec = Window.partitionBy(*key_columns).orderBy(desc(*order_columns))

    # Add a new column called "row_num" to the DataFrame based on the window specification
    deduplicated_dataframe = dataframe.withColumn("row_num", row_number().over(window_spec))

    # Filter the DataFrame to keep only rows where the "row_num" column equals 1
    deduplicated_dataframe = deduplicated_dataframe.filter(deduplicated_dataframe.row_num == 1)

    # Drop the "row_num" column from the DataFrame
    deduplicated_dataframe = deduplicated_dataframe.drop("row_num")

    return deduplicated_dataframe
