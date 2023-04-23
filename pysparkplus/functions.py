from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number
from strplus import Str


def deduplicate(df: DataFrame, columns: Optional[List[str] or str] = None) -> DataFrame:
    return df.dropDuplicates(subset=columns)


def deduplicate_order_by(df: DataFrame, by_columns, order_by) -> DataFrame:
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
