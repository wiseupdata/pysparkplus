from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number
from strplus import Str


def count_cols(df:DataFrame):
    return len(df.columns)

def deduplicate(df: DataFrame, columns: Optional[List[str] or str] = None) -> DataFrame:
    return df.dropDuplicates(subset=columns)


def deduplicate_order_by(df: DataFrame, by_columns: List[str] or str, order_by: List[str] or str, desc_:bool = True) -> DataFrame:

    if count_cols(df) == 1:
        return df.distinct()

    else:
        
        columns = Str(by_columns).split_by_sep if isinstance(by_columns, str) else by_columns
        order_by_cols = Str(order_by) if isinstance(order_by, str) else Str(",".join(order_by))

        # Create a window specification to partition by key columns and order by order columns in descending order
        window_spec = Window.partitionBy(by_columns).orderBy(desc(order_by_cols.sep_to_comma))

        # Add a new column called "row_num" to the DataFrame based on the window specification
        df_num = df.withColumn("row_num", row_number().over(window_spec))

        # Filter the DataFrame to keep only rows where the "row_num" column equals 1
        df_dedup = df_num.filter(df_num.row_num == 1)

        return df_dedup.drop("row_num")
