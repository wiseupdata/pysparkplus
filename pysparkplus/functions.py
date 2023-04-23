from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, row_number
from strplus import Str


def count_cols(df:DataFrame):
    return len(df.columns)


def deduplicate(df: DataFrame, by_columns:  Optional[List[str] or str] = None , order_by:  Optional[List[str] or str] = None, desc_:bool = True) -> DataFrame:

    if count_cols(df) == 1:
        # Native spark function!
        return df.distinct()
    
    elif order_by is None or len(order_by) ==0:
        # Native spark function!
        df.dropDuplicates(subset=columns)
        
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
