from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

def collapse(dataframe: DataFrame, key_columns: list, compare_columns: list) -> DataFrame:
    if 'valid_from_dttm' not in dataframe.columns:
                raise ValueError(f'dataframe {dataframe} must have valid_from_dttm column')

    columns = [f.coalesce(f.col(col), f.lit('none')) for col in compare_columns if col != 'valid_from_dttm' and col not in key_columns]
    w = Window.partitionBy(*key_columns).orderBy(f.col('valid_from_dttm').asc())
    df = dataframe.withColumn('hashkey', f.md5(f.concat_ws('||', *columns))) \
        .withColumn('lag_hashkey', f.lag(f.col('hashkey')).over(w)) \
        .where((f.col('hashkey') != f.col('lag_hashkey')) | (f.col('lag_hashkey').isNull()))
    return df.select(*[f.col(col) for col in df.columns if col not in ('hashkey', 'lag_hashkey')])