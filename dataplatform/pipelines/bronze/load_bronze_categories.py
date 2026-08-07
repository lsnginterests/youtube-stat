import sys
from datetime import date

from pyspark.sql import functions as f
from pyspark.sql import types as t

from dataplatform.setting.spark_session import get_spark
from dataplatform.setting.dlh_config import DLHConfig
from dataplatform.etl_tools import SCD1Loader

ingestion_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())

spark = get_spark()
dlh = DLHConfig(spark)

spark.sql('''
    create table if not exists local.bronze.categories (
        id string,
        calendar_dt date,
        title string,
        assignable boolean,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (calendar_dt)
''')

schema = t.StructType([
    t.StructField('items', t.ArrayType(
        t.StructType([
            t.StructField('id', t.StringType()),
            t.StructField('snippet', t.StructType([
                t.StructField('title', t.StringType()),
                t.StructField('assignable', t.BooleanType())
            ]))
        ])
    ))
])

to_load = spark.read.schema(schema) \
    .option('multiLine', 'true') \
    .option('basePath', f'{dlh.RAW_PATH}/categories') \
    .json(f'{dlh.RAW_PATH}/categories/ingestion_date={ingestion_date}') \
    .select(
        f.col('ingestion_date'),
        f.explode('items').alias('i')
    ) \
    .select(
        f.col('i.id'),
        f.to_date(f.col('ingestion_date')).alias('calendar_dt'),
        f.col('i.snippet.title'),
        f.col('i.snippet.assignable')
    )

if not to_load.isEmpty():
    loader = SCD1Loader(to_load, 'local.bronze.categories', ['id', 'calendar_dt'], 'upsert')
    loader.run()

spark.stop()
