from datetime import date

from pyspark.sql import functions as f
from pyspark.sql import types as t

from dataplatform.setting.spark_session import get_spark
from dataplatform.setting.dlh_config import DLHConfig
from dataplatform.etl_tools import SCD1Loader


def run(ingestion_date: str | None = None) -> None:
    ingestion_date = ingestion_date or str(date.today())

    spark = get_spark()
    dlh = DLHConfig(spark)

    spark.sql('''
        create table if not exists local.bronze.channels (
        id string,
        calendar_dt date,
        title string,
        description string,
        customUrl string,
        publishedAt timestamp,
        country string,
        subscriberCount integer,
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
                    t.StructField('description', t.StringType()),
                    t.StructField('customUrl', t.StringType()),
                    t.StructField('publishedAt', t.StringType()),
                    t.StructField('country', t.StringType())
                ])),
                t.StructField('statistics', t.StructType([
                    t.StructField('subscriberCount', t.StringType())
                ]))
            ])
        ))
    ])

    to_load = spark.read.schema(schema) \
        .option('multiline', 'true') \
        .option('basePath', f'{dlh.RAW_PATH}/channels') \
        .json(f'{dlh.RAW_PATH}/channels/ingestion_date={ingestion_date}') \
        .select(
            f.col('ingestion_date'),
            f.explode('items').alias('i')
            ) \
        .select(
            f.col('i.id'),
            f.to_date(f.col('ingestion_date')).alias('calendar_dt'),
            f.col('i.snippet.title'),
            f.col('i.snippet.description'),
            f.col('i.snippet.customUrl'),
            f.to_timestamp('i.snippet.publishedAt').alias('publishedAt'),
            f.col('i.snippet.country'),
            f.col('i.statistics.subscriberCount').cast('int').alias('subscriberCount')
        )

    if not to_load.isEmpty():
        loader = SCD1Loader(to_load, 'local.bronze.channels', ['id', 'calendar_dt'], 'upsert')
        loader.run()
