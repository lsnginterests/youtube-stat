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
    create table if not exists local.bronze.videos (
        id string,
        calendar_dt date,
        publishedAt timestamp,
        channelId string,
        title string,
        description string,
        categoryId string,
        liveBroadcastContent string,
        defaultLanguage string,
        duration string,
        dimension string,
        definition string,
        caption string,
        licensedContent string,
        contentRating string,
        projection string,
        viewcount bigint,
        likecount bigint,
        favoriteCount bigint,
        commentCount bigint,
        privacyStatus string,
        uploadStatus string,
        processed_dttm timestamp
    )  using iceberg
    partitioned by (calendar_dt)
''')

schema = t.StructType([
    t.StructField('items', t.ArrayType(
        t.StructType([
            t.StructField('id', t.StringType()),
            t.StructField('snippet', t.StructType([
                t.StructField('publishedAt', t.StringType()),
                t.StructField('channelId', t.StringType()),
                t.StructField('title', t.StringType()),
                t.StructField('description', t.StringType()),
                t.StructField('categoryId', t.StringType()),
                t.StructField('liveBroadcastContent', t.StringType()),
                t.StructField('defaultLanguage', t.StringType())
            ])),
            t.StructField('contentDetails', t.StructType([
                t.StructField('duration', t.StringType()),
                t.StructField('dimension', t.StringType()),
                t.StructField('definition', t.StringType()),
                t.StructField('caption', t.StringType()),
                t.StructField('licensedContent', t.StringType()),
                t.StructField('contentRating', t.MapType(t.StringType(), t.StringType())),
                t.StructField('projection', t.StringType())
            ])),
            t.StructField('statistics', t.StructType([
                t.StructField('viewCount', t.StringType()),
                t.StructField('likeCount', t.StringType()),
                t.StructField('favoriteCount', t.StringType()),
                t.StructField('commentCount', t.StringType())
            ])),
            t.StructField('status', t.StructType([
                t.StructField('privacyStatus', t.StringType()),
                t.StructField('uploadStatus', t.StringType())
            ]))
        ])
    ))
])

to_load = spark.read.schema(schema) \
    .option('multiLine', 'true') \
    .option('basePath', f'{dlh.RAW_PATH}/videos') \
    .json(f'{dlh.RAW_PATH}/videos/ingestion_date={ingestion_date}') \
    .select(
        f.col('ingestion_date'),
        f.explode('items').alias('i')
    ) \
    .select(
        f.col('i.id'),
        f.to_date(f.col('ingestion_date')).alias('calendar_dt'),
        f.to_timestamp(f.col('i.snippet.publishedAt')).alias('publishedAt'),
        f.col('i.snippet.channelId'),
        f.col('i.snippet.title'),
        f.col('i.snippet.description'),
        f.col('i.snippet.categoryId'),
        f.col('i.snippet.liveBroadcastContent'),
        f.col('i.snippet.defaultLanguage'),
        f.col('i.contentDetails.duration'),
        f.col('i.contentDetails.dimension'),
        f.col('i.contentDetails.definition'),
        f.col('i.contentDetails.caption'),
        f.col('i.contentDetails.licensedContent'),
        f.to_json(f.col('i.contentDetails.contentRating')).alias('contentRating'),
        f.col('i.contentDetails.projection'),
        f.col('i.statistics.viewCount').cast('bigint').alias('viewCount'),
        f.col('i.statistics.likeCount').cast('bigint').alias('likeCount'),
        f.col('i.statistics.favoriteCount').cast('bigint').alias('favoriteCount'),
        f.col('i.statistics.commentCount').cast('bigint').alias('commentCount'),
        f.col('i.status.privacyStatus'),
        f.col('i.status.uploadStatus')
    )

if not to_load.isEmpty():
    loader = SCD1Loader(to_load, 'local.bronze.videos', ['id', 'calendar_dt'], 'upsert')
    loader.run()

spark.stop()
