from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import Slicer, SliceRegistry
from dataplatform.etl_tools import SCD1Loader

spark = get_spark()
registry = SliceRegistry('load_silver_l_video_channel')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.videos',
    slice_column='processed_dttm'
)

spark.sql('''
create table if not exists local.silver.l_video_channel (
    link_id string,
    video_id string,
    channel_id string,
    created_at timestamp,
    processed_dttm timestamp
) using iceberg
partitioned by (years(created_at))
''')

load_bronze_videos = slicer.run()
to_load = load_bronze_videos.select(
    f.col('id').alias('video_id'),
    f.col('channelId').alias('channel_id'),
    f.col('publishedAt').alias('created_at')
    ) \
    .groupBy('video_id', 'channel_id').agg(f.min('created_at').alias('created_at')) \
    .withColumn('link_id', f.md5(f.concat_ws('||', f.col('video_id'), f.col('channel_id')))) \
    .join(
        other=spark.table('local.silver.l_video_channel'),
        on='link_id',
        how='leftanti'
)

if not to_load.isEmpty():
    loader = SCD1Loader(
        input=to_load,
        output='local.silver.l_video_channel',
        key_columns=['link_id'],
        mode='upsert'
    )
    loader.run()
    slicer.commit()

spark.stop()