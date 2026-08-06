from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry

spark = get_spark()
registry = SliceRegistry('load_silver_h_video')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.videos',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.silver.h_video (
    video_id string,
    created_at timestamp,
    processed_dttm timestamp
    ) using iceberg
    partitioned by (years(created_at))
''')

local_bronze_videos = slicer.run()
load_videos = local_bronze_videos.select(
    f.col('id').alias('video_id'),
    f.col('publishedAt').alias('created_at')
)

merged = load_videos.join(
    spark.table('local.silver.h_video'),
    on='video_id',
    how='leftanti'
).select(
    'video_id',
    'created_at'
)

to_load = merged.groupBy('video_id') \
    .agg(f.min(f.col('created_at')).alias('created_at'))

if not to_load.isEmpty():
    loader = SCD1Loader(to_load, 'local.silver.h_video', ['video_id'], 'upsert')
    loader.run()
    slicer.commit()

spark.stop()