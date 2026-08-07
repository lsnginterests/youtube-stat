from pyspark.sql import Window
from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry

spark = get_spark()
registry = SliceRegistry('load_gold_fct_video_daily')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.silver.s_video_stats',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.gold.fct_video_daily (
        video_id string,
        channel_id string,
        calendar_dt date,
        views_cnt bigint,
        likes_cnt bigint,
        favorites_cnt bigint,
        comments_cnt bigint,
        views_delta bigint,
        likes_delta bigint,
        comments_delta bigint,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (calendar_dt)
''')

GRAIN_COLUMNS = ['video_id', 'channel_id', 'calendar_dt']
MEASURE_COLUMNS = ['views_cnt', 'likes_cnt', 'favorites_cnt', 'comments_cnt']

local_silver_l_video_channel = spark.table('local.silver.l_video_channel').select('video_id', 'channel_id')
local_silver_s_video_stats = slicer.run()

load_stats = local_silver_l_video_channel.join(
    other=local_silver_s_video_stats,
    on='video_id',
    how='inner'
).select(*GRAIN_COLUMNS, *MEASURE_COLUMNS)

boundary = load_stats.groupBy('video_id').agg(f.min('calendar_dt').alias('boundary_dt'))
last_loaded = Window.partitionBy('video_id').orderBy(f.col('calendar_dt').desc())

previous_stats = spark.table('local.gold.fct_video_daily') \
    .join(boundary, on='video_id', how='inner') \
    .where(f.col('calendar_dt') < f.col('boundary_dt')) \
    .withColumn('rn', f.row_number().over(last_loaded)) \
    .where(f.col('rn') == 1) \
    .select(*GRAIN_COLUMNS, *MEASURE_COLUMNS)

daily = Window.partitionBy('video_id').orderBy(f.col('calendar_dt').asc())

to_load = load_stats.unionByName(previous_stats) \
    .withColumn('views_delta', f.col('views_cnt') - f.lag('views_cnt').over(daily)) \
    .withColumn('likes_delta', f.col('likes_cnt') - f.lag('likes_cnt').over(daily)) \
    .withColumn('comments_delta', f.col('comments_cnt') - f.lag('comments_cnt').over(daily)) \
    .join(load_stats.select('video_id', 'calendar_dt'), on=['video_id', 'calendar_dt'], how='left_semi')

if not to_load.isEmpty():
    loader = SCD1Loader(to_load, 'local.gold.fct_video_daily', ['video_id', 'calendar_dt'], 'upsert')
    loader.run()
    slicer.commit()

spark.stop()
