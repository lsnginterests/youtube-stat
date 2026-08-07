from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD2Loader
from dataplatform.etl_tools import Slicer, SliceRegistry

spark = get_spark()
registry = SliceRegistry('load_silver_s_video')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.videos',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.silver.s_video (
        video_id string,
        title string,
        description string,
        category_id string,
        live_broadcast_content string,
        default_language string,
        duration_sec int,
        dimension string,
        definition string,
        caption boolean,
        license boolean,
        content_rating string,
        projection string,
        valid_from_dttm timestamp,
        valid_to_dttm timestamp,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (days(valid_from_dttm))
''')

DURATION_PATTERN = r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$'

duration_sec = f.when(
    f.col('duration').rlike(DURATION_PATTERN),
    f.coalesce(f.regexp_extract('duration', DURATION_PATTERN, 1).cast('int'), f.lit(0)) * 3600
    + f.coalesce(f.regexp_extract('duration', DURATION_PATTERN, 2).cast('int'), f.lit(0)) * 60
    + f.coalesce(f.regexp_extract('duration', DURATION_PATTERN, 3).cast('int'), f.lit(0))
)

local_bronze_videos = slicer.run()
load_videos = local_bronze_videos.select(
    f.col('id').alias('video_id'),
    f.col('title').alias('title'),
    f.col('description').alias('description'),
    f.col('categoryid').alias('category_id'),
    f.col('livebroadcastcontent').alias('live_broadcast_content'),
    f.col('defaultlanguage').alias('default_language'),
    duration_sec.alias('duration_sec'),
    f.col('dimension').alias('dimension'),
    f.col('definition').alias('definition'),
    f.col('caption').cast('boolean').alias('caption'),
    f.col('licensedcontent').cast('boolean').alias('license'),
    f.col('contentrating').alias('content_rating'),
    f.col('projection').alias('projection'),
    f.col('calendar_dt').cast('timestamp').alias('valid_from_dttm')
)

if not load_videos.isEmpty():
    loader = SCD2Loader(
        input=load_videos,
        output='local.silver.s_video',
        business_key='video_id',
        key_columns=['video_id', 'valid_from_dttm'],
        mode='upsert',
        rebuild_history_mode='from_dt'
        )
    loader.run()
    slicer.commit()

spark.stop()