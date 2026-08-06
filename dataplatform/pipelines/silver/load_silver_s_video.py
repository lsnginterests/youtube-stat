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
        is_stream string,
        default_language string,
        duration string,
        dimension string,
        definition string,
        caption string,
        license string,
        content_rating string,
        projection string,
        valid_from_dttm timestamp,
        valid_to_dttm timestamp,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (days(valid_from_dttm))
''')

local_bronze_videos = slicer.run()
load_videos = local_bronze_videos.select(
    f.col('id').alias('video_id'),
    f.col('title').alias('title'),
    f.col('description').alias('description'),
    f.col('categoryid').alias('category_id'),
    f.col('livebroadcastcontent').alias('is_stream'),
    f.col('defaultlanguage').alias('default_language'),
    f.col('duration').alias('duration'),
    f.col('dimension').alias('dimension'),
    f.col('definition').alias('definition'),
    f.col('caption').alias('caption'),
    f.col('licensedcontent').alias('license'),
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