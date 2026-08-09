from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD2Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
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
            privacy_status string,
            upload_status string,
            is_available boolean,
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
        f.col('privacyStatus').alias('privacy_status'),
        f.col('uploadStatus').alias('upload_status'),
        f.lit(True).alias('is_available'),
        f.col('calendar_dt').cast('timestamp').alias('valid_from_dttm')
    )

    sliced_channels = local_bronze_videos.groupBy(f.col('channelId').alias('channel_id')) \
        .agg(f.min(f.col('calendar_dt').cast('timestamp')).alias('missing_from_dttm'))

    known_videos = spark.table('local.silver.l_video_channel').select('video_id', 'channel_id') \
        .join(other=sliced_channels, on='channel_id', how='inner') \
        .join(other=load_videos.select('video_id'), on='video_id', how='left_anti') \
        .join(other=spark.table('local.silver.h_video'), on='video_id', how='inner') \
        .where(f.col('created_at') <= f.col('missing_from_dttm')) \
        .select('video_id', 'missing_from_dttm')

    current_versions = spark.table('local.silver.s_video').where(f.col('valid_to_dttm').isNull())

    missing_videos = known_videos.join(other=current_versions, on='video_id', how='inner') \
        .where(f.col('is_available')) \
        .select(
            *[column for column in load_videos.columns if column not in ('is_available', 'valid_from_dttm')],
            f.lit(False).alias('is_available'),
            f.col('missing_from_dttm').alias('valid_from_dttm')
        )

    to_load = load_videos.unionByName(missing_videos)

    if not to_load.isEmpty():
        loader = SCD2Loader(
            input=to_load,
            output='local.silver.s_video',
            business_key='video_id',
            key_columns=['video_id', 'valid_from_dttm'],
            mode='upsert',
            rebuild_history_mode='from_dt'
            )
        loader.run()
        slicer.commit()
