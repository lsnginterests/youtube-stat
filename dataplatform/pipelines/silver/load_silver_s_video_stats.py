from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_silver_s_video_stats')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.bronze.videos',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.silver.s_video_stats (
            video_id string,
            calendar_dt date,
            views_cnt bigint,
            likes_cnt bigint,
            favorites_cnt bigint,
            comments_cnt bigint,
            processed_dttm timestamp
        ) using iceberg
        partitioned by (calendar_dt)
    ''')

    local_bronze_videos = slicer.run()
    load_stats = local_bronze_videos.select(
        f.col('id').alias('video_id'),
        f.col('calendar_dt'),
        f.col('viewCount').alias('views_cnt'),
        f.col('likeCount').alias('likes_cnt'),
        f.col('favoriteCount').alias('favorites_cnt'),
        f.col('commentCount').alias('comments_cnt')
    )

    if not load_stats.isEmpty():
        loader = SCD1Loader(load_stats, 'local.silver.s_video_stats', ['video_id', 'calendar_dt'], 'upsert')
        loader.run()
    slicer.commit()
