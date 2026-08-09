from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_gold_fct_channel_daily')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.gold.fct_video_daily',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.gold.fct_channel_daily (
            channel_id string,
            calendar_dt date,
            subscribers_cnt bigint,
            videos_cnt bigint,
            views_total bigint,
            likes_total bigint,
            comments_total bigint,
            views_delta bigint,
            likes_delta bigint,
            comments_delta bigint,
            processed_dttm timestamp
        ) using iceberg
        partitioned by (calendar_dt)
    ''')

    local_gold_fct_video_daily = spark.table('local.gold.fct_video_daily')
    local_silver_s_channel_stats = spark.table('local.silver.s_channel_stats')
    sliced_fct_video_daily = slicer.run()

    affected_days = sliced_fct_video_daily.select('channel_id', 'calendar_dt').distinct()

    video_totals = local_gold_fct_video_daily.join(
        other=affected_days,
        on=['channel_id', 'calendar_dt'],
        how='inner'
    ).groupBy('channel_id', 'calendar_dt').agg(
        f.count('video_id').alias('videos_cnt'),
        f.sum('views_cnt').alias('views_total'),
        f.sum('likes_cnt').alias('likes_total'),
        f.sum('comments_cnt').alias('comments_total'),
        f.sum('views_delta').alias('views_delta'),
        f.sum('likes_delta').alias('likes_delta'),
        f.sum('comments_delta').alias('comments_delta')
    )

    to_load = video_totals.join(
        other=local_silver_s_channel_stats,
        on=['channel_id', 'calendar_dt'],
        how='inner'
    ).select(
        'channel_id',
        'calendar_dt',
        'subscribers_cnt',
        'videos_cnt',
        'views_total',
        'likes_total',
        'comments_total',
        'views_delta',
        'likes_delta',
        'comments_delta'
    )

    if not to_load.isEmpty():
        loader = SCD1Loader(to_load, 'local.gold.fct_channel_daily', ['channel_id', 'calendar_dt'], 'upsert')
        loader.run()
        slicer.commit()
