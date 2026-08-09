from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_silver_s_channel_stats')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.bronze.channels',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.silver.s_channel_stats (
            channel_id string,
            calendar_dt date,
            subscribers_cnt bigint,
            processed_dttm timestamp
        ) using iceberg
        partitioned by (calendar_dt)
    ''')

    local_bronze_channels = slicer.run()
    load_stats = local_bronze_channels.select(
        f.col('id').alias('channel_id'),
        f.col('calendar_dt'),
        f.col('subscriberCount').alias('subscribers_cnt')
    )

    if not load_stats.isEmpty():
        loader = SCD1Loader(load_stats, 'local.silver.s_channel_stats', ['channel_id', 'calendar_dt'], 'upsert')
        loader.run()
    slicer.commit()
