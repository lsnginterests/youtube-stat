from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools.slicer.slicer import Slicer, SliceRegistry
from dataplatform.etl_tools.loaders import SCD2Loader

from pyspark.sql import functions as f

spark = get_spark()
registry = SliceRegistry('load_silver_s_channel')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.channels',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.silver.s_channel (
        channel_id string,
        title string,
        description string,
        custom_url string,
        country string,
        valid_from_dttm timestamp,
        valid_to_dttm timestamp,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (days(valid_from_dttm))
''')

local_bronze_channels = slicer.run()

load_channels = local_bronze_channels.select(
    f.col('id').alias('channel_id'),
    f.col('title'),
    f.col('description'),
    f.col('customUrl').alias('custom_url'),
    f.col('country'),
    f.col('calendar_dt').cast('timestamp').alias('valid_from_dttm')
)

if not load_channels.isEmpty():
    loader = SCD2Loader(
        input=load_channels,
        output='local.silver.s_channel',
        business_key='channel_id',
        key_columns=['channel_id', 'valid_from_dttm'],
        mode='upsert',
        rebuild_history_mode='from_dt'
    )
    loader.run()
    slicer.commit()

spark.stop()
