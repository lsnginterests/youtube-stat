from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry

spark = get_spark()
registry = SliceRegistry('load_silver_h_channel')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.channels',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.silver.h_channel (
        channel_id string,
        created_at timestamp,
        processed_dttm timestamp
    ) using iceberg
    partitioned by (years(created_at))
    ''')

local_bronze_channels = slicer.run()
load_channels = local_bronze_channels.select(
    f.col('id').alias('channel_id'), 
    f.col('publishedAt').alias('created_at')
    )

merged = load_channels.join(spark.table('local.silver.h_channel'), 'channel_id', how='leftanti').select('channel_id', 'created_at')

to_load = merged.groupBy(f.col('channel_id')) \
    .agg(f.min('created_at').alias('created_at'))

loader = SCD1Loader(to_load, 'local.silver.h_channel', ['channel_id'], 'upsert')
loader.run()
slicer.commit()

spark.sql('select * from local.silver.h_channel').show()
spark.stop()