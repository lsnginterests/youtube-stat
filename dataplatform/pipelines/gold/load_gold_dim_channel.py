from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import Slicer, SliceRegistry
from dataplatform.etl_tools import SCD2Loader


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_gold_dim_channel')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.silver.s_channel',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.gold.dim_channel (
        channel_id string,
        created_at timestamp,
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

    local_silver_h_channel = spark.table('local.silver.h_channel')
    local_silver_s_channel = slicer.run()

    to_load = local_silver_h_channel.join(
        other=local_silver_s_channel,
        on='channel_id',
        how='inner'
    ).select(
        'channel_id',
        'created_at',
        'title',
        'description',
        'custom_url',
        'country',
        'valid_from_dttm'
    )

    if not to_load.isEmpty():
        loader = SCD2Loader(
            input=to_load,
            output='local.gold.dim_channel',
            business_key='channel_id',
            key_columns=['channel_id', 'valid_from_dttm'],
            mode='upsert',
            rebuild_history_mode='from_dt'
        )
        loader.run()
    slicer.commit()
