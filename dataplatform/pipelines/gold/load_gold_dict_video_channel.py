from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_gold_dict_video_channel')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.silver.l_video_channel',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.gold.dict_video_channel (
            video_id string,
            channel_id string,
            processed_dttm timestamp
        ) using iceberg
    ''')

    local_silver_l_video_channel = slicer.run()

    to_load = local_silver_l_video_channel.select(
        'video_id',
        'channel_id'
    )

    if not to_load.isEmpty():
        loader = SCD1Loader(to_load, 'local.gold.dict_video_channel', ['video_id'], 'upsert')
        loader.run()
        slicer.commit()
