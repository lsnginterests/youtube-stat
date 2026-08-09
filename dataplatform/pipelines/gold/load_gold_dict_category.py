from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_gold_dict_category')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.silver.ref_category',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.gold.dict_category (
            category_id string,
            category_name string,
            processed_dttm timestamp
        ) using iceberg
    ''')

    local_silver_ref_category = slicer.run()

    to_load = local_silver_ref_category.select(
        'category_id',
        'category_name'
    )

    if not to_load.isEmpty():
        loader = SCD1Loader(to_load, 'local.gold.dict_category', ['category_id'], 'upsert')
        loader.run()
    slicer.commit()
