from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader
from dataplatform.etl_tools import Slicer, SliceRegistry

spark = get_spark()
registry = SliceRegistry('load_silver_ref_category')
slicer = Slicer(
    session=spark,
    registry=registry,
    table='local.bronze.categories',
    slice_column='processed_dttm'
)

spark.sql('''
    create table if not exists local.silver.ref_category (
        category_id string,
        category_name string,
        is_assignable boolean,
        processed_dttm timestamp
    ) using iceberg
''')

local_bronze_categories = slicer.run()

to_load = local_bronze_categories.select(
    f.col('id').alias('category_id'),
    f.col('title').alias('category_name'),
    f.col('assignable').alias('is_assignable')
)

if not to_load.isEmpty():
    loader = SCD1Loader(to_load, 'local.silver.ref_category', ['category_id'], 'upsert')
    loader.run()
    slicer.commit()

spark.stop()
