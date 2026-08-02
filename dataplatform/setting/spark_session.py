from pathlib import Path
from pyspark.sql import SparkSession

ICEBERG_VERSION = '1.7.1'
ICEBERG_PACKAGE = f'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}'

CATALOG = 'local'
# Iceberg-warehouse живёт над lakehouse (silver/gold), рядом с bronze — не внутри setting/
WAREHOUSE = Path(__file__).resolve().parent.parent.joinpath('lakehouse')


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName('appLake')
        .master('local[2]')
        .config('spark.jars.packages', ICEBERG_PACKAGE)
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{CATALOG}.type', 'hadoop')
        .config(f'spark.sql.catalog.{CATALOG}.warehouse', WAREHOUSE.as_uri())
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
