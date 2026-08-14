from pyspark.sql import SparkSession

from config import get_settings

ICEBERG_VERSION = '1.7.1'
ICEBERG_PACKAGE = f'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}'
HADOOP_AWS_PACKAGE = 'org.apache.hadoop:hadoop-aws:3.3.4'
AWS_SDK_PACKAGE = 'com.amazonaws:aws-java-sdk-bundle:1.12.262'
POSTGRES_PACKAGE = 'org.postgresql:postgresql:42.7.4'
PACKAGES = ','.join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE, AWS_SDK_PACKAGE, POSTGRES_PACKAGE])

CATALOG = 'local'


def get_spark() -> SparkSession:
    settings = get_settings()
    return (
        SparkSession.builder
        .appName('appLake')
        .master('local[2]')
        .config('spark.jars.packages', PACKAGES)
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{CATALOG}.type', 'jdbc')
        .config(f'spark.sql.catalog.{CATALOG}.uri', settings.catalog_uri)
        .config(f'spark.sql.catalog.{CATALOG}.jdbc.user', settings.catalog_user)
        .config(f'spark.sql.catalog.{CATALOG}.jdbc.password', settings.catalog_password)
        .config(f'spark.sql.catalog.{CATALOG}.jdbc.schema-version', 'V1')
        .config(f'spark.sql.catalog.{CATALOG}.warehouse', f's3a://{settings.s3_bucket_warehouse}')
        .config('spark.hadoop.fs.s3a.endpoint', settings.s3_endpoint_url)
        .config('spark.hadoop.fs.s3a.access.key', settings.s3_access_key)
        .config('spark.hadoop.fs.s3a.secret.key', settings.s3_secret_key)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
