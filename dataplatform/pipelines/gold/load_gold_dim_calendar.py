from pyspark.sql import functions as f

from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import SCD1Loader


def run() -> None:
    spark = get_spark()
    spark.sql('''
        create table if not exists local.gold.dim_calendar (
            calendar_dt date,
            monthday int,
            month int,
            month_name string,
            quarter int,
            year int,
            year_month string,
            week_of_year int,
            iso_year int,
            weekday int,
            day_name string,
            is_weekend boolean,
            processed_dttm timestamp
        ) using iceberg
    ''')

    calendar = spark.sql("select explode(sequence(date '2015-01-01', date '2035-01-01', interval 1 day)) as calendar_dt")
    to_load = calendar.select(
        f.col('calendar_dt'),
        (f.extract(f.lit('day'), 'calendar_dt').cast('int').alias('monthday')),
        f.month('calendar_dt').alias('month'),
        f.date_format('calendar_dt', 'MMMM').alias('month_name'),
        f.quarter('calendar_dt').alias('quarter'),
        f.year('calendar_dt').alias('year'),
        f.date_format('calendar_dt', 'yyyy-MM').alias('year_month'),
        f.weekofyear('calendar_dt').alias('week_of_year'),
        f.extract(f.lit('yearofweek'), 'calendar_dt').alias('iso_year'),
        (f.weekday('calendar_dt') + 1).alias('weekday'),
        f.date_format('calendar_dt', 'EEEE').alias('day_name')
    ).withColumn('is_weekend', f.col('weekday') > 5)

    if not to_load.isEmpty():
        loader = SCD1Loader(
            input=to_load,
            output='local.gold.dim_calendar',
            key_columns=['calendar_dt'],
            mode='upsert'
        )
        loader.run()