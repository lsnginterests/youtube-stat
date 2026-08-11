from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as f

CATALOG = 'local'


@dataclass(frozen=True)
class Table:
    name: str
    key_columns: tuple[str, ...]
    measure_columns: tuple[str, ...] = ()
    business_key: str = ''
    references: tuple[tuple[str, str], ...] = ()

    @property
    def full_name(self) -> str:
        return f'{CATALOG}.{self.name}'


@dataclass(frozen=True)
class Result:
    check: str
    violations: int
    detail: str = ''

    @property
    def passed(self) -> bool:
        return self.violations == 0


TABLES: tuple[Table, ...] = (
    Table(
        name='bronze.categories',
        key_columns=('id', 'calendar_dt')
    ),
    Table(
        name='bronze.channels',
        key_columns=('id', 'calendar_dt'),
        measure_columns=('subscriberCount',)
    ),
    Table(
        name='bronze.videos',
        key_columns=('id', 'calendar_dt'),
        measure_columns=('viewcount', 'likecount', 'favoriteCount', 'commentCount')
    ),
    Table(
        name='silver.h_channel',
        key_columns=('channel_id',)
    ),
    Table(
        name='silver.h_video',
        key_columns=('video_id',)
    ),
    Table(
        name='silver.l_video_channel',
        key_columns=('link_id',),
        references=(('video_id', 'silver.h_video'), ('channel_id', 'silver.h_channel'))
    ),
    Table(
        name='silver.ref_category',
        key_columns=('category_id',)
    ),
    Table(
        name='silver.s_channel',
        key_columns=('channel_id', 'valid_from_dttm'),
        business_key='channel_id',
        references=(('channel_id', 'silver.h_channel'),)
    ),
    Table(
        name='silver.s_channel_stats',
        key_columns=('channel_id', 'calendar_dt'),
        measure_columns=('subscribers_cnt',),
        references=(('channel_id', 'silver.h_channel'),)
    ),
    Table(
        name='silver.s_video',
        key_columns=('video_id', 'valid_from_dttm'),
        business_key='video_id',
        references=(('video_id', 'silver.h_video'),)
    ),
    Table(
        name='silver.s_video_stats',
        key_columns=('video_id', 'calendar_dt'),
        measure_columns=('views_cnt', 'likes_cnt', 'favorites_cnt', 'comments_cnt'),
        references=(('video_id', 'silver.h_video'),)
    ),
    Table(
        name='gold.dim_calendar',
        key_columns=('calendar_dt',)
    ),
    Table(
        name='gold.dict_category',
        key_columns=('category_id',)
    ),
    Table(
        name='gold.dict_video_channel',
        key_columns=('video_id',),
        references=(('video_id', 'gold.dim_video'), ('channel_id', 'gold.dim_channel'))
    ),
    Table(
        name='gold.dim_channel',
        key_columns=('channel_id', 'valid_from_dttm'),
        business_key='channel_id'
    ),
    Table(
        name='gold.dim_video',
        key_columns=('video_id', 'valid_from_dttm'),
        business_key='video_id'
    ),
    Table(
        name='gold.fct_video_daily',
        key_columns=('video_id', 'calendar_dt'),
        measure_columns=('views_cnt', 'likes_cnt', 'favorites_cnt', 'comments_cnt'),
        references=(
            ('video_id', 'gold.dim_video'),
            ('channel_id', 'gold.dim_channel'),
            ('category_id', 'gold.dict_category'),
            ('calendar_dt', 'gold.dim_calendar')
        )
    ),
    Table(
        name='gold.fct_channel_daily',
        key_columns=('channel_id', 'calendar_dt'),
        measure_columns=('subscribers_cnt', 'videos_cnt', 'views_total', 'likes_total', 'comments_total'),
        references=(('channel_id', 'gold.dim_channel'), ('calendar_dt', 'gold.dim_calendar'))
    )
)

BY_NAME: dict[str, Table] = {table.name: table for table in TABLES}


def count_violations(dataframe: DataFrame, columns: tuple[str, ...],
                     condition: Callable[[str], Column]) -> dict[str, int]:
    counters = [f.count(f.when(condition(column), True)).alias(column) for column in columns]
    counted = dataframe.agg(*counters).first()
    return {column: counted[column] for column in columns if counted[column]}


def describe(counted: dict[str, int]) -> str:
    return ', '.join(f'{column}: {count}' for column, count in counted.items())


def not_empty(dataframe: DataFrame) -> Result:
    return Result('not_empty', int(dataframe.isEmpty()), 'table has no rows')


def no_duplicates(dataframe: DataFrame, key_columns: tuple[str, ...]) -> Result:
    duplicated = dataframe.groupBy(*key_columns).count().where(f.col('count') > 1)
    return Result('no_duplicates', duplicated.count(), f'duplicated keys ({", ".join(key_columns)})')


def no_nulls_in_key(dataframe: DataFrame, key_columns: tuple[str, ...]) -> Result:
    counted = count_violations(dataframe, key_columns, lambda column: f.col(column).isNull())
    return Result('no_nulls_in_key', sum(counted.values()), f'null keys ({describe(counted)})')


def no_negative(dataframe: DataFrame, measure_columns: tuple[str, ...]) -> Result:
    counted = count_violations(dataframe, measure_columns, lambda column: f.col(column) < 0)
    return Result('no_negative', sum(counted.values()), f'negative measures ({describe(counted)})')


def single_open_version(dataframe: DataFrame, business_key: str) -> Result:
    open_versions = dataframe.groupBy(business_key) \
        .agg(f.count(f.when(f.col('valid_to_dttm').isNull(), True)).alias('open_cnt')) \
        .where(f.col('open_cnt') != 1)
    return Result('single_open_version', open_versions.count(),
                  f'{business_key} values without exactly one open version')


def no_orphans(session: SparkSession, dataframe: DataFrame, column: str, parent: str) -> Result:
    parent_keys = session.table(f'{CATALOG}.{parent}').select(column).distinct()
    orphans = dataframe.select(column) \
        .where(f.col(column).isNotNull()) \
        .join(other=parent_keys, on=column, how='left_anti')
    return Result(f'no_orphans[{column}]', orphans.count(),
                  f'rows with {column} missing in {CATALOG}.{parent}')


def check(session: SparkSession, table: Table) -> list[Result]:
    if not session.catalog.tableExists(table.full_name):
        return [Result('table_exists', 1, f'{table.full_name} does not exist')]

    dataframe = session.table(table.full_name).cache()
    try:
        results = [
            not_empty(dataframe),
            no_duplicates(dataframe, table.key_columns),
            no_nulls_in_key(dataframe, table.key_columns)
        ]
        if table.measure_columns:
            results.append(no_negative(dataframe, table.measure_columns))
        if table.business_key:
            results.append(single_open_version(dataframe, table.business_key))
        results.extend(
            no_orphans(session, dataframe, column, parent) for column, parent in table.references
        )
        return results
    finally:
        dataframe.unpersist()


def validate_names(names: list[str]) -> list[Table]:
    unknown = set(names) - set(BY_NAME)
    if unknown:
        raise ValueError(f'Unknown tables: {sorted(unknown)}')
    return [BY_NAME[name] for name in names] if names else list(TABLES)
