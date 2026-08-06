from pyspark.sql import functions as f
from pyspark.sql import DataFrame, Window

from dataplatform.etl_tools.loaders.loader import Loader
from dataplatform.etl_tools.collapse import collapse

class SCD2Loader(Loader):
    _WRITE_MODE = ['insert', 'upsert', 'rewrite']
    _REBUILD_HISTORY_MODE = ['no', 'from_dt', 'full']
    _VALID_FROM_COLUMN = 'valid_from_dttm'
    _VALID_TO_COLUMN = 'valid_to_dttm'
    _TECHNICAL_COLUMNS = [_VALID_TO_COLUMN, Loader._PROCESSED_COLUMN]
    _OPERATION_COLUMN = '_operation'
    _UPSERT_OPERATION = 'upsert'
    _CLOSE_OPERATION = 'close'
    _DELETE_OPERATION = 'delete'
    _UPSERT_VIEW = 'scd2_to_upsert'

    def __init__(self, input: DataFrame, output: str, business_key: str, key_columns: list[str], mode: str, rebuild_history_mode: str = 'no'):
        super().__init__(input, output, key_columns, mode)
        self._rebuild_history_mode = rebuild_history_mode
        self.business_key = business_key
        self._source_df = None
        self._target_df = None
        self._rebuilt_df = None

    def run(self) -> str:
        self.validate_mode()
        self.validate_key_columns()
        self.validate_input_keys()
        match self._mode:
            case 'insert':
                return self.run_insert()
            case 'upsert':
                return self.run_upsert()
            case 'rewrite':
                return self.run_rewrite()

    def run_insert(self) -> str:
        self.validate_output()
        self._versioned_input().writeTo(self._output).append()
        return self._output

    def run_upsert(self) -> str:
        self.validate_output()
        versions = self.processed_dttm(self.rebuild_history())
        self.validate_columns(versions)
        closed = self.processed_dttm(self.closed_versions())
        stale = self.obsolete_versions()
        changes = versions.withColumn(self._OPERATION_COLUMN, f.lit(self._UPSERT_OPERATION)) \
            .unionByName(closed.withColumn(self._OPERATION_COLUMN, f.lit(self._CLOSE_OPERATION)), allowMissingColumns=True) \
            .unionByName(stale.withColumn(self._OPERATION_COLUMN, f.lit(self._DELETE_OPERATION)), allowMissingColumns=True)
        changes.createOrReplaceTempView(self._UPSERT_VIEW)
        columns = versions.columns
        value_columns = [col for col in columns if col not in self._key_columns]
        compare_columns = [col for col in value_columns if col != self._PROCESSED_COLUMN]
        try:
            self._session.sql(f'''
                merge into {self._output} as t
                using {self._UPSERT_VIEW} as s
                {self.generate_join_conditions('t', 's', self._key_columns)}
                when matched and s.{self._OPERATION_COLUMN} = '{self._DELETE_OPERATION}' then delete
                when matched and s.{self._OPERATION_COLUMN} = '{self._CLOSE_OPERATION}' and not (t.{self._VALID_TO_COLUMN} <=> s.{self._VALID_TO_COLUMN})
                    then update set {self._VALID_TO_COLUMN} = s.{self._VALID_TO_COLUMN}, {self._PROCESSED_COLUMN} = s.{self._PROCESSED_COLUMN}
                when matched and s.{self._OPERATION_COLUMN} = '{self._UPSERT_OPERATION}' and {self.generate_change_conditions('t', 's', compare_columns)}
                    then update set {self.generate_update_expression('s', value_columns)}
                when not matched and s.{self._OPERATION_COLUMN} = '{self._UPSERT_OPERATION}' then insert {self.generate_insert_expression('s', columns)}
                ''')
        finally:
            self._session.catalog.dropTempView(self._UPSERT_VIEW)
        return self._output

    def run_rewrite(self) -> str:
        self.validate_output()
        self._versioned_input().writeTo(self._output).overwrite(f.lit(True))
        return self._output

    def rebuild_history(self) -> DataFrame:
        return self._rebuilt_versions().join(
            self._source().select(*self._key_columns),
            how='left_semi',
            on=self._key_columns
        )

    def closed_versions(self) -> DataFrame:
        return self._rebuilt_versions().join(
            self._source().select(*self._key_columns),
            how='left_anti',
            on=self._key_columns
        )

    def obsolete_versions(self) -> DataFrame:
        return self._drop_technical_columns(self.history_slice()).select(*self._key_columns).join(
            self._rebuilt_versions().select(*self._key_columns),
            how='left_anti',
            on=self._key_columns
        )

    def history_slice(self) -> DataFrame:
        self.validate_rebuild_mode()
        match self._rebuild_history_mode:
            case 'no':
                return self.slice_no()
            case 'from_dt':
                return self.slice_from_dt()
            case 'full':
                return self.slice_full()

    def slice_no(self) -> DataFrame:
        return self._last_version(self._touched_history())

    def slice_from_dt(self) -> DataFrame:
        history = self._history_before_input()
        return history.where(f.col(self._VALID_FROM_COLUMN) >= f.col('boundary_dttm')) \
            .unionByName(self._last_version(history, 'boundary_dttm')) \
            .drop('boundary_dttm')

    def slice_full(self) -> DataFrame:
        return self._touched_history()

    def _versioned_input(self) -> DataFrame:
        return self.processed_dttm(self._build_versions(self._source()))

    def _rebuilt_versions(self) -> DataFrame:
        if self._rebuilt_df is None:
            source = self._source()
            scope = self._drop_technical_columns(self.history_slice()).select(source.columns)
            replaced = scope.join(source.select(*self._key_columns), how='left_anti', on=self._key_columns)
            self._rebuilt_df = self._build_versions(replaced.unionByName(source))
        return self._rebuilt_df

    def _build_versions(self, dataframe: DataFrame) -> DataFrame:
        self.validate_column(self._VALID_FROM_COLUMN, dataframe)
        compare_columns = [col for col in dataframe.columns
                           if col not in [self.business_key, self._VALID_FROM_COLUMN, self._PROCESSED_COLUMN]]
        w = Window.partitionBy(self.business_key).orderBy(f.col(self._VALID_FROM_COLUMN).asc())
        return collapse(dataframe, [self.business_key], compare_columns) \
            .withColumn(self._VALID_TO_COLUMN, f.lead(self._VALID_FROM_COLUMN).over(w))

    def _history_before_input(self) -> DataFrame:
        boundary = self._source().groupBy(self.business_key).agg(f.min(self._VALID_FROM_COLUMN).alias('boundary_dttm'))
        return self._target().join(boundary, how='inner', on=self.business_key)

    def _last_version(self, dataframe: DataFrame, boundary_column: str | None = None) -> DataFrame:
        if boundary_column:
            dataframe = dataframe.where(f.col(self._VALID_FROM_COLUMN) < f.col(boundary_column))
        w = Window.partitionBy(self.business_key).orderBy(f.col(self._VALID_FROM_COLUMN).desc())
        return dataframe.withColumn('rn', f.row_number().over(w)) \
            .where(f.col('rn') == 1) \
            .drop('rn')

    def _touched_history(self) -> DataFrame:
        return self._target().join(
            self._source().select(self.business_key).distinct(),
            how='inner',
            on=self.business_key
        )

    def _source(self) -> DataFrame:
        if self._source_df is None:
            self._source_df = self._drop_technical_columns(self._input)
        return self._source_df

    def _target(self) -> DataFrame:
        if self._target_df is None:
            self._target_df = self._session.table(self._output)
        return self._target_df

    def _drop_technical_columns(self, dataframe: DataFrame) -> DataFrame:
        return dataframe.select([col for col in dataframe.columns if col not in self._TECHNICAL_COLUMNS])

    @staticmethod
    def generate_update_expression(right_prefix: str, columns: list[str]) -> str:
        if not columns:
            raise ValueError('Field columns have not to be empty')
        return ', '.join(f'{col} = {right_prefix}.{col}' for col in columns)

    @staticmethod
    def generate_insert_expression(right_prefix: str, columns: list[str]) -> str:
        if not columns:
            raise ValueError('Field columns have not to be empty')
        values = ', '.join(f'{right_prefix}.{col}' for col in columns)
        return f"({', '.join(columns)}) values ({values})"

    def validate_mode(self) -> None:
        if self._mode not in self._WRITE_MODE:
            raise ValueError(f'Unknown write mode, expected one of: {self._WRITE_MODE}')

    def validate_rebuild_mode(self) -> None:
        if self._rebuild_history_mode not in self._REBUILD_HISTORY_MODE:
            raise ValueError(f'Unknown rebuild history mode, expected one of: {self._REBUILD_HISTORY_MODE}')

    def validate_key_columns(self) -> None:
        if self.business_key not in self._key_columns:
            raise ValueError(f'business key {self.business_key} is not include in key columns {self._key_columns}')
        if self._VALID_FROM_COLUMN not in self._key_columns:
            raise ValueError(f'key columns {self._key_columns} must have {self._VALID_FROM_COLUMN} column')

    def validate_input_keys(self) -> None:
        empty_key = f.lit(False)
        for col in self._key_columns:
            empty_key = empty_key | f.col(col).isNull()
        broken = self._source().groupBy(*self._key_columns).count() \
            .where((f.col('count') > 1) | empty_key) \
            .limit(1) \
            .count()
        if broken:
            raise ValueError(f'input must have unique not null key columns {self._key_columns}')

    def validate_columns(self, dataframe: DataFrame) -> None:
        expected = self._target().columns
        missing = [col for col in expected if col not in dataframe.columns]
        unexpected = [col for col in dataframe.columns if col not in expected]
        if missing or unexpected:
            raise ValueError(f'input columns do not match table {self._output}: missing {missing}, extra {unexpected}')

    @staticmethod
    def validate_column(column: str, dataframe: DataFrame) -> None:
        if column not in dataframe.columns:
            raise ValueError(f'column {column} is not include in dataframe {dataframe.columns}')
