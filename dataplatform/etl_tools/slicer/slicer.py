from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from datetime import datetime

from dataplatform.etl_tools.slicer.slice_registry import SliceRegistry

class Slicer:
    def __init__(self, session: SparkSession, registry: SliceRegistry, table: str, slice_column: str):
        self._session = session
        self._table = table
        self._slice_column = slice_column
        self._registry = registry
        self._new_watermark = None
        self._snapshot_id = None

    def run(self) -> DataFrame:
        self.validate()
        self.current_snapshot()
        dttm = self._registry.read(self._table)
        df = self.to_dataframe()
        if dttm:
            df = df.where(f.col(self._slice_column) > f.lit(self.validate_watermark(dttm)))
        self._new_watermark = df.select(f.max(f.col(self._slice_column))).first()[0]
        return df

    def commit(self):
        if self._new_watermark:
            self._registry.advance(self._table, str(self._new_watermark))
            self._new_watermark = None

    def validate(self):
        self.validate_table()
        self.validate_column()

    def validate_column(self):
        columns = self._session.catalog.listColumns(self._table)
        if not any(col.name == self._slice_column for col in columns):
            raise ValueError(f'datetime column {self._slice_column} does not exist in table {self._table}')

    def validate_table(self):
        if not self._session.catalog.tableExists(self._table):
            raise ValueError(f'table {self._table} does not exist')

    def validate_watermark(self, dttm: str) -> datetime:
        try:
            return datetime.fromisoformat(dttm)
        except (TypeError, ValueError) as e:
            raise ValueError(f'invalid watermark {dttm!r} for table {self._table} in slice registry') from e

    def to_dataframe(self):
        reader = self._session.read
        if self._snapshot_id:
            reader = reader.option('snapshot-id', self._snapshot_id)
        return reader.table(self._table)

    def current_snapshot(self):
        row = self._session.sql(f'select snapshot_id from {self._table}.refs where name = "main"').first()
        self._snapshot_id = row[0] if row else None
        return self._snapshot_id