from pyspark.sql.dataframe import DataFrame
from dataplatform.setting.spark_session import get_spark
from pyspark.sql import functions as f

class Loader:
    _PROCESSED_COLUMN = 'processed_dttm'

    def __init__(self, input: DataFrame, output: str, key_columns: list[str], mode: str):
        self._input = input
        self._output = output
        self._key_columns = key_columns
        self._mode = mode
        self._session = get_spark()

    def validate_output(self):
        if not self._session.catalog.tableExists(self._output):
            raise ValueError(f'Output table {self._output} does not exist')

    @staticmethod
    def processed_dttm(dataframe: DataFrame):
        return dataframe.withColumn(Loader._PROCESSED_COLUMN, f.current_timestamp())

    @staticmethod
    def generate_join_conditions(left_prefix, right_prefix, columns: list[str]):
        if not columns:
            raise ValueError('Field columns have not to be empty')
        string = f'on {left_prefix}.{columns[0]} = {right_prefix}.{columns[0]}'
        if len(columns) > 1:
            string += '\n'
            for col in columns[1:]:
                string += f'and {left_prefix}.{col} = {right_prefix}.{col}\n'
        return string

    @staticmethod
    def generate_change_conditions(left_prefix: str, right_prefix: str, columns: list[str]) -> str:
        if not columns:
            raise ValueError('Field columns have not to be empty')
        comparisons = ' and '.join(f'{left_prefix}.{col} <=> {right_prefix}.{col}' for col in columns)
        return f'not ({comparisons})'