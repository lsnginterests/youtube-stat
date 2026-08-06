from pyspark.sql import functions as f
from pyspark.sql import DataFrame

from dataplatform.etl_tools.loaders.loader import Loader

class SCD1Loader(Loader):
    _WRITE_MODES = ['insert', 'upsert', 'rewrite']

    def run(self):
        self.validate_mode()
        self.validate_output()
        match self._mode:
            case 'insert':
                self.run_insert()
            case 'upsert':
                self.run_upsert()
            case 'rewrite':
                self.run_rewrite()

    def run_insert(self) -> DataFrame:
        self._input = SCD1Loader.processed_dttm(self._input)
        self._input.writeTo(self._output).append()
        return self._output

    def run_upsert(self) -> DataFrame:
        self._input = SCD1Loader.processed_dttm(self._input)
        self._input.createOrReplaceTempView('to_upsert')
        compare_columns = [col for col in self._input.columns if col not in self._key_columns and col != self._PROCESSED_COLUMN]
        update_clause = f"when matched and {self.generate_change_conditions('t', 's', compare_columns)} then update set *" if compare_columns else ''
        self._session.sql(f'''
            merge into {self._output} as t
            using to_upsert as s
            {self.generate_join_conditions('t', 's', self._key_columns)}
            {update_clause}
            when not matched then insert *
            ''')
        return self._output

    def run_rewrite(self) -> DataFrame:
        self._input = self.processed_dttm(self._input)
        self._input.writeTo(self._output).overwrite(f.lit(True))
        return self._output

    def validate_mode(self):
        if self._mode not in self._WRITE_MODES:
            raise ValueError(f'Unknown write mode, expected one of: {self._WRITE_MODES}')