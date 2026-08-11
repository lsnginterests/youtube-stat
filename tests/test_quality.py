import inspect
import re
from pathlib import Path

import pytest

from dataplatform import quality
from dataplatform.quality import TABLES, Table
from dataplatform.registry import BY_NAME as PIPELINE_BY_NAME
from dataplatform.registry import PIPELINES, PRODUCER

DDL_PATTERN = re.compile(r'create table if not exists\s+(\S+)\s*\((.*?)\)\s*using iceberg', re.DOTALL)
COLUMN_PATTERN = re.compile(r'^\s*(\w+)\s+(?:string|bigint|integer|int|date|timestamp|boolean)\s*,?\s*$',
                            re.MULTILINE)


def columns_in_ddl(name: str) -> set[str]:
    pipeline = PIPELINE_BY_NAME[PRODUCER[f'local.{name}']]
    source = Path(inspect.getfile(pipeline.run)).read_text(encoding='utf-8')
    for table, body in DDL_PATTERN.findall(source):
        if table == f'local.{name}':
            return set(COLUMN_PATTERN.findall(body))
    raise AssertionError(f'No create table statement for local.{name}')


def test_every_pipeline_output_is_checked():
    produced = {table.removeprefix('local.') for pipeline in PIPELINES for table in pipeline.outputs}
    assert produced == set(quality.BY_NAME)


def test_names_are_unique():
    names = [table.name for table in TABLES]
    assert len(names) == len(set(names))


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_key_columns_are_declared(table: Table):
    assert table.key_columns, f'{table.name} has no key columns'


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_key_and_measure_columns_do_not_overlap(table: Table):
    assert not set(table.key_columns) & set(table.measure_columns)


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_business_key_is_part_of_the_key(table: Table):
    if table.business_key:
        assert table.business_key in table.key_columns


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_versioned_tables_declare_a_business_key(table: Table):
    assert bool(table.business_key) == ('valid_from_dttm' in table.key_columns)


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_references_point_to_declared_tables(table: Table):
    for column, parent in table.references:
        assert parent in quality.BY_NAME, f'{table.name} references unknown table {parent}'
        assert parent != table.name, f'{table.name} references itself'


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_declared_columns_exist_in_table(table: Table):
    columns = columns_in_ddl(table.name)
    declared = set(table.key_columns) | set(table.measure_columns) | {column for column, _ in table.references}
    missing = declared - columns
    assert not missing, f'{table.name} declares columns absent from its DDL: {sorted(missing)}'


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_referenced_columns_exist_in_parent(table: Table):
    for column, parent in table.references:
        assert column in columns_in_ddl(parent), f'{parent} has no column {column} referenced by {table.name}'


@pytest.mark.parametrize('table', TABLES, ids=lambda table: table.name)
def test_versioned_tables_carry_the_open_version_column(table: Table):
    if table.business_key:
        assert 'valid_to_dttm' in columns_in_ddl(table.name)


def test_importing_quality_does_not_start_spark():
    from pyspark.sql import SparkSession

    assert SparkSession.getActiveSession() is None
