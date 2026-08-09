import ast
import inspect
import re
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import pytest

from dataplatform import registry
from dataplatform.registry import PIPELINES, PRODUCER, Pipeline


TABLE_PATTERN = re.compile(r'local\.\w+\.\w+')


def tables_in_code(pipeline: Pipeline) -> set[str]:
    source = Path(inspect.getfile(pipeline.run)).read_text(encoding='utf-8')
    tables = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            tables.update(TABLE_PATTERN.findall(node.value))
    return tables


def test_graph_is_acyclic():
    sorter = TopologicalSorter(registry.graph())
    try:
        sorter.prepare()
    except CycleError as error:
        pytest.fail(f'Dependency graph has a cycle: {error.args[1]}')


def test_every_table_has_single_producer():
    produced = [table for pipeline in PIPELINES for table in pipeline.outputs]
    duplicates = {table for table in produced if produced.count(table) > 1}
    assert not duplicates, f'Tables written by more than one pipeline: {sorted(duplicates)}'


def test_every_input_is_produced():
    dangling = {
        (pipeline.name, table)
        for pipeline in PIPELINES
        for table in pipeline.inputs
        if table not in PRODUCER
    }
    assert not dangling, f'Inputs nobody produces: {sorted(dangling)}'


def test_names_are_unique():
    names = [pipeline.name for pipeline in PIPELINES]
    assert len(names) == len(set(names))


@pytest.mark.parametrize('pipeline', PIPELINES, ids=lambda pipeline: pipeline.name)
def test_name_matches_output_table(pipeline: Pipeline):
    assert pipeline.name == pipeline.outputs[0].removeprefix('local.')


@pytest.mark.parametrize('pipeline', PIPELINES, ids=lambda pipeline: pipeline.name)
def test_declaration_matches_code(pipeline: Pipeline):
    declared = set(pipeline.inputs) | set(pipeline.outputs)
    assert tables_in_code(pipeline) == declared


@pytest.mark.parametrize('pipeline', PIPELINES, ids=lambda pipeline: pipeline.name)
def test_takes_date_matches_signature(pipeline: Pipeline):
    parameters = inspect.signature(pipeline.run).parameters
    assert bool(parameters) == pipeline.takes_date


def test_order_covers_every_pipeline():
    assert sorted(registry.order()) == sorted(registry.BY_NAME)


def test_order_respects_dependencies():
    ordered = registry.order()
    dependencies = registry.graph()
    for position, name in enumerate(ordered):
        for producer in dependencies[name]:
            assert ordered.index(producer) < position, f'{producer} must run before {name}'


def test_order_is_deterministic():
    assert registry.order() == registry.order()


def test_layer_contains_only_its_own_pipelines():
    for name in registry.LAYERS:
        assert all(pipeline.startswith(f'{name}.') for pipeline in registry.layer(name))


def test_layer_order_respects_dependencies():
    ordered = registry.layer('silver')
    assert ordered.index('silver.h_video') < ordered.index('silver.s_video')
    assert ordered.index('silver.l_video_channel') < ordered.index('silver.s_video')


def test_with_deps_pulls_upstream():
    ordered = registry.with_deps(['gold.fct_channel_daily'])
    assert ordered[-1] == 'gold.fct_channel_daily'
    assert 'bronze.videos' in ordered
    assert ordered.index('gold.fct_video_daily') < ordered.index('gold.fct_channel_daily')
    assert 'gold.dim_calendar' not in ordered


def test_with_deps_of_source_pipeline_is_itself():
    assert registry.with_deps(['bronze.videos']) == ['bronze.videos']


def test_unknown_pipeline_is_rejected():
    with pytest.raises(ValueError, match='Unknown pipelines'):
        registry.order(['silver.s_vidoe'])


def test_unknown_layer_is_rejected():
    with pytest.raises(ValueError, match='Unknown layer'):
        registry.layer('purple')


def test_self_reference_is_not_a_dependency():
    assert 'silver.s_video' not in registry.graph()['silver.s_video']
    assert 'gold.fct_video_daily' not in registry.graph()['gold.fct_video_daily']


def test_importing_registry_does_not_start_spark():
    from pyspark.sql import SparkSession

    assert SparkSession.getActiveSession() is None
