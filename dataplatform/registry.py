from collections.abc import Callable, Iterable
from dataclasses import dataclass
from graphlib import TopologicalSorter

from dataplatform.pipelines.bronze import (
    load_bronze_categories,
    load_bronze_channels,
    load_bronze_video
)
from dataplatform.pipelines.silver import (
    load_silver_h_video,
    load_silver_l_video_channel,
    load_silver_s_video,
    load_silver_s_channel_stats,
    load_silver_ref_category,
    load_silver_s_video_stats,
    load_silver_h_channel,
    load_silver_s_channel
)
from dataplatform.pipelines.gold import (
    load_gold_dict_category,
    load_gold_dim_calendar,
    load_gold_dim_channel,
    load_gold_fct_channel_daily,
    load_gold_dict_video_channel,
    load_gold_fct_video_daily,
    load_gold_dim_video
)


@dataclass(frozen=True)
class Pipeline:
    name: str
    run: Callable[..., None]
    outputs: tuple[str, ...]
    inputs: tuple[str, ...] = ()
    takes_date: bool = False


PIPELINES: tuple[Pipeline, ...] = (
    Pipeline(
        name='bronze.categories',
        run=load_bronze_categories.run,
        outputs=('local.bronze.categories',),
        takes_date=True
    ),
    Pipeline(
        name='bronze.channels',
        run=load_bronze_channels.run,
        outputs=('local.bronze.channels',),
        takes_date=True
    ),
    Pipeline(
        name='bronze.videos',
        run=load_bronze_video.run,
        outputs=('local.bronze.videos',),
        takes_date=True
    ),
    Pipeline(
        name='silver.h_channel',
        run=load_silver_h_channel.run,
        inputs=('local.bronze.channels', 'local.silver.h_channel'),
        outputs=('local.silver.h_channel',)
    ),
    Pipeline(
        name='silver.h_video',
        run=load_silver_h_video.run,
        inputs=('local.bronze.videos', 'local.silver.h_video'),
        outputs=('local.silver.h_video',)
    ),
    Pipeline(
        name='silver.l_video_channel',
        run=load_silver_l_video_channel.run,
        inputs=('local.bronze.videos', 'local.silver.l_video_channel'),
        outputs=('local.silver.l_video_channel',)
    ),
    Pipeline(
        name='silver.ref_category',
        run=load_silver_ref_category.run,
        inputs=('local.bronze.categories',),
        outputs=('local.silver.ref_category',)
    ),
    Pipeline(
        name='silver.s_channel',
        run=load_silver_s_channel.run,
        inputs=('local.bronze.channels',),
        outputs=('local.silver.s_channel',)
    ),
    Pipeline(
        name='silver.s_channel_stats',
        run=load_silver_s_channel_stats.run,
        inputs=('local.bronze.channels',),
        outputs=('local.silver.s_channel_stats',)
    ),
    Pipeline(
        name='silver.s_video',
        run=load_silver_s_video.run,
        inputs=(
            'local.bronze.videos',
            'local.silver.h_video',
            'local.silver.l_video_channel',
            'local.silver.s_video'
        ),
        outputs=('local.silver.s_video',)
    ),
    Pipeline(
        name='silver.s_video_stats',
        run=load_silver_s_video_stats.run,
        inputs=('local.bronze.videos',),
        outputs=('local.silver.s_video_stats',)
    ),
    Pipeline(
        name='gold.dim_calendar',
        run=load_gold_dim_calendar.run,
        outputs=('local.gold.dim_calendar',)
    ),
    Pipeline(
        name='gold.dict_category',
        run=load_gold_dict_category.run,
        inputs=('local.silver.ref_category',),
        outputs=('local.gold.dict_category',)
    ),
    Pipeline(
        name='gold.dict_video_channel',
        run=load_gold_dict_video_channel.run,
        inputs=('local.silver.l_video_channel',),
        outputs=('local.gold.dict_video_channel',)
    ),
    Pipeline(
        name='gold.dim_channel',
        run=load_gold_dim_channel.run,
        inputs=('local.silver.h_channel', 'local.silver.s_channel'),
        outputs=('local.gold.dim_channel',)
    ),
    Pipeline(
        name='gold.dim_video',
        run=load_gold_dim_video.run,
        inputs=('local.silver.h_video', 'local.silver.s_video'),
        outputs=('local.gold.dim_video',)
    ),
    Pipeline(
        name='gold.fct_video_daily',
        run=load_gold_fct_video_daily.run,
        inputs=(
            'local.gold.fct_video_daily',
            'local.silver.l_video_channel',
            'local.silver.s_video',
            'local.silver.s_video_stats'
        ),
        outputs=('local.gold.fct_video_daily',)
    ),
    Pipeline(
        name='gold.fct_channel_daily',
        run=load_gold_fct_channel_daily.run,
        inputs=('local.gold.fct_video_daily', 'local.silver.s_channel_stats'),
        outputs=('local.gold.fct_channel_daily',)
    )
)

BY_NAME: dict[str, Pipeline] = {pipeline.name: pipeline for pipeline in PIPELINES}
PRODUCER: dict[str, str] = {table: pipeline.name for pipeline in PIPELINES for table in pipeline.outputs}
LAYERS: tuple[str, ...] = ('bronze', 'silver', 'gold')


def validate_names(names: Iterable[str]) -> set[str]:
    selected = set(names)
    unknown = selected - set(BY_NAME)
    if unknown:
        raise ValueError(f'Unknown pipelines: {sorted(unknown)}')
    return selected


def graph(names: Iterable[str] | None = None) -> dict[str, set[str]]:
    selected = set(BY_NAME) if names is None else validate_names(names)
    dependencies = {}
    for pipeline in PIPELINES:
        if pipeline.name not in selected:
            continue
        producers = set()
        for table in pipeline.inputs:
            producer = PRODUCER.get(table)
            if producer is None or producer == pipeline.name or producer not in selected:
                continue
            producers.add(producer)
        dependencies[pipeline.name] = producers
    return dependencies


def order(names: Iterable[str] | None = None) -> list[str]:
    sorter = TopologicalSorter(graph(names))
    sorter.prepare()
    ordered = []
    while sorter.is_active():
        ready = sorted(sorter.get_ready())
        ordered.extend(ready)
        sorter.done(*ready)
    return ordered


def with_deps(names: Iterable[str]) -> list[str]:
    full_graph = graph()
    queue = list(validate_names(names))
    selected = set()
    while queue:
        name = queue.pop()
        if name in selected:
            continue
        selected.add(name)
        queue.extend(full_graph[name])
    return order(selected)


def layer(name: str) -> list[str]:
    if name not in LAYERS:
        raise ValueError(f'Unknown layer {name}, expected one of: {list(LAYERS)}')
    return order([pipeline.name for pipeline in PIPELINES if pipeline.name.startswith(f'{name}.')])
