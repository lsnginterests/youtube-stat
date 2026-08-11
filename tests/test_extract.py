import ast
import inspect
from datetime import date
from pathlib import Path

import pytest

from extract_load import run
from extract_load.load.s3_loader import S3RawSink

SINK_FILE = Path(inspect.getfile(S3RawSink))
CLOCK_CALLS = {('datetime', 'now'), ('date', 'today'), ('time', 'time')}
WRITE_METHODS = ('write_raw', 'write_reference', 'write_channel', 'write_video', 'write_category')


def clock_calls_in(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [
        f'{node.func.value.id}.{node.func.attr}'
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and (node.func.value.id, node.func.attr) in CLOCK_CALLS
    ]


def test_sink_does_not_read_the_clock():
    found = clock_calls_in(SINK_FILE)
    assert not found, f'S3RawSink decides the ingestion date itself instead of taking it: {found}'


@pytest.mark.parametrize('method', WRITE_METHODS)
def test_write_methods_take_the_ingestion_date(method: str):
    parameters = inspect.signature(getattr(S3RawSink, method)).parameters
    assert 'ingestion_date' in parameters, f'{method} does not accept an ingestion date'
    assert parameters['ingestion_date'].default is inspect.Parameter.empty, (
        f'{method} makes the ingestion date optional, so a caller can silently fall back to a default'
    )


def test_date_option_is_passed_through():
    assert run.build_parser().parse_args(['--date', '2026-08-01']).date == '2026-08-01'


def test_date_defaults_to_today():
    assert run.build_parser().parse_args([]).date == str(date.today())


@pytest.mark.parametrize('value', ['2026-13-01', '01-08-2026', 'yesterday', '2026-08'])
def test_malformed_date_is_rejected(value: str):
    with pytest.raises(SystemExit):
        run.build_parser().parse_args(['--date', value])


class RecordingClient:
    def __init__(self):
        self.keys = []

    def put_object(self, **kwargs) -> None:
        self.keys.append(kwargs['Key'])


def build_sink() -> tuple[S3RawSink, RecordingClient]:
    sink = object.__new__(S3RawSink)
    client = RecordingClient()
    sink._client = client
    sink.bucket = 'bronze'
    return sink, client


def test_snapshot_paths_carry_the_given_date():
    sink, client = build_sink()
    sink.write_raw({'channel_id': 'UC1', 'channel_data': {}, 'video_data': {}}, '2026-08-01')
    sink.write_reference({'category_data': {}}, '2026-08-01')
    assert client.keys == [
        'youtube/channels/ingestion_date=2026-08-01/channel_id=UC1/channel_2026-08-01.json',
        'youtube/videos/ingestion_date=2026-08-01/channel_id=UC1/video_2026-08-01.json',
        'youtube/categories/ingestion_date=2026-08-01/category_2026-08-01.json'
    ]
