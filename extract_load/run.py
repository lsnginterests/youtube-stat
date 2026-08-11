import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import yaml

from extract_load.extract.fetch_youtube_data import YoutubeClient
from extract_load.extract.extractor import Extractor
from extract_load.load.s3_loader import S3RawSink

_CHANNELS_CONFIG = Path(__file__).resolve().parent.joinpath('extract/channels.yml')
_REGION_CODE = 'US'
_LANGUAGE = 'en'


def ingestion_date(value: str) -> str:
    datetime.strptime(value, '%Y-%m-%d')
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m extract_load.run',
        description='Extract a daily YouTube snapshot into the raw bucket.'
    )
    parser.add_argument('--date', type=ingestion_date, metavar='YYYY-MM-DD',
                        default=str(date.today()),
                        help='ingestion date the snapshot is written under, defaults to today')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    channel_ids = []
    with open(_CHANNELS_CONFIG) as file:
        for ch in yaml.safe_load(file)['channels']:
            channel_ids.append((ch['id']))

    extractor = Extractor(YoutubeClient())
    loader = S3RawSink()
    for ch in channel_ids:
        data = extractor.extract_channel_data(ch)
        loader.write_raw(data, args.date)
    loader.write_reference(extractor.extract_category_data(_REGION_CODE, _LANGUAGE), args.date)
    return 0


if __name__ == '__main__':
    sys.exit(main())
