import yaml
from pathlib import Path
from extract_load.extract.fetch_youtube_data import YoutubeClient
from extract_load.extract.extractor import Extractor
from extract_load.load.s3_loader import S3RawSink

_CHANNELS_CONFIG = Path(__file__).resolve().parent.joinpath('extract/channels.yml')
_REGION_CODE = 'US'
_LANGUAGE = 'en'

if __name__ == '__main__':
    channel_ids = []
    with open(_CHANNELS_CONFIG) as file:
        for ch in yaml.safe_load(file)['channels']:
            channel_ids.append((ch['id']))
    extractor = Extractor(YoutubeClient())
    loader = S3RawSink()
    for ch in channel_ids:
        data = extractor.extract_channel_data(ch)
        loader.write_raw(data)
    loader.write_reference(extractor.extract_category_data(_REGION_CODE, _LANGUAGE))