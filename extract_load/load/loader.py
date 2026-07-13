from pathlib import Path
from datetime import datetime
import json

_DEFAULT_LOCAL_PATH = Path(__file__).resolve().parents[1].joinpath('local_base/raw')

class LocalRawSink:
    def __init__(self, base: Path = _DEFAULT_LOCAL_PATH):
        self.base = base
        self.base.mkdir(parents=True, exist_ok=True)

    def write_raw(self, data: dict) -> dict:
        base = self.base
        channel_id = data['channel_id']
        channels = data['channel_data']
        videos = data['video_data']
        ingestion_ts = datetime.now()
        channel_path = self.write_channel(base, channel_id, channels, ingestion_ts)
        video_path = self.write_video(base, channel_id, videos, ingestion_ts)
        return {
            'channel_path': channel_path,
            'video_path': video_path
        }

    @staticmethod
    def _save_data(base: Path, data_type: str, channel_id: str, data: dict, ts: datetime) -> str:
        ts_str = LocalRawSink.dttm_to_str(ts)
        date_str = ts.strftime('%Y-%m-%d')

        data_dir = base.joinpath(f'{data_type}s/channel_id={channel_id}/date={date_str}')
        data_dir.mkdir(parents=True, exist_ok=True)

        file_name = f'{data_type}_{ts_str}.json'
        path = data_dir.joinpath(file_name)

        with open(path, 'w') as file:
            json.dump(data, file, indent=2, default=str)

        return str(path)

    @staticmethod
    def write_video(base, channel_id: str, data: dict, ts: datetime):
        return LocalRawSink._save_data(base, 'video', channel_id, data, ts)

    @staticmethod
    def write_channel(base, channel_id: str, data: dict, ts: datetime):
        return LocalRawSink._save_data(base, 'channel', channel_id, data, ts)
    
    @staticmethod
    def dttm_to_str(ts: datetime) -> str:
        return ts.strftime('%Y-%m-%d_%H-%M-%S')