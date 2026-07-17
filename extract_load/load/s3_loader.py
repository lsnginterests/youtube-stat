import boto3
from datetime import datetime
import json

from extract_load import settings

class S3RawSink:
    def __init__(self):
        self._client = boto3.client(
            's3',
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name='us-east-1'
        )
        self.bucket = settings.s3_bucket_raw

    def write_raw(self, data: dict) -> dict:
        channel_id = data['channel_id']
        channels = data['channel_data']
        videos = data['video_data']
        ingestion_ts = datetime.now()
        channel_path = self.write_channel(channel_id, channels, ingestion_ts)
        video_path = self.write_video(channel_id, videos, ingestion_ts)
        return {
            'channel_path': channel_path,
            'video_path': video_path
        }

    def _save_data(self, data_type: str, channel_id: str, data: dict, ts: datetime) -> str:
        ts_str = S3RawSink.dttm_to_str(ts)
        date_str = ts.strftime('%Y-%m-%d')

        data_dir = f'youtube/{data_type}s/ingestion_date={date_str}/channel_id={channel_id}/'
        file_name = f'{data_type}_{ts_str}.json'
        path = data_dir + file_name
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

        self._client.put_object(
            Bucket = self.bucket,
            Key = path,
            Body = body,
            ContentType = 'application/json'
        )

        return path

    def write_video(self, channel_id: str, data: dict, ts: datetime):
        return S3RawSink._save_data(self, 'video', channel_id, data, ts)

    def write_channel(self, channel_id: str, data: dict, ts: datetime):
        return S3RawSink._save_data(self, 'channel', channel_id, data, ts)
    
    @staticmethod
    def dttm_to_str(ts: datetime) -> str:
        return ts.strftime('%Y-%m-%d_%H-%M-%S')