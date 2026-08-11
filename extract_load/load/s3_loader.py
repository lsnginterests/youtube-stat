import boto3
import json

from config import get_settings

class S3RawSink:
    def __init__(self):
        settings = get_settings()
        self._client = boto3.client(
            's3',
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name='us-east-1'
        )
        self.bucket = settings.s3_bucket_raw

    def write_raw(self, data: dict, ingestion_date: str) -> dict:
        channel_id = data['channel_id']
        channels = data['channel_data']
        videos = data['video_data']
        channel_path = self.write_channel(channel_id, channels, ingestion_date)
        video_path = self.write_video(channel_id, videos, ingestion_date)
        return {
            'channel_path': channel_path,
            'video_path': video_path
        }

    def write_reference(self, data: dict, ingestion_date: str) -> dict:
        return {'category_path': self.write_category(data['category_data'], ingestion_date)}

    def _save_data(self, data_type: str, channel_id: str, data: dict, ingestion_date: str) -> str:
        data_dir = f'youtube/{data_type}s/ingestion_date={ingestion_date}/channel_id={channel_id}/'
        file_name = f'{data_type}_{ingestion_date}.json'
        path = data_dir + file_name
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

        self._client.put_object(
            Bucket = self.bucket,
            Key = path,
            Body = body,
            ContentType = 'application/json'
        )

        return path

    def write_video(self, channel_id: str, data: dict, ingestion_date: str):
        return S3RawSink._save_data(self, 'video', channel_id, data, ingestion_date)

    def write_channel(self, channel_id: str, data: dict, ingestion_date: str):
        return S3RawSink._save_data(self, 'channel', channel_id, data, ingestion_date)

    def write_category(self, data: dict, ingestion_date: str) -> str:
        path = f'youtube/categories/ingestion_date={ingestion_date}/category_{ingestion_date}.json'
        self._client.put_object(
            Bucket = self.bucket,
            Key = path,
            Body = json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType = 'application/json'
        )
        return path