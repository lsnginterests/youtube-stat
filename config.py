from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent

class Settings(BaseSettings):
    yt_api_key: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket_raw: str
    s3_bucket_silver: str
    s3_bucket_gold: str = "gold"

    model_config = SettingsConfigDict(
        env_file=_PROJECT_ROOT / '.env',
        env_file_encoding='utf-8',
    )

settings = Settings()
