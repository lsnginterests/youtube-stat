from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    yt_api_key: str

    model_config = SettingsConfigDict(
        env_file=_PROJECT_ROOT / '.env',
        env_file_encoding='utf-8',
    )

settings = Settings()