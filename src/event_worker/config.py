from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EWA_",
        env_file=".env",
        extra="ignore",
    )

    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias=AliasChoices("EWA_REDIS_URL", "REDIS_URL"),
    )
    queue_backend: str = Field(
        default="redis",
        validation_alias=AliasChoices("EWA_QUEUE_BACKEND", "QUEUE_BACKEND"),
    )
    lease_seconds: float = 30.0
    max_attempts: int = 3
    poll_interval: float = 0.5
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 60.0
    backoff_jitter: float = 0.1
    worker_id: str = ""
    completed_keep: int = 100
    key_prefix: str = "ew"


def load_settings() -> Settings:
    return Settings()
