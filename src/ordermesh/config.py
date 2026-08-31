from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ORDERMESH_",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "OrderMesh API"
    app_version: str = "0.1.0"
    orders_database_url: str
    inventory_database_url: str
    rabbitmq_url: str
    log_level: str = "INFO"
    retry_delay_ms: int = 2000
    max_retries: int = 3


@lru_cache
def get_settings() -> Settings:
    return Settings()
