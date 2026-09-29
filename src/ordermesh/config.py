from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
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
    max_retries: int = Field(default=3, ge=0, le=10)
    api_keys: dict[str, SecretStr] = Field(default_factory=dict)
    allow_failure_simulation: bool = False

    @model_validator(mode="after")
    def validate_clients(self) -> "Settings":
        values = [key.get_secret_value() for key in self.api_keys.values()]
        if any(not 1 <= len(client) <= 100 for client in self.api_keys):
            raise ValueError("ID клиента должен содержать от 1 до 100 символов")
        if any(len(value) < 16 for value in values) or len(values) != len(set(values)):
            raise ValueError("Ключи клиентов должны быть уникальными и не короче 16 символов")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
