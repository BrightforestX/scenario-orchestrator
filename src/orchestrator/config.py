"""Configuration management for the orchestrator."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SurrealDBSettings(BaseSettings):
    """SurrealDB connection settings."""

    model_config = SettingsConfigDict(env_prefix="SURREALDB_")

    url: str = Field(default="ws://localhost:8000", description="SurrealDB WebSocket URL")
    namespace: str = Field(default="buzz", description="SurrealDB namespace")
    database: str = Field(default="scenarios", description="SurrealDB database name")
    username: str = Field(default="root", description="SurrealDB username")
    password: str = Field(default="root", description="SurrealDB password")


class BasetenSettings(BaseSettings):
    """Baseten chainlet settings."""

    model_config = SettingsConfigDict(env_prefix="BASETEN_")

    api_key: str = Field(default="", description="Baseten API key")
    chain_endpoint: str = Field(default="", description="Baseten chain endpoint URL")
    timeout_seconds: int = Field(default=120, description="Request timeout in seconds")


class DaytonaSettings(BaseSettings):
    """Daytona sandbox settings."""

    model_config = SettingsConfigDict(env_prefix="DAYTONA_")

    api_key: str = Field(default="", description="Daytona API key")
    target_region: str = Field(default="us", description="Target region for sandboxes")
    auto_stop_minutes: int = Field(default=15, description="Auto-stop interval in minutes")
    auto_archive_minutes: int = Field(default=10080, description="Auto-archive interval in minutes")


class BuzzSettings(BaseSettings):
    """Buzz relay settings."""

    model_config = SettingsConfigDict(env_prefix="BUZZ_")

    relay_url: str = Field(default="ws://localhost:3000", description="Buzz relay WebSocket URL")
    private_key: str = Field(default="", description="Nostr private key (hex)")
    default_channel: str = Field(default="scenario-results", description="Default channel for results")


class OrchestratorSettings(BaseSettings):
    """Orchestrator service settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    poll_interval_seconds: int = Field(default=30, description="Polling interval in seconds")
    function_trigger_port: int = Field(default=8080, description="HTTP port for function trigger")
    log_level: str = Field(default="INFO", description="Logging level")

    # Nested settings
    surrealdb: SurrealDBSettings = Field(default_factory=SurrealDBSettings)
    baseten: BasetenSettings = Field(default_factory=BasetenSettings)
    daytona: DaytonaSettings = Field(default_factory=DaytonaSettings)
    buzz: BuzzSettings = Field(default_factory=BuzzSettings)


def get_settings() -> OrchestratorSettings:
    """Get orchestrator settings from environment."""
    return OrchestratorSettings(
        surrealdb=SurrealDBSettings(),
        baseten=BasetenSettings(),
        daytona=DaytonaSettings(),
        buzz=BuzzSettings(),
    )
