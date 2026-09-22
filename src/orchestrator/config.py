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


class PipelineSettings(BaseSettings):
    """Durable scenario analysis pipeline settings."""

    model_config = SettingsConfigDict(env_prefix="PIPELINE_")

    staging_dir: str = Field(default="/tmp/scenario-pipeline", description="JSONL staging directory")
    trino_host: str = Field(default="localhost", description="Trino coordinator host")
    trino_port: int = Field(default=8080, description="Trino coordinator port")
    trino_user: str = Field(default="pipeline", description="Trino user")
    trino_catalog: str = Field(default="hive", description="Trino catalog")
    trino_schema: str = Field(default="default", description="Trino schema")


class TemporalSettings(BaseSettings):
    """Temporal worker settings."""

    model_config = SettingsConfigDict(env_prefix="TEMPORAL_")

    host: str = Field(default="localhost:7233", description="Temporal frontend address")
    namespace: str = Field(default="default", description="Temporal namespace")
    task_queue: str = Field(default="scenario-pipeline", description="Task queue name")
    pipeline_api_url: str = Field(
        default="http://localhost:8080", description="Base URL for pipeline HTTP API"
    )
    gate_url: str = Field(
        default="http://localhost:8090/v1/gate", description="TypeScript keep-or-revert gate URL"
    )
    use_inline_gate: bool = Field(
        default=False,
        description="When true, apply gate criteria inline in activity (tests only)",
    )


class GateSettings(BaseSettings):
    """Keep-or-revert gate service settings."""

    model_config = SettingsConfigDict(env_prefix="GATE_")

    port: int = Field(default=8090, description="Gate HTTP port")
    default_min_score: float = Field(default=0.0, description="Default minimum score threshold")


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
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    temporal: TemporalSettings = Field(default_factory=TemporalSettings)
    gate: GateSettings = Field(default_factory=GateSettings)


def get_settings() -> OrchestratorSettings:
    """Get orchestrator settings from environment."""
    return OrchestratorSettings(
        surrealdb=SurrealDBSettings(),
        baseten=BasetenSettings(),
        daytona=DaytonaSettings(),
        buzz=BuzzSettings(),
        pipeline=PipelineSettings(),
        temporal=TemporalSettings(),
        gate=GateSettings(),
    )
