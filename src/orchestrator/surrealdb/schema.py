"""Pydantic models for SurrealDB schema."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ScenarioStatus(str, Enum):
    """Status of a scenario."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionStatus(str, Enum):
    """Status of an execution."""

    STARTED = "started"
    CHAINLET_RUNNING = "chainlet_running"
    SANDBOX_RUNNING = "sandbox_running"
    PUBLISHING = "publishing"
    COMPLETED = "completed"
    FAILED = "failed"


class TriggerType(str, Enum):
    """Type of trigger that initiated the execution."""

    EVENT = "event"
    POLL = "poll"
    FUNCTION = "function"


class InterestType(str, Enum):
    """Type of interest."""

    PROMPT = "prompt"
    CODE = "code"
    DATA = "data"
    CONFIG = "config"


class ChainletConfig(BaseModel):
    """Configuration for Baseten chainlet execution."""

    model: str = Field(description="Model identifier or endpoint")
    prompt_template: str | None = Field(default=None, description="Prompt template with placeholders")
    parameters: dict[str, Any] = Field(default_factory=dict, description="Model parameters")
    timeout_seconds: int = Field(default=120, description="Request timeout")


class SandboxConfig(BaseModel):
    """Configuration for Daytona sandbox execution."""

    enabled: bool = Field(default=False, description="Whether to create a sandbox")
    cpu: int = Field(default=2, ge=1, le=4, description="CPU cores")
    memory: int = Field(default=4, ge=1, le=8, description="Memory in GB")
    disk: int = Field(default=5, ge=1, le=10, description="Disk in GB")
    commands: list[str] = Field(default_factory=list, description="Commands to execute")
    files: dict[str, str] = Field(default_factory=dict, description="Files to upload (path -> content)")
    env: dict[str, str] = Field(default_factory=dict, description="Environment variables")


class BuzzConfig(BaseModel):
    """Configuration for Buzz channel publishing."""

    enabled: bool = Field(default=True, description="Whether to publish to Buzz")
    channel: str | None = Field(default=None, description="Target channel (uses default if None)")
    include_chainlet_result: bool = Field(default=True, description="Include chainlet result in message")
    include_sandbox_output: bool = Field(default=True, description="Include sandbox output in message")
    custom_message: str | None = Field(default=None, description="Custom message template")


class Interest(BaseModel):
    """An interest that defines part of a scenario."""

    id: str | None = Field(default=None, description="SurrealDB record ID")
    name: str = Field(description="Interest name")
    type: InterestType = Field(description="Type of interest")
    value: Any = Field(description="Interest value")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional metadata")
    created_at: datetime | None = Field(default=None, description="Creation timestamp")


class Scenario(BaseModel):
    """A scenario representing a permutation of interests to execute."""

    id: str | None = Field(default=None, description="SurrealDB record ID")
    name: str = Field(description="Scenario name")
    description: str | None = Field(default=None, description="Scenario description")
    status: ScenarioStatus = Field(default=ScenarioStatus.PENDING, description="Current status")
    interests: list[dict[str, Any]] = Field(default_factory=list, description="Interest objects")
    chainlet_config: ChainletConfig = Field(description="Chainlet configuration")
    sandbox_config: SandboxConfig | None = Field(default=None, description="Optional sandbox config")
    buzz_config: BuzzConfig | None = Field(default=None, description="Optional Buzz config")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional metadata")
    priority: int = Field(default=0, description="Execution priority")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_count: int = Field(default=0, description="Current retry count")
    created_at: datetime | None = Field(default=None, description="Creation timestamp")
    updated_at: datetime | None = Field(default=None, description="Last update timestamp")
    executed_at: datetime | None = Field(default=None, description="Execution timestamp")

    @classmethod
    def from_surreal(cls, data: dict[str, Any]) -> "Scenario":
        """Create a Scenario from SurrealDB record data."""
        # Handle nested configs
        chainlet_config = data.get("chainlet_config", {})
        if isinstance(chainlet_config, dict):
            chainlet_config = ChainletConfig(**chainlet_config)

        sandbox_config = data.get("sandbox_config")
        if sandbox_config and isinstance(sandbox_config, dict):
            sandbox_config = SandboxConfig(**sandbox_config)

        buzz_config = data.get("buzz_config")
        if buzz_config and isinstance(buzz_config, dict):
            buzz_config = BuzzConfig(**buzz_config)

        return cls(
            id=str(data.get("id", "")),
            name=data.get("name", ""),
            description=data.get("description"),
            status=ScenarioStatus(data.get("status", "pending")),
            interests=data.get("interests", []),
            chainlet_config=chainlet_config,
            sandbox_config=sandbox_config,
            buzz_config=buzz_config,
            metadata=data.get("metadata"),
            priority=data.get("priority", 0),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            executed_at=data.get("executed_at"),
        )


class Execution(BaseModel):
    """Record of a scenario execution."""

    id: str | None = Field(default=None, description="SurrealDB record ID")
    scenario: str = Field(description="Reference to scenario record")
    trigger_type: TriggerType = Field(description="Type of trigger")
    status: ExecutionStatus = Field(default=ExecutionStatus.STARTED, description="Current status")
    chainlet_request: dict[str, Any] | None = Field(default=None, description="Request to chainlet")
    chainlet_result: dict[str, Any] | None = Field(default=None, description="Response from chainlet")
    chainlet_latency_ms: int | None = Field(default=None, description="Chainlet latency in ms")
    sandbox_id: str | None = Field(default=None, description="Daytona sandbox ID")
    sandbox_output: str | None = Field(default=None, description="Sandbox execution output")
    buzz_event_id: str | None = Field(default=None, description="Nostr event ID")
    error: str | None = Field(default=None, description="Error message if failed")
    started_at: datetime | None = Field(default=None, description="Start timestamp")
    completed_at: datetime | None = Field(default=None, description="Completion timestamp")

    @classmethod
    def from_surreal(cls, data: dict[str, Any]) -> "Execution":
        """Create an Execution from SurrealDB record data."""
        scenario = data.get("scenario", "")
        if isinstance(scenario, dict):
            scenario = str(scenario.get("id", scenario))

        return cls(
            id=str(data.get("id", "")),
            scenario=str(scenario),
            trigger_type=TriggerType(data.get("trigger_type", "event")),
            status=ExecutionStatus(data.get("status", "started")),
            chainlet_request=data.get("chainlet_request"),
            chainlet_result=data.get("chainlet_result"),
            chainlet_latency_ms=data.get("chainlet_latency_ms"),
            sandbox_id=data.get("sandbox_id"),
            sandbox_output=data.get("sandbox_output"),
            buzz_event_id=data.get("buzz_event_id"),
            error=data.get("error"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
        )


class ScenarioCreate(BaseModel):
    """Request model for creating a scenario."""

    name: str = Field(description="Scenario name")
    description: str | None = Field(default=None, description="Scenario description")
    interests: list[dict[str, Any]] = Field(default_factory=list, description="Interest objects")
    chainlet_config: ChainletConfig = Field(description="Chainlet configuration")
    sandbox_config: SandboxConfig | None = Field(default=None, description="Optional sandbox config")
    buzz_config: BuzzConfig | None = Field(default=None, description="Optional Buzz config")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional metadata")
    priority: int = Field(default=0, description="Execution priority")


class ExecutionResult(BaseModel):
    """Result of a scenario execution."""

    execution_id: str = Field(description="Execution record ID")
    scenario_id: str = Field(description="Scenario record ID")
    success: bool = Field(description="Whether execution succeeded")
    chainlet_result: dict[str, Any] | None = Field(default=None, description="Chainlet result")
    sandbox_output: str | None = Field(default=None, description="Sandbox output")
    buzz_event_id: str | None = Field(default=None, description="Buzz event ID")
    error: str | None = Field(default=None, description="Error if failed")
    latency_ms: int | None = Field(default=None, description="Total latency in ms")
