"""Tests for schema models."""

import pytest

from orchestrator.surrealdb.schema import (
    BuzzConfig,
    ChainletConfig,
    Execution,
    ExecutionStatus,
    InterestType,
    SandboxConfig,
    Scenario,
    ScenarioCreate,
    ScenarioStatus,
    TriggerType,
)


class TestChainletConfig:
    """Tests for ChainletConfig model."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ChainletConfig(model="test-model")

        assert config.model == "test-model"
        assert config.prompt_template is None
        assert config.parameters == {}
        assert config.timeout_seconds == 120

    def test_full_config(self) -> None:
        """Test full configuration."""
        config = ChainletConfig(
            model="llama-3",
            prompt_template="Question: {q}",
            parameters={"max_tokens": 500},
            timeout_seconds=60,
        )

        assert config.model == "llama-3"
        assert config.prompt_template == "Question: {q}"
        assert config.parameters == {"max_tokens": 500}
        assert config.timeout_seconds == 60


class TestSandboxConfig:
    """Tests for SandboxConfig model."""

    def test_default_values(self) -> None:
        """Test default sandbox configuration."""
        config = SandboxConfig()

        assert config.enabled is False
        assert config.cpu == 2
        assert config.memory == 4
        assert config.disk == 5
        assert config.commands == []
        assert config.files == {}
        assert config.env == {}

    def test_validation(self) -> None:
        """Test validation constraints."""
        # CPU max is 4
        with pytest.raises(ValueError):
            SandboxConfig(cpu=10)

        # Memory max is 8
        with pytest.raises(ValueError):
            SandboxConfig(memory=16)


class TestBuzzConfig:
    """Tests for BuzzConfig model."""

    def test_default_values(self) -> None:
        """Test default Buzz configuration."""
        config = BuzzConfig()

        assert config.enabled is True
        assert config.channel is None
        assert config.include_chainlet_result is True
        assert config.include_sandbox_output is True
        assert config.custom_message is None


class TestScenarioCreate:
    """Tests for ScenarioCreate model."""

    def test_minimal_scenario(self) -> None:
        """Test creating a minimal scenario."""
        create = ScenarioCreate(
            name="Test",
            chainlet_config=ChainletConfig(model="test"),
        )

        assert create.name == "Test"
        assert create.description is None
        assert create.interests == []
        assert create.priority == 0

    def test_full_scenario(self) -> None:
        """Test creating a full scenario."""
        create = ScenarioCreate(
            name="Full Test",
            description="A full test scenario",
            interests=[
                {"name": "prompt", "type": "prompt", "value": "Hello"},
            ],
            chainlet_config=ChainletConfig(model="test"),
            sandbox_config=SandboxConfig(enabled=True, commands=["echo hello"]),
            buzz_config=BuzzConfig(channel="results"),
            metadata={"key": "value"},
            priority=5,
        )

        assert create.name == "Full Test"
        assert create.description == "A full test scenario"
        assert len(create.interests) == 1
        assert create.sandbox_config is not None
        assert create.sandbox_config.enabled is True
        assert create.buzz_config is not None
        assert create.priority == 5


class TestScenario:
    """Tests for Scenario model."""

    def test_from_surreal(self) -> None:
        """Test creating Scenario from SurrealDB data."""
        data = {
            "id": "scenario:abc123",
            "name": "Test Scenario",
            "description": "Description",
            "status": "pending",
            "interests": [{"type": "prompt", "value": "test"}],
            "chainlet_config": {"model": "llama", "timeout_seconds": 60},
            "priority": 3,
            "max_retries": 5,
            "retry_count": 1,
        }

        scenario = Scenario.from_surreal(data)

        assert scenario.id == "scenario:abc123"
        assert scenario.name == "Test Scenario"
        assert scenario.status == ScenarioStatus.PENDING
        assert scenario.chainlet_config.model == "llama"
        assert scenario.priority == 3
        assert scenario.max_retries == 5
        assert scenario.retry_count == 1

    def test_status_enum(self) -> None:
        """Test scenario status values."""
        assert ScenarioStatus.PENDING.value == "pending"
        assert ScenarioStatus.RUNNING.value == "running"
        assert ScenarioStatus.COMPLETED.value == "completed"
        assert ScenarioStatus.FAILED.value == "failed"
        assert ScenarioStatus.CANCELLED.value == "cancelled"


class TestExecution:
    """Tests for Execution model."""

    def test_from_surreal(self) -> None:
        """Test creating Execution from SurrealDB data."""
        data = {
            "id": "execution:xyz789",
            "scenario": "scenario:abc123",
            "trigger_type": "event",
            "status": "completed",
            "chainlet_result": {"output": "test output"},
            "chainlet_latency_ms": 150,
            "sandbox_id": "sandbox-123",
            "buzz_event_id": "event-456",
        }

        execution = Execution.from_surreal(data)

        assert execution.id == "execution:xyz789"
        assert execution.scenario == "scenario:abc123"
        assert execution.trigger_type == TriggerType.EVENT
        assert execution.status == ExecutionStatus.COMPLETED
        assert execution.chainlet_result == {"output": "test output"}
        assert execution.chainlet_latency_ms == 150

    def test_trigger_types(self) -> None:
        """Test trigger type values."""
        assert TriggerType.EVENT.value == "event"
        assert TriggerType.POLL.value == "poll"
        assert TriggerType.FUNCTION.value == "function"

    def test_execution_status(self) -> None:
        """Test execution status values."""
        assert ExecutionStatus.STARTED.value == "started"
        assert ExecutionStatus.CHAINLET_RUNNING.value == "chainlet_running"
        assert ExecutionStatus.SANDBOX_RUNNING.value == "sandbox_running"
        assert ExecutionStatus.PUBLISHING.value == "publishing"
        assert ExecutionStatus.COMPLETED.value == "completed"
        assert ExecutionStatus.FAILED.value == "failed"


class TestInterestType:
    """Tests for InterestType enum."""

    def test_interest_types(self) -> None:
        """Test interest type values."""
        assert InterestType.PROMPT.value == "prompt"
        assert InterestType.CODE.value == "code"
        assert InterestType.DATA.value == "data"
        assert InterestType.CONFIG.value == "config"
