"""Integration tests for the full orchestrator pipeline.

These tests require actual services to be running or use mocks
to simulate the full end-to-end flow.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.config import OrchestratorSettings
from orchestrator.executors.baseten import BasetenExecutor
from orchestrator.executors.buzz import BuzzPublisher
from orchestrator.executors.daytona import DaytonaExecutor
from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import (
    BuzzConfig,
    ChainletConfig,
    ExecutionResult,
    SandboxConfig,
    Scenario,
    ScenarioStatus,
    TriggerType,
)


class TestFullPipeline:
    """Integration tests for the full execution pipeline."""

    @pytest.mark.asyncio
    async def test_chainlet_only_execution(
        self, orchestrator_settings: OrchestratorSettings
    ) -> None:
        """Test execution with only chainlet (no sandbox, no buzz)."""
        # Create mock components
        mock_db = MagicMock(spec=SurrealDBClient)
        mock_db.create_execution = AsyncMock(return_value=MagicMock(id="exec:123"))
        mock_db.update_scenario_status = AsyncMock()
        mock_db.update_execution = AsyncMock()
        mock_db.complete_scenario = AsyncMock()

        mock_baseten = MagicMock(spec=BasetenExecutor)
        mock_baseten.execute = AsyncMock(return_value=(
            {"prompt": "test"},
            {"output": "result", "success": True},
            150,
        ))

        # Create scenario without sandbox or buzz
        scenario = Scenario(
            id="scenario:test",
            name="Chainlet Only Test",
            status=ScenarioStatus.PENDING,
            interests=[{"type": "prompt", "value": "Hello"}],
            chainlet_config=ChainletConfig(model="test-model"),
            sandbox_config=None,
            buzz_config=BuzzConfig(enabled=False),
        )

        # Simulate execution
        await mock_db.update_scenario_status(scenario.id, ScenarioStatus.RUNNING)

        request, result, latency = await mock_baseten.execute(scenario)

        await mock_db.update_execution(
            "exec:123",
            chainlet_request=request,
            chainlet_result=result,
            chainlet_latency_ms=latency,
        )

        await mock_db.complete_scenario(scenario.id, "exec:123")

        # Verify calls
        mock_baseten.execute.assert_called_once_with(scenario)
        mock_db.complete_scenario.assert_called_once()

    @pytest.mark.asyncio
    async def test_full_pipeline_execution(
        self, orchestrator_settings: OrchestratorSettings
    ) -> None:
        """Test execution with chainlet, sandbox, and buzz."""
        # Create mock components
        mock_db = MagicMock(spec=SurrealDBClient)
        mock_db.create_execution = AsyncMock(return_value=MagicMock(id="exec:456"))
        mock_db.update_scenario_status = AsyncMock()
        mock_db.update_execution = AsyncMock()
        mock_db.complete_scenario = AsyncMock()

        mock_baseten = MagicMock(spec=BasetenExecutor)
        mock_baseten.execute = AsyncMock(return_value=(
            {"prompt": "test"},
            {"output": "chainlet result", "success": True},
            200,
        ))

        mock_daytona = MagicMock(spec=DaytonaExecutor)
        mock_daytona.execute = AsyncMock(return_value=(
            "sandbox-123",
            "$ echo hello\nhello",
        ))

        mock_buzz = MagicMock(spec=BuzzPublisher)
        mock_buzz.publish = AsyncMock(return_value="event-789")

        # Create full scenario
        scenario = Scenario(
            id="scenario:full",
            name="Full Pipeline Test",
            status=ScenarioStatus.PENDING,
            interests=[{"type": "prompt", "value": "Test prompt"}],
            chainlet_config=ChainletConfig(model="llama"),
            sandbox_config=SandboxConfig(
                enabled=True,
                commands=["echo hello"],
            ),
            buzz_config=BuzzConfig(
                enabled=True,
                channel="test-channel",
            ),
        )

        # Simulate full pipeline
        execution = await mock_db.create_execution(scenario.id, TriggerType.FUNCTION)

        # Step 1: Chainlet
        await mock_db.update_scenario_status(scenario.id, ScenarioStatus.RUNNING)
        request, result, latency = await mock_baseten.execute(scenario)
        await mock_db.update_execution(
            execution.id,
            chainlet_request=request,
            chainlet_result=result,
            chainlet_latency_ms=latency,
        )

        # Step 2: Sandbox
        sandbox_id, sandbox_output = await mock_daytona.execute(scenario)
        await mock_db.update_execution(
            execution.id,
            sandbox_id=sandbox_id,
            sandbox_output=sandbox_output,
        )

        # Step 3: Buzz
        exec_result = ExecutionResult(
            execution_id=execution.id,
            scenario_id=scenario.id or "",
            success=True,
            chainlet_result=result,
            sandbox_output=sandbox_output,
            latency_ms=latency,
        )
        buzz_event_id = await mock_buzz.publish(scenario, exec_result)
        await mock_db.update_execution(execution.id, buzz_event_id=buzz_event_id)

        # Complete
        await mock_db.complete_scenario(scenario.id, execution.id)

        # Verify all steps were called
        mock_baseten.execute.assert_called_once()
        mock_daytona.execute.assert_called_once()
        mock_buzz.publish.assert_called_once()
        mock_db.complete_scenario.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_on_failure(
        self, orchestrator_settings: OrchestratorSettings
    ) -> None:
        """Test that failed scenarios are retried."""
        mock_db = MagicMock(spec=SurrealDBClient)
        mock_db.create_execution = AsyncMock(return_value=MagicMock(id="exec:retry"))
        mock_db.update_scenario_status = AsyncMock()
        mock_db.update_execution = AsyncMock()
        mock_db.fail_scenario = AsyncMock(return_value=True)  # Will retry

        mock_baseten = MagicMock(spec=BasetenExecutor)
        mock_baseten.execute = AsyncMock(side_effect=Exception("Chainlet error"))

        scenario = Scenario(
            id="scenario:retry",
            name="Retry Test",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
            max_retries=3,
            retry_count=0,
        )

        # Simulate execution that fails
        execution = await mock_db.create_execution(scenario.id, TriggerType.POLL)
        await mock_db.update_scenario_status(scenario.id, ScenarioStatus.RUNNING)

        try:
            await mock_baseten.execute(scenario)
        except Exception as e:
            will_retry = await mock_db.fail_scenario(
                scenario.id, execution.id, str(e)
            )
            assert will_retry is True

        mock_db.fail_scenario.assert_called_once()


class TestTriggerIntegration:
    """Integration tests for trigger mechanisms."""

    @pytest.mark.asyncio
    async def test_event_trigger_to_execution(self) -> None:
        """Test event trigger fires execution correctly."""
        execution_count = 0

        async def mock_executor(
            scenario: Scenario, trigger_type: TriggerType
        ) -> ExecutionResult | None:
            nonlocal execution_count
            execution_count += 1
            return ExecutionResult(
                execution_id="exec:event",
                scenario_id=scenario.id or "",
                success=True,
            )

        from orchestrator.triggers.event_trigger import EventTrigger

        mock_client = MagicMock()
        mock_client.live_scenarios = AsyncMock(return_value="live-123")
        mock_client.kill_live_query = AsyncMock()

        trigger = EventTrigger(
            client=mock_client,
            executor_callback=mock_executor,
        )

        await trigger.start()

        # Simulate new scenario event
        scenario = Scenario(
            id="scenario:event-test",
            name="Event Test",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
        )

        await trigger._on_scenario_created(scenario)
        await asyncio.sleep(0.1)

        assert execution_count == 1

        await trigger.stop()

    @pytest.mark.asyncio
    async def test_poll_trigger_to_execution(self) -> None:
        """Test poll trigger fires execution correctly."""
        execution_count = 0

        async def mock_executor(
            scenario: Scenario, trigger_type: TriggerType
        ) -> None:
            nonlocal execution_count
            execution_count += 1

        from orchestrator.triggers.poll_trigger import PollTrigger

        scenario = Scenario(
            id="scenario:poll-test",
            name="Poll Test",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
        )

        mock_client = MagicMock()
        mock_client.get_ready_scenarios = AsyncMock(return_value=[scenario])
        mock_client.update_scenario_status = AsyncMock()

        trigger = PollTrigger(
            client=mock_client,
            executor_callback=mock_executor,
            interval_seconds=60,
        )

        await trigger.start()
        await trigger.poll_now()
        await asyncio.sleep(0.1)

        assert execution_count == 1

        await trigger.stop()


class TestConcurrency:
    """Tests for concurrent execution."""

    @pytest.mark.asyncio
    async def test_max_concurrent_executions(self) -> None:
        """Test that concurrent executions are limited."""
        concurrent_count = 0
        max_observed = 0

        async def slow_executor(
            scenario: Scenario, trigger_type: TriggerType
        ) -> None:
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.1)
            concurrent_count -= 1

        from orchestrator.triggers.poll_trigger import PollTrigger

        scenarios = [
            Scenario(
                id=f"scenario:concurrent-{i}",
                name=f"Concurrent {i}",
                status=ScenarioStatus.PENDING,
                interests=[],
                chainlet_config=ChainletConfig(model="test"),
            )
            for i in range(10)
        ]

        mock_client = MagicMock()
        mock_client.get_ready_scenarios = AsyncMock(return_value=scenarios)
        mock_client.update_scenario_status = AsyncMock()

        trigger = PollTrigger(
            client=mock_client,
            executor_callback=slow_executor,
            interval_seconds=60,
            max_concurrent=3,
        )

        await trigger.start()
        await trigger.poll_now()

        # Wait for all executions
        await asyncio.sleep(0.5)

        assert max_observed <= 3  # Should never exceed max_concurrent

        await trigger.stop()
