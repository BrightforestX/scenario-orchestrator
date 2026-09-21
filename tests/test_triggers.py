"""Tests for trigger mechanisms."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.surrealdb.schema import (
    ExecutionResult,
    Scenario,
    ScenarioStatus,
    TriggerType,
)
from orchestrator.triggers.event_trigger import EventTrigger
from orchestrator.triggers.poll_trigger import PollTrigger


class TestEventTrigger:
    """Tests for EventTrigger."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self) -> None:
        """Test starting and stopping the event trigger."""
        mock_client = MagicMock()
        mock_client.live_scenarios = AsyncMock(return_value="live-id-123")
        mock_client.kill_live_query = AsyncMock()

        mock_executor = AsyncMock()

        trigger = EventTrigger(
            client=mock_client,
            executor_callback=mock_executor,
        )

        await trigger.start()
        assert trigger.is_running
        mock_client.live_scenarios.assert_called_once()

        await trigger.stop()
        assert not trigger.is_running
        mock_client.kill_live_query.assert_called_once_with("live-id-123")

    @pytest.mark.asyncio
    async def test_handles_new_pending_scenario(self) -> None:
        """Test that new pending scenarios trigger execution."""
        mock_client = MagicMock()
        mock_client.live_scenarios = AsyncMock(return_value="live-id")
        mock_client.kill_live_query = AsyncMock()

        executed_scenarios = []

        async def mock_executor(scenario: Scenario, trigger_type: TriggerType) -> None:
            executed_scenarios.append((scenario, trigger_type))

        trigger = EventTrigger(
            client=mock_client,
            executor_callback=mock_executor,
        )

        await trigger.start()

        # Simulate receiving a new pending scenario
        scenario = Scenario(
            id="scenario:new",
            name="New Scenario",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=MagicMock(),
        )

        await trigger._on_scenario_created(scenario)

        # Wait for async task to complete
        await asyncio.sleep(0.1)

        assert len(executed_scenarios) == 1
        assert executed_scenarios[0][0].id == "scenario:new"
        assert executed_scenarios[0][1] == TriggerType.EVENT

        await trigger.stop()

    @pytest.mark.asyncio
    async def test_ignores_non_pending_scenarios(self) -> None:
        """Test that non-pending scenarios are not triggered."""
        mock_client = MagicMock()
        mock_client.live_scenarios = AsyncMock(return_value="live-id")
        mock_client.kill_live_query = AsyncMock()

        executed_scenarios = []

        async def mock_executor(scenario: Scenario, trigger_type: TriggerType) -> None:
            executed_scenarios.append(scenario)

        trigger = EventTrigger(
            client=mock_client,
            executor_callback=mock_executor,
        )

        await trigger.start()

        # Simulate receiving a completed scenario
        scenario = Scenario(
            id="scenario:completed",
            name="Completed Scenario",
            status=ScenarioStatus.COMPLETED,
            interests=[],
            chainlet_config=MagicMock(),
        )

        await trigger._on_scenario_created(scenario)
        await asyncio.sleep(0.1)

        assert len(executed_scenarios) == 0

        await trigger.stop()


class TestPollTrigger:
    """Tests for PollTrigger."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self) -> None:
        """Test starting and stopping the poll trigger."""
        mock_client = MagicMock()
        mock_client.get_pending_scenarios = AsyncMock(return_value=[])
        mock_client.get_ready_scenarios = AsyncMock(return_value=[])

        mock_executor = AsyncMock()

        trigger = PollTrigger(
            client=mock_client,
            executor_callback=mock_executor,
            interval_seconds=1,
        )

        await trigger.start()
        assert trigger.is_running

        await trigger.stop()
        assert not trigger.is_running

    @pytest.mark.asyncio
    async def test_polls_for_scenarios(self) -> None:
        """Test that polling finds and executes scenarios."""
        executed_scenarios = []

        async def mock_executor(scenario: Scenario, trigger_type: TriggerType) -> None:
            executed_scenarios.append((scenario, trigger_type))

        scenario = Scenario(
            id="scenario:poll-test",
            name="Poll Test",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=MagicMock(),
        )

        mock_client = MagicMock()
        mock_client.get_ready_scenarios = AsyncMock(return_value=[scenario])
        mock_client.update_scenario_status = AsyncMock()

        trigger = PollTrigger(
            client=mock_client,
            executor_callback=mock_executor,
            interval_seconds=60,  # Long interval so it doesn't auto-poll
            check_dependencies=True,
        )

        await trigger.start()

        # Manually trigger a poll
        count = await trigger.poll_now()

        # Wait for async execution
        await asyncio.sleep(0.1)

        assert count == 1
        assert len(executed_scenarios) == 1
        assert executed_scenarios[0][0].id == "scenario:poll-test"
        assert executed_scenarios[0][1] == TriggerType.POLL

        await trigger.stop()

    @pytest.mark.asyncio
    async def test_avoids_duplicate_processing(self) -> None:
        """Test that recently processed scenarios are not re-processed."""
        executed_count = 0

        async def mock_executor(scenario: Scenario, trigger_type: TriggerType) -> None:
            nonlocal executed_count
            executed_count += 1

        scenario = Scenario(
            id="scenario:duplicate",
            name="Duplicate Test",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=MagicMock(),
        )

        mock_client = MagicMock()
        # Return the same scenario multiple times
        mock_client.get_ready_scenarios = AsyncMock(return_value=[scenario])
        mock_client.update_scenario_status = AsyncMock()

        trigger = PollTrigger(
            client=mock_client,
            executor_callback=mock_executor,
            interval_seconds=60,
        )

        await trigger.start()

        # Poll multiple times
        await trigger.poll_now()
        await asyncio.sleep(0.1)
        await trigger.poll_now()
        await asyncio.sleep(0.1)
        await trigger.poll_now()
        await asyncio.sleep(0.1)

        # Should only execute once
        assert executed_count == 1

        await trigger.stop()
