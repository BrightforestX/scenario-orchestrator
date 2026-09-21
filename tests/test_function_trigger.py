"""Tests for the function trigger HTTP API."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from orchestrator.surrealdb.schema import (
    ChainletConfig,
    ExecutionResult,
    Scenario,
    ScenarioStatus,
    TriggerType,
)
from orchestrator.triggers.function_trigger import FunctionTrigger


@pytest.fixture
def function_trigger() -> FunctionTrigger:
    """Create a FunctionTrigger for testing."""
    mock_client = MagicMock()
    mock_executor = AsyncMock(return_value=ExecutionResult(
        execution_id="exec:123",
        scenario_id="scenario:test",
        success=True,
    ))

    trigger = FunctionTrigger(
        client=mock_client,
        executor_callback=mock_executor,
    )

    return trigger


@pytest.fixture
def test_client(function_trigger: FunctionTrigger) -> TestClient:
    """Create a test client for the FastAPI app."""
    return TestClient(function_trigger.app)


class TestFunctionTriggerAPI:
    """Tests for FunctionTrigger HTTP API."""

    def test_health_check(self, test_client: TestClient) -> None:
        """Test health check endpoint."""
        response = test_client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_pending_count(self, test_client: TestClient) -> None:
        """Test pending count endpoint."""
        response = test_client.get("/pending")

        assert response.status_code == 200
        assert "pending_tasks" in response.json()

    def test_trigger_not_found(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test triggering a non-existent scenario."""
        function_trigger.client.get_scenario = AsyncMock(return_value=None)

        response = test_client.post(
            "/trigger",
            json={"scenario_id": "scenario:nonexistent"},
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_trigger_invalid_status(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test triggering a scenario with invalid status."""
        scenario = Scenario(
            id="scenario:running",
            name="Running Scenario",
            status=ScenarioStatus.RUNNING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
        )
        function_trigger.client.get_scenario = AsyncMock(return_value=scenario)

        response = test_client.post(
            "/trigger",
            json={"scenario_id": "scenario:running"},
        )

        assert response.status_code == 400
        assert "running" in response.json()["detail"].lower()

    def test_trigger_success_background(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test successfully triggering a scenario in background."""
        scenario = Scenario(
            id="scenario:pending",
            name="Pending Scenario",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
        )
        function_trigger.client.get_scenario = AsyncMock(return_value=scenario)
        function_trigger.client.update_scenario_status = AsyncMock()

        response = test_client.post(
            "/trigger",
            json={"scenario_id": "scenario:pending", "wait_for_result": False},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["message"] == "Execution triggered"
        assert data["scenario_id"] == "scenario:pending"

    def test_get_stats(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test getting execution statistics."""
        function_trigger.client.get_stats = AsyncMock(return_value={
            "total_scenarios": 100,
            "pending": 10,
            "running": 5,
            "completed": 80,
            "failed": 5,
            "total_executions": 150,
            "avg_latency_ms": 250.5,
        })

        response = test_client.get("/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["total_scenarios"] == 100
        assert data["pending"] == 10
        assert data["completed"] == 80
        assert data["avg_latency_ms"] == 250.5


class TestBatchTrigger:
    """Tests for batch triggering."""

    def test_batch_trigger_partial_success(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test batch triggering with mixed results."""
        pending_scenario = Scenario(
            id="scenario:pending1",
            name="Pending 1",
            status=ScenarioStatus.PENDING,
            interests=[],
            chainlet_config=ChainletConfig(model="test"),
        )

        def mock_get_scenario(scenario_id: str) -> Scenario | None:
            if scenario_id == "scenario:pending1" or scenario_id == "pending1":
                return pending_scenario
            return None

        function_trigger.client.get_scenario = AsyncMock(side_effect=mock_get_scenario)
        function_trigger.client.update_scenario_status = AsyncMock()

        response = test_client.post(
            "/trigger/batch",
            json={"scenario_ids": ["pending1", "nonexistent"]},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["triggered"] == 1
        assert data["failed"] == 1
        assert len(data["results"]) == 2


class TestCreateAndTrigger:
    """Tests for create and trigger endpoint."""

    def test_create_and_trigger(
        self, test_client: TestClient, function_trigger: FunctionTrigger
    ) -> None:
        """Test creating and triggering a scenario."""
        created_scenario = Scenario(
            id="scenario:new123",
            name="New Scenario",
            status=ScenarioStatus.PENDING,
            interests=[{"type": "prompt", "value": "test"}],
            chainlet_config=ChainletConfig(model="llama"),
        )

        function_trigger.client.create_scenario = AsyncMock(return_value=created_scenario)
        function_trigger.client.update_scenario_status = AsyncMock()

        response = test_client.post(
            "/trigger/create",
            json={
                "scenario": {
                    "name": "New Scenario",
                    "interests": [{"type": "prompt", "value": "test"}],
                    "chainlet_config": {"model": "llama"},
                },
                "wait_for_result": False,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["scenario_id"] == "scenario:new123"
        function_trigger.client.create_scenario.assert_called_once()
