"""HTTP endpoint trigger for SurrealDB custom functions."""

import asyncio
from typing import Any, Callable, Coroutine

import structlog
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import (
    Scenario,
    ScenarioCreate,
    ScenarioStatus,
    TriggerType,
    ExecutionResult,
)

logger = structlog.get_logger(__name__)


# Type alias for the executor callback
ExecutorCallback = Callable[[Scenario, TriggerType], Coroutine[Any, Any, ExecutionResult | None]]


class TriggerRequest(BaseModel):
    """Request to trigger a scenario execution."""

    scenario_id: str = Field(description="ID of the scenario to trigger")
    wait_for_result: bool = Field(
        default=False, description="Whether to wait for execution result"
    )


class TriggerResponse(BaseModel):
    """Response from triggering a scenario."""

    success: bool
    message: str
    scenario_id: str
    execution_id: str | None = None
    result: ExecutionResult | None = None


class BatchTriggerRequest(BaseModel):
    """Request to trigger multiple scenarios."""

    scenario_ids: list[str] = Field(description="IDs of scenarios to trigger")


class BatchTriggerResponse(BaseModel):
    """Response from batch triggering scenarios."""

    triggered: int
    failed: int
    results: list[TriggerResponse]


class CreateAndTriggerRequest(BaseModel):
    """Request to create and immediately trigger a scenario."""

    scenario: ScenarioCreate
    wait_for_result: bool = Field(default=False)


class StatsResponse(BaseModel):
    """Execution statistics response."""

    total_scenarios: int | None = None
    pending: int | None = None
    running: int | None = None
    completed: int | None = None
    failed: int | None = None
    total_executions: int | None = None
    avg_latency_ms: float | None = None


class FunctionTrigger:
    """HTTP API trigger for scenario execution.

    This trigger exposes HTTP endpoints that can be called by SurrealDB
    custom functions or external services to trigger scenario execution.
    """

    def __init__(
        self,
        client: SurrealDBClient,
        executor_callback: ExecutorCallback,
        max_concurrent: int = 10,
    ) -> None:
        """Initialize the function trigger.

        Args:
            client: SurrealDB client instance
            executor_callback: Async function to call for execution
            max_concurrent: Maximum concurrent executions
        """
        self.client = client
        self.executor_callback = executor_callback
        self.max_concurrent = max_concurrent

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._pending_tasks: set[asyncio.Task[Any]] = set()

        # Create FastAPI app
        self.app = FastAPI(
            title="Scenario Orchestrator Function Trigger",
            description="HTTP endpoints for triggering scenario execution",
            version="0.1.0",
        )

        self._setup_routes()

    def _setup_routes(self) -> None:
        """Set up FastAPI routes."""

        @self.app.post("/trigger", response_model=TriggerResponse)
        async def trigger_scenario(
            request: TriggerRequest,
            background_tasks: BackgroundTasks,
        ) -> TriggerResponse:
            """Trigger execution of a single scenario."""
            return await self._handle_trigger(request, background_tasks)

        @self.app.post("/trigger/batch", response_model=BatchTriggerResponse)
        async def trigger_batch(request: BatchTriggerRequest) -> BatchTriggerResponse:
            """Trigger execution of multiple scenarios."""
            return await self._handle_batch_trigger(request)

        @self.app.post("/trigger/create", response_model=TriggerResponse)
        async def create_and_trigger(
            request: CreateAndTriggerRequest,
            background_tasks: BackgroundTasks,
        ) -> TriggerResponse:
            """Create a new scenario and immediately trigger it."""
            return await self._handle_create_and_trigger(request, background_tasks)

        @self.app.get("/stats", response_model=StatsResponse)
        async def get_stats() -> StatsResponse:
            """Get execution statistics."""
            stats = await self.client.get_stats()
            return StatsResponse(**stats)

        @self.app.get("/health")
        async def health_check() -> dict[str, str]:
            """Health check endpoint."""
            return {"status": "healthy"}

        @self.app.get("/pending")
        async def get_pending_count() -> dict[str, int]:
            """Get count of pending executions."""
            return {"pending_tasks": len(self._pending_tasks)}

    async def _handle_trigger(
        self,
        request: TriggerRequest,
        background_tasks: BackgroundTasks,
    ) -> TriggerResponse:
        """Handle a trigger request."""
        # Get the scenario
        scenario = await self.client.get_scenario(request.scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")

        # Check if scenario can be triggered
        if scenario.status not in [ScenarioStatus.PENDING, ScenarioStatus.FAILED]:
            raise HTTPException(
                status_code=400,
                detail=f"Scenario status is '{scenario.status.value}', cannot trigger",
            )

        # Update status to running
        await self.client.update_scenario_status(
            request.scenario_id, ScenarioStatus.RUNNING
        )

        if request.wait_for_result:
            # Execute synchronously and return result
            result = await self._execute_scenario(scenario)
            return TriggerResponse(
                success=result is not None and result.success,
                message="Execution completed" if result else "Execution failed",
                scenario_id=request.scenario_id,
                execution_id=result.execution_id if result else None,
                result=result,
            )
        else:
            # Execute in background
            background_tasks.add_task(self._execute_scenario_background, scenario)
            return TriggerResponse(
                success=True,
                message="Execution triggered",
                scenario_id=request.scenario_id,
            )

    async def _handle_batch_trigger(
        self, request: BatchTriggerRequest
    ) -> BatchTriggerResponse:
        """Handle a batch trigger request."""
        results: list[TriggerResponse] = []
        triggered = 0
        failed = 0

        for scenario_id in request.scenario_ids:
            try:
                scenario = await self.client.get_scenario(scenario_id)
                if not scenario:
                    results.append(
                        TriggerResponse(
                            success=False,
                            message="Scenario not found",
                            scenario_id=scenario_id,
                        )
                    )
                    failed += 1
                    continue

                if scenario.status not in [ScenarioStatus.PENDING, ScenarioStatus.FAILED]:
                    results.append(
                        TriggerResponse(
                            success=False,
                            message=f"Invalid status: {scenario.status.value}",
                            scenario_id=scenario_id,
                        )
                    )
                    failed += 1
                    continue

                # Update status and trigger in background
                await self.client.update_scenario_status(
                    scenario_id, ScenarioStatus.RUNNING
                )

                task = asyncio.create_task(self._execute_scenario(scenario))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)

                results.append(
                    TriggerResponse(
                        success=True,
                        message="Execution triggered",
                        scenario_id=scenario_id,
                    )
                )
                triggered += 1

            except Exception as e:
                logger.error(
                    "Error triggering scenario",
                    scenario_id=scenario_id,
                    error=str(e),
                )
                results.append(
                    TriggerResponse(
                        success=False,
                        message=str(e),
                        scenario_id=scenario_id,
                    )
                )
                failed += 1

        return BatchTriggerResponse(
            triggered=triggered,
            failed=failed,
            results=results,
        )

    async def _handle_create_and_trigger(
        self,
        request: CreateAndTriggerRequest,
        background_tasks: BackgroundTasks,
    ) -> TriggerResponse:
        """Handle create and trigger request."""
        # Create the scenario
        scenario = await self.client.create_scenario(request.scenario)

        # Update to running
        await self.client.update_scenario_status(
            scenario.id or "", ScenarioStatus.RUNNING
        )

        if request.wait_for_result:
            result = await self._execute_scenario(scenario)
            return TriggerResponse(
                success=result is not None and result.success,
                message="Execution completed" if result else "Execution failed",
                scenario_id=scenario.id or "",
                execution_id=result.execution_id if result else None,
                result=result,
            )
        else:
            background_tasks.add_task(self._execute_scenario_background, scenario)
            return TriggerResponse(
                success=True,
                message="Scenario created and execution triggered",
                scenario_id=scenario.id or "",
            )

    async def _execute_scenario(self, scenario: Scenario) -> ExecutionResult | None:
        """Execute a scenario with semaphore control."""
        async with self._semaphore:
            try:
                return await self.executor_callback(scenario, TriggerType.FUNCTION)
            except Exception as e:
                logger.error(
                    "Error executing scenario",
                    scenario_id=scenario.id,
                    error=str(e),
                    exc_info=True,
                )
                return None

    async def _execute_scenario_background(self, scenario: Scenario) -> None:
        """Execute a scenario in the background."""
        task = asyncio.create_task(self._execute_scenario(scenario))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    @property
    def pending_count(self) -> int:
        """Get the number of pending executions."""
        return len(self._pending_tasks)
