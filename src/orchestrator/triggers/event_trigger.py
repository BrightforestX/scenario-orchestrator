"""Event-sourcing trigger using SurrealDB LIVE queries."""

import asyncio
from typing import Any, Callable, Coroutine

import structlog

from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.live_listener import LiveScenarioListener
from orchestrator.surrealdb.schema import Scenario, ScenarioStatus, TriggerType

logger = structlog.get_logger(__name__)


# Type alias for the executor callback
ExecutorCallback = Callable[[Scenario, TriggerType], Coroutine[Any, Any, None]]


class EventTrigger:
    """Trigger scenarios based on SurrealDB LIVE query events.

    This trigger listens for changes to scenarios in SurrealDB and
    automatically triggers execution when:
    - A new scenario is created with status 'pending'
    - An existing scenario is updated to status 'pending'
    """

    def __init__(
        self,
        client: SurrealDBClient,
        executor_callback: ExecutorCallback,
        max_concurrent: int = 5,
    ) -> None:
        """Initialize the event trigger.

        Args:
            client: SurrealDB client instance
            executor_callback: Async function to call when a scenario should be executed
            max_concurrent: Maximum number of concurrent executions
        """
        self.client = client
        self.executor_callback = executor_callback
        self.max_concurrent = max_concurrent
        self._listener: LiveScenarioListener | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._pending_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Start the event trigger."""
        if self._running:
            logger.warning("Event trigger already running")
            return

        self._running = True

        self._listener = LiveScenarioListener(
            client=self.client,
            on_create=self._on_scenario_created,
            on_update=self._on_scenario_updated,
        )

        await self._listener.start()
        logger.info("Event trigger started", max_concurrent=self.max_concurrent)

    async def stop(self) -> None:
        """Stop the event trigger."""
        if not self._running:
            return

        self._running = False

        # Stop the listener
        if self._listener:
            await self._listener.stop()
            self._listener = None

        # Wait for pending tasks to complete
        if self._pending_tasks:
            logger.info("Waiting for pending executions to complete", count=len(self._pending_tasks))
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()

        logger.info("Event trigger stopped")

    async def _on_scenario_created(self, scenario: Scenario) -> None:
        """Handle a newly created scenario."""
        if scenario.status == ScenarioStatus.PENDING:
            logger.info(
                "New pending scenario detected",
                scenario_id=scenario.id,
                name=scenario.name,
            )
            await self._trigger_execution(scenario)

    async def _on_scenario_updated(self, scenario: Scenario) -> None:
        """Handle an updated scenario."""
        # Only trigger if status changed to pending (e.g., retry)
        if scenario.status == ScenarioStatus.PENDING:
            logger.info(
                "Scenario updated to pending",
                scenario_id=scenario.id,
                name=scenario.name,
                retry_count=scenario.retry_count,
            )
            await self._trigger_execution(scenario)

    async def _trigger_execution(self, scenario: Scenario) -> None:
        """Trigger execution of a scenario with concurrency control."""
        if not self._running:
            return

        task = asyncio.create_task(self._execute_with_semaphore(scenario))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _execute_with_semaphore(self, scenario: Scenario) -> None:
        """Execute a scenario with semaphore for concurrency control."""
        async with self._semaphore:
            try:
                await self.executor_callback(scenario, TriggerType.EVENT)
            except Exception as e:
                logger.error(
                    "Error executing scenario from event trigger",
                    scenario_id=scenario.id,
                    error=str(e),
                    exc_info=True,
                )

    @property
    def is_running(self) -> bool:
        """Check if the trigger is running."""
        return self._running

    @property
    def pending_count(self) -> int:
        """Get the number of pending executions."""
        return len(self._pending_tasks)
