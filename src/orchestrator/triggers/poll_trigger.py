"""Polling trigger for scenarios."""

import asyncio
from typing import Any, Callable, Coroutine

import structlog

from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import Scenario, ScenarioStatus, TriggerType

logger = structlog.get_logger(__name__)


# Type alias for the executor callback
ExecutorCallback = Callable[[Scenario, TriggerType], Coroutine[Any, Any, None]]


class PollTrigger:
    """Trigger scenarios by polling SurrealDB at regular intervals.

    This trigger periodically queries for pending scenarios and triggers
    their execution. It serves as a fallback when LIVE queries are not
    available or for batch processing scenarios.
    """

    def __init__(
        self,
        client: SurrealDBClient,
        executor_callback: ExecutorCallback,
        interval_seconds: int = 30,
        batch_size: int = 10,
        max_concurrent: int = 5,
        check_dependencies: bool = True,
    ) -> None:
        """Initialize the poll trigger.

        Args:
            client: SurrealDB client instance
            executor_callback: Async function to call when a scenario should be executed
            interval_seconds: Polling interval in seconds
            batch_size: Maximum scenarios to fetch per poll
            max_concurrent: Maximum concurrent executions
            check_dependencies: Whether to check scenario dependencies before execution
        """
        self.client = client
        self.executor_callback = executor_callback
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.check_dependencies = check_dependencies

        self._running = False
        self._poll_task: asyncio.Task[None] | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._pending_tasks: set[asyncio.Task[None]] = set()
        self._processed_ids: set[str] = set()  # Track recently processed to avoid duplicates

    async def start(self) -> None:
        """Start the polling trigger."""
        if self._running:
            logger.warning("Poll trigger already running")
            return

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "Poll trigger started",
            interval_seconds=self.interval_seconds,
            batch_size=self.batch_size,
            max_concurrent=self.max_concurrent,
        )

    async def stop(self) -> None:
        """Stop the polling trigger."""
        if not self._running:
            return

        self._running = False

        # Cancel the poll loop
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        # Wait for pending executions
        if self._pending_tasks:
            logger.info("Waiting for pending executions", count=len(self._pending_tasks))
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()

        self._processed_ids.clear()
        logger.info("Poll trigger stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in poll loop", error=str(e), exc_info=True)

            # Wait for next poll interval
            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break

    async def _poll_once(self) -> None:
        """Perform a single poll for pending scenarios."""
        # Get scenarios based on dependency checking
        if self.check_dependencies:
            scenarios = await self.client.get_ready_scenarios(limit=self.batch_size)
        else:
            scenarios = await self.client.get_pending_scenarios(limit=self.batch_size)

        if not scenarios:
            logger.debug("No pending scenarios found")
            return

        # Filter out recently processed scenarios
        new_scenarios = [
            s for s in scenarios
            if s.id and s.id not in self._processed_ids
        ]

        if not new_scenarios:
            logger.debug("All found scenarios recently processed")
            return

        logger.info(
            "Found pending scenarios",
            count=len(new_scenarios),
            total_found=len(scenarios),
        )

        # Trigger execution for each scenario
        for scenario in new_scenarios:
            if scenario.id:
                self._processed_ids.add(scenario.id)
            await self._trigger_execution(scenario)

        # Clean up old processed IDs (keep last 1000)
        if len(self._processed_ids) > 1000:
            # Convert to list, keep last 500
            ids_list = list(self._processed_ids)
            self._processed_ids = set(ids_list[-500:])

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
                # Update status to running before execution
                await self.client.update_scenario_status(
                    scenario.id or "", ScenarioStatus.RUNNING
                )
                await self.executor_callback(scenario, TriggerType.POLL)
            except Exception as e:
                logger.error(
                    "Error executing scenario from poll trigger",
                    scenario_id=scenario.id,
                    error=str(e),
                    exc_info=True,
                )
                # Remove from processed so it can be retried
                if scenario.id:
                    self._processed_ids.discard(scenario.id)

    async def poll_now(self) -> int:
        """Trigger an immediate poll. Returns number of scenarios triggered."""
        if not self._running:
            logger.warning("Poll trigger not running, cannot poll now")
            return 0

        initial_pending = len(self._pending_tasks)
        await self._poll_once()
        return len(self._pending_tasks) - initial_pending

    @property
    def is_running(self) -> bool:
        """Check if the trigger is running."""
        return self._running

    @property
    def pending_count(self) -> int:
        """Get the number of pending executions."""
        return len(self._pending_tasks)
