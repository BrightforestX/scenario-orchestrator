"""LIVE query listener for SurrealDB scenarios."""

import asyncio
from typing import Any, Callable, Coroutine

import structlog

from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import Scenario

logger = structlog.get_logger(__name__)


class LiveScenarioListener:
    """Listens for live updates on scenarios from SurrealDB."""

    def __init__(
        self,
        client: SurrealDBClient,
        on_create: Callable[[Scenario], Coroutine[Any, Any, None]] | None = None,
        on_update: Callable[[Scenario], Coroutine[Any, Any, None]] | None = None,
        on_delete: Callable[[str], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        """Initialize the listener.

        Args:
            client: SurrealDB client instance
            on_create: Callback for new scenarios
            on_update: Callback for updated scenarios
            on_delete: Callback for deleted scenarios
        """
        self.client = client
        self.on_create = on_create
        self.on_update = on_update
        self.on_delete = on_delete
        self._live_id: str | None = None
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start listening for scenario changes."""
        if self._running:
            logger.warning("Live listener already running")
            return

        self._running = True

        def callback(action: str, result: dict[str, Any]) -> None:
            """Handle live query notifications."""
            # Schedule the async callback in the event loop
            asyncio.create_task(self._handle_notification(action, result))

        self._live_id = await self.client.live_scenarios(callback)
        logger.info("Live scenario listener started", live_id=self._live_id)

    async def _handle_notification(self, action: str, result: dict[str, Any]) -> None:
        """Process a live query notification."""
        try:
            if action == "CREATE" and self.on_create:
                scenario = Scenario.from_surreal(result)
                logger.debug("Received CREATE notification", scenario_id=scenario.id)
                await self.on_create(scenario)

            elif action == "UPDATE" and self.on_update:
                scenario = Scenario.from_surreal(result)
                logger.debug(
                    "Received UPDATE notification",
                    scenario_id=scenario.id,
                    status=scenario.status.value,
                )
                await self.on_update(scenario)

            elif action == "DELETE" and self.on_delete:
                scenario_id = str(result.get("id", ""))
                logger.debug("Received DELETE notification", scenario_id=scenario_id)
                await self.on_delete(scenario_id)

        except Exception as e:
            logger.error(
                "Error handling live notification",
                action=action,
                error=str(e),
                exc_info=True,
            )

    async def stop(self) -> None:
        """Stop listening for scenario changes."""
        if not self._running:
            return

        self._running = False

        if self._live_id:
            try:
                await self.client.kill_live_query(self._live_id)
            except Exception as e:
                logger.warning("Error killing live query", error=str(e))
            self._live_id = None

        logger.info("Live scenario listener stopped")

    @property
    def is_running(self) -> bool:
        """Check if the listener is currently running."""
        return self._running
