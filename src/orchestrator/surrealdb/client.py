"""SurrealDB client with connection management."""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable

import structlog
from surrealdb import AsyncSurrealDB

from orchestrator.config import SurrealDBSettings
from orchestrator.surrealdb.schema import (
    Execution,
    ExecutionStatus,
    Scenario,
    ScenarioCreate,
    ScenarioStatus,
    TriggerType,
)

logger = structlog.get_logger(__name__)


class SurrealDBClient:
    """Async SurrealDB client with connection management."""

    def __init__(self, settings: SurrealDBSettings) -> None:
        """Initialize the client with settings."""
        self.settings = settings
        self._db: AsyncSurrealDB | None = None
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to SurrealDB."""
        async with self._lock:
            if self._connected:
                return

            self._db = AsyncSurrealDB(self.settings.url)
            await self._db.connect()

            # Sign in
            await self._db.signin({
                "username": self.settings.username,
                "password": self.settings.password,
            })

            # Use namespace and database
            await self._db.use(self.settings.namespace, self.settings.database)

            self._connected = True
            logger.info(
                "Connected to SurrealDB",
                url=self.settings.url,
                namespace=self.settings.namespace,
                database=self.settings.database,
            )

    async def disconnect(self) -> None:
        """Disconnect from SurrealDB."""
        async with self._lock:
            if self._db and self._connected:
                await self._db.close()
                self._connected = False
                logger.info("Disconnected from SurrealDB")

    @asynccontextmanager
    async def session(self) -> AsyncGenerator["SurrealDBClient", None]:
        """Context manager for database sessions."""
        await self.connect()
        try:
            yield self
        finally:
            pass  # Keep connection open for reuse

    async def _ensure_connected(self) -> AsyncSurrealDB:
        """Ensure we have a connected database instance."""
        if not self._connected or self._db is None:
            await self.connect()
        assert self._db is not None
        return self._db

    # -------------------------------------------------------------------------
    # Scenario Operations
    # -------------------------------------------------------------------------

    async def create_scenario(self, scenario: ScenarioCreate) -> Scenario:
        """Create a new scenario."""
        db = await self._ensure_connected()

        data = scenario.model_dump(exclude_none=True)
        # Convert nested models to dicts
        if "chainlet_config" in data:
            data["chainlet_config"] = (
                data["chainlet_config"]
                if isinstance(data["chainlet_config"], dict)
                else data["chainlet_config"]
            )

        result = await db.create("scenario", data)
        logger.info("Created scenario", scenario_id=result.get("id"))
        return Scenario.from_surreal(result)

    async def get_scenario(self, scenario_id: str) -> Scenario | None:
        """Get a scenario by ID."""
        db = await self._ensure_connected()

        # Ensure proper record ID format
        if not scenario_id.startswith("scenario:"):
            scenario_id = f"scenario:{scenario_id}"

        result = await db.select(scenario_id)
        if not result:
            return None
        return Scenario.from_surreal(result)

    async def update_scenario_status(
        self, scenario_id: str, status: ScenarioStatus
    ) -> Scenario | None:
        """Update a scenario's status."""
        db = await self._ensure_connected()

        if not scenario_id.startswith("scenario:"):
            scenario_id = f"scenario:{scenario_id}"

        result = await db.merge(scenario_id, {"status": status.value})
        if not result:
            return None

        logger.info("Updated scenario status", scenario_id=scenario_id, status=status.value)
        return Scenario.from_surreal(result)

    async def get_pending_scenarios(self, limit: int = 10) -> list[Scenario]:
        """Get pending scenarios ordered by priority."""
        db = await self._ensure_connected()

        query = """
            SELECT * FROM scenario 
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT $limit
        """
        result = await db.query(query, {"limit": limit})

        scenarios = []
        if result and len(result) > 0 and result[0].get("result"):
            for item in result[0]["result"]:
                scenarios.append(Scenario.from_surreal(item))

        return scenarios

    async def get_ready_scenarios(self, limit: int = 10) -> list[Scenario]:
        """Get scenarios that are ready to execute (pending with met dependencies)."""
        db = await self._ensure_connected()

        # Call the custom function
        result = await db.query("RETURN fn::get_ready_scenarios($limit)", {"limit": limit})

        scenarios = []
        if result and len(result) > 0 and result[0].get("result"):
            for item in result[0]["result"]:
                scenarios.append(Scenario.from_surreal(item))

        return scenarios

    # -------------------------------------------------------------------------
    # Execution Operations
    # -------------------------------------------------------------------------

    async def create_execution(
        self, scenario_id: str, trigger_type: TriggerType
    ) -> Execution:
        """Create a new execution record."""
        db = await self._ensure_connected()

        if not scenario_id.startswith("scenario:"):
            scenario_id = f"scenario:{scenario_id}"

        data = {
            "scenario": scenario_id,
            "trigger_type": trigger_type.value,
            "status": ExecutionStatus.STARTED.value,
        }

        result = await db.create("execution", data)
        logger.info(
            "Created execution",
            execution_id=result.get("id"),
            scenario_id=scenario_id,
            trigger_type=trigger_type.value,
        )
        return Execution.from_surreal(result)

    async def update_execution(
        self,
        execution_id: str,
        status: ExecutionStatus | None = None,
        chainlet_request: dict[str, Any] | None = None,
        chainlet_result: dict[str, Any] | None = None,
        chainlet_latency_ms: int | None = None,
        sandbox_id: str | None = None,
        sandbox_output: str | None = None,
        buzz_event_id: str | None = None,
        error: str | None = None,
    ) -> Execution | None:
        """Update an execution record."""
        db = await self._ensure_connected()

        if not execution_id.startswith("execution:"):
            execution_id = f"execution:{execution_id}"

        updates: dict[str, Any] = {}
        if status is not None:
            updates["status"] = status.value
        if chainlet_request is not None:
            updates["chainlet_request"] = chainlet_request
        if chainlet_result is not None:
            updates["chainlet_result"] = chainlet_result
        if chainlet_latency_ms is not None:
            updates["chainlet_latency_ms"] = chainlet_latency_ms
        if sandbox_id is not None:
            updates["sandbox_id"] = sandbox_id
        if sandbox_output is not None:
            updates["sandbox_output"] = sandbox_output
        if buzz_event_id is not None:
            updates["buzz_event_id"] = buzz_event_id
        if error is not None:
            updates["error"] = error

        if status == ExecutionStatus.COMPLETED or status == ExecutionStatus.FAILED:
            updates["completed_at"] = "time::now()"

        result = await db.merge(execution_id, updates)
        if not result:
            return None

        return Execution.from_surreal(result)

    async def complete_scenario(
        self, scenario_id: str, execution_id: str
    ) -> bool:
        """Mark a scenario as completed using the custom function."""
        db = await self._ensure_connected()

        if not scenario_id.startswith("scenario:"):
            scenario_id = f"scenario:{scenario_id}"
        if not execution_id.startswith("execution:"):
            execution_id = f"execution:{execution_id}"

        result = await db.query(
            "RETURN fn::complete_scenario($scenario_id, $execution_id)",
            {"scenario_id": scenario_id, "execution_id": execution_id},
        )

        logger.info("Completed scenario", scenario_id=scenario_id, execution_id=execution_id)
        return bool(result)

    async def fail_scenario(
        self, scenario_id: str, execution_id: str, error: str
    ) -> bool:
        """Mark a scenario as failed using the custom function. Returns True if will retry."""
        db = await self._ensure_connected()

        if not scenario_id.startswith("scenario:"):
            scenario_id = f"scenario:{scenario_id}"
        if not execution_id.startswith("execution:"):
            execution_id = f"execution:{execution_id}"

        result = await db.query(
            "RETURN fn::fail_scenario($scenario_id, $execution_id, $error)",
            {"scenario_id": scenario_id, "execution_id": execution_id, "error": error},
        )

        will_retry = bool(result and result[0].get("result"))
        logger.info(
            "Failed scenario",
            scenario_id=scenario_id,
            execution_id=execution_id,
            error=error,
            will_retry=will_retry,
        )
        return will_retry

    # -------------------------------------------------------------------------
    # LIVE Query Support
    # -------------------------------------------------------------------------

    async def live_scenarios(
        self, callback: Callable[[str, dict[str, Any]], None]
    ) -> str:
        """Subscribe to live updates on scenarios. Returns the live query ID."""
        db = await self._ensure_connected()

        async def handle_notification(notification: dict[str, Any]) -> None:
            action = notification.get("action", "")
            result = notification.get("result", {})
            callback(action, result)

        live_id = await db.live("scenario", callback=handle_notification)
        logger.info("Started live query on scenarios", live_id=live_id)
        return live_id

    async def kill_live_query(self, live_id: str) -> None:
        """Kill a live query subscription."""
        db = await self._ensure_connected()
        await db.kill(live_id)
        logger.info("Killed live query", live_id=live_id)

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    async def get_stats(self) -> dict[str, Any]:
        """Get execution statistics."""
        db = await self._ensure_connected()

        result = await db.query("RETURN fn::get_execution_stats()")
        if result and len(result) > 0:
            return result[0].get("result", {})
        return {}

    # -------------------------------------------------------------------------
    # Raw Query
    # -------------------------------------------------------------------------

    async def query(self, query: str, params: dict[str, Any] | None = None) -> list[Any]:
        """Execute a raw SurrealQL query."""
        db = await self._ensure_connected()
        return await db.query(query, params or {})
