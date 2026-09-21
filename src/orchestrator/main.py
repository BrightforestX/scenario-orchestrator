"""Main entry point for the Scenario Orchestrator service."""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import structlog
import uvicorn

from orchestrator.config import OrchestratorSettings, get_settings
from orchestrator.executors.baseten import BasetenExecutor, ChainletError
from orchestrator.executors.buzz import BuzzPublisher
from orchestrator.executors.daytona import DaytonaExecutor, SandboxError
from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import (
    ExecutionResult,
    ExecutionStatus,
    Scenario,
    ScenarioStatus,
    TriggerType,
)
from orchestrator.triggers.event_trigger import EventTrigger
from orchestrator.triggers.function_trigger import FunctionTrigger
from orchestrator.triggers.poll_trigger import PollTrigger

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ScenarioOrchestrator:
    """Main orchestrator that coordinates all components."""

    def __init__(self, settings: OrchestratorSettings) -> None:
        """Initialize the orchestrator.

        Args:
            settings: Orchestrator settings
        """
        self.settings = settings

        # Initialize components
        self.db_client = SurrealDBClient(settings.surrealdb)
        self.baseten_executor = BasetenExecutor(settings.baseten)
        self.daytona_executor = DaytonaExecutor(settings.daytona)
        self.buzz_publisher = BuzzPublisher(settings.buzz)

        # Initialize triggers
        self.event_trigger = EventTrigger(
            client=self.db_client,
            executor_callback=self._execute_scenario,
        )
        self.poll_trigger = PollTrigger(
            client=self.db_client,
            executor_callback=self._execute_scenario_no_return,
            interval_seconds=settings.poll_interval_seconds,
        )
        self.function_trigger = FunctionTrigger(
            client=self.db_client,
            executor_callback=self._execute_scenario,
        )

        self._running = False

    async def start(self) -> None:
        """Start the orchestrator and all triggers."""
        logger.info("Starting Scenario Orchestrator")

        # Connect to SurrealDB
        await self.db_client.connect()

        # Connect to Buzz
        try:
            await self.buzz_publisher.connect()
        except Exception as e:
            logger.warning("Could not connect to Buzz", error=str(e))

        # Start triggers
        await self.event_trigger.start()
        await self.poll_trigger.start()

        self._running = True
        logger.info("Scenario Orchestrator started")

    async def stop(self) -> None:
        """Stop the orchestrator and all triggers."""
        logger.info("Stopping Scenario Orchestrator")

        self._running = False

        # Stop triggers
        await self.event_trigger.stop()
        await self.poll_trigger.stop()

        # Disconnect from services
        await self.buzz_publisher.disconnect()
        await self.baseten_executor.close()
        await self.daytona_executor.close()
        await self.db_client.disconnect()

        logger.info("Scenario Orchestrator stopped")

    async def _execute_scenario(
        self, scenario: Scenario, trigger_type: TriggerType
    ) -> ExecutionResult | None:
        """Execute a scenario through the full pipeline.

        Args:
            scenario: The scenario to execute
            trigger_type: Type of trigger that initiated execution

        Returns:
            Execution result or None on failure
        """
        import time

        start_time = time.monotonic()
        scenario_id = scenario.id or ""

        logger.info(
            "Executing scenario",
            scenario_id=scenario_id,
            scenario_name=scenario.name,
            trigger_type=trigger_type.value,
        )

        # Create execution record
        execution = await self.db_client.create_execution(scenario_id, trigger_type)
        execution_id = execution.id or ""

        result = ExecutionResult(
            execution_id=execution_id,
            scenario_id=scenario_id,
            success=False,
        )

        try:
            # Update status to running
            await self.db_client.update_scenario_status(scenario_id, ScenarioStatus.RUNNING)

            # Step 1: Execute chainlet
            await self.db_client.update_execution(
                execution_id, status=ExecutionStatus.CHAINLET_RUNNING
            )

            try:
                chainlet_request, chainlet_result, chainlet_latency = (
                    await self.baseten_executor.execute(scenario)
                )

                await self.db_client.update_execution(
                    execution_id,
                    chainlet_request=chainlet_request,
                    chainlet_result=chainlet_result,
                    chainlet_latency_ms=chainlet_latency,
                )

                result.chainlet_result = chainlet_result
                result.latency_ms = chainlet_latency

            except ChainletError as e:
                logger.error("Chainlet execution failed", error=str(e))
                result.error = f"Chainlet error: {str(e)}"
                await self._fail_execution(scenario_id, execution_id, result.error)
                return result

            # Step 2: Execute sandbox (if configured)
            if scenario.sandbox_config and scenario.sandbox_config.enabled:
                await self.db_client.update_execution(
                    execution_id, status=ExecutionStatus.SANDBOX_RUNNING
                )

                try:
                    sandbox_id, sandbox_output = await self.daytona_executor.execute(scenario)

                    await self.db_client.update_execution(
                        execution_id,
                        sandbox_id=sandbox_id,
                        sandbox_output=sandbox_output,
                    )

                    result.sandbox_output = sandbox_output

                except SandboxError as e:
                    logger.error("Sandbox execution failed", error=str(e))
                    result.error = f"Sandbox error: {str(e)}"
                    await self._fail_execution(scenario_id, execution_id, result.error)
                    return result

            # Step 3: Publish to Buzz (if configured)
            if scenario.buzz_config and scenario.buzz_config.enabled:
                await self.db_client.update_execution(
                    execution_id, status=ExecutionStatus.PUBLISHING
                )

                result.success = True  # Set success before publishing
                buzz_event_id = await self.buzz_publisher.publish(scenario, result)

                if buzz_event_id:
                    await self.db_client.update_execution(
                        execution_id, buzz_event_id=buzz_event_id
                    )
                    result.buzz_event_id = buzz_event_id

            # Mark as completed
            result.success = True
            total_latency = int((time.monotonic() - start_time) * 1000)
            result.latency_ms = total_latency

            await self.db_client.complete_scenario(scenario_id, execution_id)

            logger.info(
                "Scenario execution completed",
                scenario_id=scenario_id,
                execution_id=execution_id,
                latency_ms=total_latency,
            )

            return result

        except Exception as e:
            logger.error(
                "Unexpected error executing scenario",
                scenario_id=scenario_id,
                error=str(e),
                exc_info=True,
            )
            result.error = str(e)
            await self._fail_execution(scenario_id, execution_id, result.error)
            return result

    async def _execute_scenario_no_return(
        self, scenario: Scenario, trigger_type: TriggerType
    ) -> None:
        """Execute a scenario without returning result (for poll trigger)."""
        await self._execute_scenario(scenario, trigger_type)

    async def _fail_execution(
        self, scenario_id: str, execution_id: str, error: str
    ) -> None:
        """Mark a scenario execution as failed."""
        will_retry = await self.db_client.fail_scenario(scenario_id, execution_id, error)
        if will_retry:
            logger.info("Scenario will be retried", scenario_id=scenario_id)


@asynccontextmanager
async def lifespan(app: Any) -> AsyncGenerator[None, None]:
    """FastAPI lifespan context manager."""
    # Startup
    settings = get_settings()
    orchestrator = ScenarioOrchestrator(settings)
    await orchestrator.start()

    # Store orchestrator in app state
    app.state.orchestrator = orchestrator

    yield

    # Shutdown
    await orchestrator.stop()


def create_app() -> Any:
    """Create the FastAPI application."""
    settings = get_settings()
    orchestrator = ScenarioOrchestrator(settings)

    # Use the function trigger's app with lifespan
    app = orchestrator.function_trigger.app

    @app.on_event("startup")
    async def startup() -> None:
        await orchestrator.start()
        app.state.orchestrator = orchestrator

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await orchestrator.stop()

    return app


async def run_orchestrator() -> None:
    """Run the orchestrator service."""
    settings = get_settings()
    orchestrator = ScenarioOrchestrator(settings)

    # Handle signals for graceful shutdown
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def signal_handler() -> None:
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await orchestrator.start()

        # Start the HTTP server for function trigger
        config = uvicorn.Config(
            orchestrator.function_trigger.app,
            host="0.0.0.0",
            port=settings.function_trigger_port,
            log_level=settings.log_level.lower(),
        )
        server = uvicorn.Server(config)

        # Run server and wait for stop signal
        server_task = asyncio.create_task(server.serve())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [server_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel remaining tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    finally:
        await orchestrator.stop()


def main() -> None:
    """Main entry point."""
    try:
        asyncio.run(run_orchestrator())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
