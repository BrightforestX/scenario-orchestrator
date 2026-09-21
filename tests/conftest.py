"""Pytest configuration and fixtures for integration tests."""

import asyncio
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio

from orchestrator.config import (
    BasetenSettings,
    BuzzSettings,
    DaytonaSettings,
    OrchestratorSettings,
    SurrealDBSettings,
)
from orchestrator.executors.baseten import BasetenExecutor
from orchestrator.executors.buzz import BuzzPublisher
from orchestrator.executors.daytona import DaytonaExecutor
from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import ChainletConfig, Scenario, ScenarioCreate


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def surrealdb_settings() -> SurrealDBSettings:
    """Create SurrealDB settings for testing."""
    return SurrealDBSettings(
        url="mem://",  # Use in-memory database for tests
        namespace="test",
        database="scenarios",
        username="root",
        password="root",
    )


@pytest.fixture
def baseten_settings() -> BasetenSettings:
    """Create Baseten settings for testing."""
    return BasetenSettings(
        api_key="test_api_key",
        chain_endpoint="http://localhost:8000/predict",
        timeout_seconds=30,
    )


@pytest.fixture
def daytona_settings() -> DaytonaSettings:
    """Create Daytona settings for testing."""
    return DaytonaSettings(
        api_key="test_api_key",
        target_region="us",
        auto_stop_minutes=5,
    )


@pytest.fixture
def buzz_settings() -> BuzzSettings:
    """Create Buzz settings for testing."""
    return BuzzSettings(
        relay_url="ws://localhost:3000",
        private_key="0" * 64,  # Dummy key for tests
        default_channel="test-channel",
    )


@pytest.fixture
def orchestrator_settings(
    surrealdb_settings: SurrealDBSettings,
    baseten_settings: BasetenSettings,
    daytona_settings: DaytonaSettings,
    buzz_settings: BuzzSettings,
) -> OrchestratorSettings:
    """Create full orchestrator settings for testing."""
    return OrchestratorSettings(
        poll_interval_seconds=5,
        function_trigger_port=8080,
        log_level="DEBUG",
        surrealdb=surrealdb_settings,
        baseten=baseten_settings,
        daytona=daytona_settings,
        buzz=buzz_settings,
    )


@pytest_asyncio.fixture
async def db_client(
    surrealdb_settings: SurrealDBSettings,
) -> AsyncGenerator[SurrealDBClient, None]:
    """Create and connect a SurrealDB client for testing."""
    client = SurrealDBClient(surrealdb_settings)
    await client.connect()
    yield client
    await client.disconnect()


@pytest_asyncio.fixture
async def baseten_executor(
    baseten_settings: BasetenSettings,
) -> AsyncGenerator[BasetenExecutor, None]:
    """Create a Baseten executor for testing."""
    executor = BasetenExecutor(baseten_settings)
    yield executor
    await executor.close()


@pytest_asyncio.fixture
async def daytona_executor(
    daytona_settings: DaytonaSettings,
) -> AsyncGenerator[DaytonaExecutor, None]:
    """Create a Daytona executor for testing."""
    executor = DaytonaExecutor(daytona_settings)
    yield executor
    await executor.close()


@pytest_asyncio.fixture
async def buzz_publisher(
    buzz_settings: BuzzSettings,
) -> AsyncGenerator[BuzzPublisher, None]:
    """Create a Buzz publisher for testing."""
    publisher = BuzzPublisher(buzz_settings)
    yield publisher
    await publisher.disconnect()


@pytest.fixture
def sample_chainlet_config() -> ChainletConfig:
    """Create a sample chainlet configuration."""
    return ChainletConfig(
        model="meta-llama/Llama-3.1-8B-Instruct",
        prompt_template="Answer the following: {question}",
        parameters={"max_tokens": 100, "temperature": 0.7},
        timeout_seconds=30,
    )


@pytest.fixture
def sample_scenario_create(sample_chainlet_config: ChainletConfig) -> ScenarioCreate:
    """Create a sample scenario for testing."""
    return ScenarioCreate(
        name="Test Scenario",
        description="A test scenario for integration testing",
        interests=[
            {"name": "question", "type": "prompt", "value": "What is 2 + 2?"},
        ],
        chainlet_config=sample_chainlet_config,
        priority=1,
    )


@pytest.fixture
def sample_scenario(sample_chainlet_config: ChainletConfig) -> Scenario:
    """Create a sample Scenario object for testing."""
    return Scenario(
        id="scenario:test123",
        name="Test Scenario",
        description="A test scenario",
        interests=[
            {"name": "question", "type": "prompt", "value": "What is 2 + 2?"},
        ],
        chainlet_config=sample_chainlet_config,
        priority=1,
    )
