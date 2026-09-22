"""Fixtures for pipeline tests."""

import json
from pathlib import Path

import pytest
import pytest_asyncio

from orchestrator.config import PipelineSettings, SurrealDBSettings
from orchestrator.pipeline.schema_bootstrap import bootstrap_pipeline_schema
from orchestrator.surrealdb.client import SurrealDBClient

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "golden"


@pytest.fixture
def pipeline_settings(tmp_path: Path) -> PipelineSettings:
    return PipelineSettings(staging_dir=str(tmp_path / "staging"))


@pytest.fixture
def golden_fixture_path() -> Path:
    return FIXTURES_DIR / "entities.jsonl"


@pytest.fixture
def golden_bounds() -> list[dict[str, object]]:
    return json.loads((FIXTURES_DIR / "bounds.json").read_text(encoding="utf-8"))


@pytest.fixture
def golden_criteria() -> dict[str, float]:
    return json.loads((FIXTURES_DIR / "criteria.json").read_text(encoding="utf-8"))


@pytest_asyncio.fixture
async def pipeline_db() -> SurrealDBClient:
    settings = SurrealDBSettings(
        url="mem://",
        namespace="test",
        database="pipeline",
        username="root",
        password="root",
    )
    client = SurrealDBClient(settings)
    await client.connect()
    await bootstrap_pipeline_schema(client)
    yield client
    await client.disconnect()
