"""Bootstrap SurrealDB pipeline schema for tests and local dev."""

from pathlib import Path

from orchestrator.surrealdb.client import SurrealDBClient

_SCHEMA_DIR = Path(__file__).resolve().parents[3] / "surreal"


async def bootstrap_pipeline_schema(client: SurrealDBClient) -> None:
    """Apply pipeline schema and functions idempotently."""
    for filename in ("pipeline_schema.surql", "pipeline_functions.surql"):
        sql = (_SCHEMA_DIR / filename).read_text(encoding="utf-8")
        await client.query(sql)
