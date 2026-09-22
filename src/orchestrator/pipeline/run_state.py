"""Scenario run state helpers (Python-side, avoids complex SurrealQL spreads)."""

from typing import Any

from orchestrator.surrealdb.client import SurrealDBClient


async def upsert_scenario_run(client: SurrealDBClient, run_id: str, patch: dict[str, Any]) -> None:
    """Create or update scenario_run record."""
    existing = await client.query(
        "SELECT * FROM scenario_run WHERE run_id = $run_id LIMIT 1",
        {"run_id": run_id},
    )
    rows = existing if isinstance(existing, list) else []
    if rows:
        await client.query(
            "UPDATE scenario_run MERGE $patch WHERE run_id = $run_id",
            {"run_id": run_id, "patch": patch},
        )
        await client.query(
            "UPDATE scenario_run SET updated_at = time::now() WHERE run_id = $run_id",
            {"run_id": run_id},
        )
    else:
        await client.query(
            "CREATE scenario_run CONTENT $content",
            {"content": {"run_id": run_id, **patch}},
        )
