"""Load stage: batch upsert JSONL into SurrealDB (idempotent by record id)."""

import json
from pathlib import Path
from typing import Any

import structlog

from orchestrator.pipeline.models import LoadRequest, LoadResponse
from orchestrator.pipeline.run_state import upsert_scenario_run
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)

OPENAPI_FACT_FIELDS = frozenset({"id", "source", "pulled_at", "payload_hash", "payload"})
def _parse_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                records.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at line {line_no}: {exc}") from exc
    return records


def _safe_id_part(value: str) -> str:
    """Sanitize strings used in Surreal record IDs (hyphens are subtraction operators)."""
    return value.replace("-", "_").replace(":", "_")


def _record_id(record: dict[str, Any], table: str) -> str:
    raw_id = record.get("id")
    if raw_id is None:
        raise ValueError(f"Record missing id field: {record}")
    raw_id_str = str(raw_id)
    if ":" in raw_id_str:
        return raw_id_str
    return f"{table}:{raw_id_str}"


def _normalize_record(record: dict[str, Any], table: str, run_id: str) -> dict[str, Any]:
    normalized = dict(record)
    normalized["run_id"] = run_id
    if table == "openapi_fact":
        missing = OPENAPI_FACT_FIELDS - set(normalized.keys())
        if missing:
            raise ValueError(f"openapi_fact record missing fields: {sorted(missing)}")
    payload = normalized.get("payload")
    if table == "scenario_entity" and isinstance(payload, dict):
        if "value" in payload:
            normalized["value"] = payload["value"]
        if "weight" in payload:
            normalized["weight"] = payload["weight"]
    return normalized


async def run_load(request: LoadRequest, client: SurrealDBClient) -> LoadResponse:
    """Batch upsert JSONL records; resume-safe via batch_index."""
    path = Path(request.path)
    if not path.exists():
        raise FileNotFoundError(f"JSONL not found: {path}")

    records = _parse_jsonl(path)
    total = len(records)
    total_batches = max(1, (total + request.batch_size - 1) // request.batch_size) if total else 0

    if request.batch_index >= total_batches and total > 0:
        return LoadResponse(
            run_id=request.run_id,
            loaded=0,
            batch_index=request.batch_index,
            total_batches=total_batches,
            next_batch_index=None,
            records_total=total,
        )

    start = request.batch_index * request.batch_size
    end = min(start + request.batch_size, total)
    batch = records[start:end]

    loaded = 0
    for record in batch:
        record_id = _record_id(record, request.table)
        content = _normalize_record(record, request.table, request.run_id)
        content.pop("id", None)
        await client.query(
            f"UPSERT {record_id} CONTENT $content",
            {"content": content},
        )
        loaded += 1

    progress_id = f"load_progress:{_safe_id_part(request.run_id)}_{_safe_id_part(request.table)}"
    prior = await client.query(f"SELECT * FROM {progress_id}")
    prior_loaded = 0
    if isinstance(prior, list) and prior and isinstance(prior[0], dict):
        prior_loaded = int(prior[0].get("records_loaded", 0))
    await client.query(
        f"UPSERT {progress_id} MERGE $content",
        {
            "content": {
                "run_id": request.run_id,
                "table_name": request.table,
                "last_batch_index": request.batch_index,
                "records_loaded": prior_loaded + loaded,
            }
        },
    )

    next_batch = request.batch_index + 1 if end < total else None
    await upsert_scenario_run(client, request.run_id, {"status": "loading"})

    logger.info(
        "Load batch complete",
        run_id=request.run_id,
        batch_index=request.batch_index,
        loaded=loaded,
    )

    return LoadResponse(
        run_id=request.run_id,
        loaded=loaded,
        batch_index=request.batch_index,
        total_batches=total_batches,
        next_batch_index=next_batch,
        records_total=total,
    )
