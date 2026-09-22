"""Extract stage: Trino SQL or fixture path → JSONL."""

import json
import shutil
from pathlib import Path
from typing import Any

import structlog

from orchestrator.config import PipelineSettings
from orchestrator.pipeline.checksum import sha256_file
from orchestrator.pipeline.models import ExtractRequest, ExtractResponse
from orchestrator.pipeline.run_state import upsert_scenario_run
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)


async def run_extract(
    request: ExtractRequest,
    settings: PipelineSettings,
    client: SurrealDBClient | None = None,
) -> ExtractResponse:
    """Run extract: copy fixture or execute Trino SQL, write JSONL, return checksum."""
    staging = Path(request.staging_dir or settings.staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    out_path = staging / f"{request.run_id}.jsonl"

    record_count = 0

    if request.fixture_path:
        src = Path(request.fixture_path)
        if not src.exists():
            raise FileNotFoundError(f"Fixture not found: {src}")
        shutil.copy2(src, out_path)
        record_count = sum(1 for _ in out_path.open(encoding="utf-8") if _.strip())
    elif request.sql:
        record_count = await _extract_from_trino(request.sql, out_path, settings)
    else:
        raise ValueError("Either fixture_path or sql must be provided")

    checksum = sha256_file(out_path)

    if client is not None:
        await upsert_scenario_run(
            client,
            request.run_id,
            {
                "status": "extracting",
                "checksum": checksum,
                "jsonl_path": str(out_path),
            },
        )

    logger.info(
        "Extract complete",
        run_id=request.run_id,
        path=str(out_path),
        checksum=checksum,
        record_count=record_count,
    )
    return ExtractResponse(
        path=str(out_path),
        checksum=checksum,
        run_id=request.run_id,
        record_count=record_count,
    )


async def _extract_from_trino(sql: str, out_path: Path, settings: PipelineSettings) -> int:
    """Execute Trino query and write rows as JSONL."""
    try:
        import trino  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "Trino client not installed. Install with: pip install scenario-orchestrator[trino]"
        ) from exc

    conn = trino.dbapi.connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description or []]
    count = 0
    with out_path.open("w", encoding="utf-8") as handle:
        for row in cursor.fetchall():
            record: dict[str, Any] = dict(zip(columns, row, strict=True))
            handle.write(json.dumps(record, default=str) + "\n")
            count += 1
    return count
