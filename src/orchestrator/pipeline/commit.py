"""Commit and compensate stages for pipeline winners."""

import structlog

from orchestrator.pipeline.models import (
    CommitRequest,
    CommitResponse,
    CompensateRequest,
    CompensateResponse,
)
from orchestrator.pipeline.run_state import upsert_scenario_run
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)


async def run_commit(request: CommitRequest, client: SurrealDBClient) -> CommitResponse:
    """Persist winners tagged with run_id."""
    committed = 0
    for winner in request.winners:
        await client.query(
            "CREATE scenario_winner CONTENT $content",
            {
                "content": {
                    "run_id": request.run_id,
                    "scenario_id": winner.scenario_id,
                    "draw_index": winner.draw_index,
                    "score": winner.score,
                    "metrics": winner.metrics,
                    "committed": True,
                }
            },
        )
        committed += 1

    await upsert_scenario_run(client, request.run_id, {"status": "committed"})
    logger.info("Winners committed", run_id=request.run_id, committed=committed)
    return CommitResponse(run_id=request.run_id, committed=committed)


async def run_compensate(request: CompensateRequest, client: SurrealDBClient) -> CompensateResponse:
    """Delete staged (uncommitted) winners for run_id."""
    before = await client.query(
        """
        SELECT count() AS count FROM scenario_winner
        WHERE run_id = $run_id AND committed = false
        GROUP ALL
        """,
        {"run_id": request.run_id},
    )
    deleted = 0
    if isinstance(before, list) and before and isinstance(before[0], dict):
        deleted = int(before[0].get("count", 0))

    await client.query("RETURN fn::compensate_run($run_id)", {"run_id": request.run_id})
    logger.info("Compensated run", run_id=request.run_id, deleted=deleted)
    return CompensateResponse(run_id=request.run_id, deleted=deleted)
