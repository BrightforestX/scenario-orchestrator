"""Score stage: invoke SurrealQL fn::score_v1 — no keep/revert decisions."""

from typing import Any

import structlog

from orchestrator.pipeline.models import ScenarioScore, ScoreRequest, ScoreResponse
from orchestrator.pipeline.run_state import upsert_scenario_run
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)


def _coerce_scores(raw: Any) -> list[ScenarioScore]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [ScenarioScore.model_validate(raw)]
    if isinstance(raw, list):
        return [ScenarioScore.model_validate(item) for item in raw if isinstance(item, dict)]
    return []


async def run_score(request: ScoreRequest, client: SurrealDBClient) -> ScoreResponse:
    """Call fn::score_one_v1 per draw in SurrealDB; optionally persist shortlist."""
    scores: list[ScenarioScore] = []
    for draw in request.draws:
        result = await client.query(
            "RETURN fn::score_one_v1($run_id, $draw)",
            {"run_id": request.run_id, "draw": draw.model_dump()},
        )
        scores.extend(_coerce_scores(result))

    if request.persist_shortlist:
        for score in scores:
            await client.query(
                "CREATE scenario_shortlist CONTENT $content",
                {
                    "content": {
                        "run_id": request.run_id,
                        "scenario_id": score.scenario_id,
                        "draw_index": score.draw_index,
                        "score": score.score,
                        "metrics": score.metrics,
                    }
                },
            )

    await upsert_scenario_run(
        client,
        request.run_id,
        {"status": "scoring", "checksum": request.checksum},
    )

    logger.info("Scoring complete", run_id=request.run_id, score_count=len(scores))
    return ScoreResponse(run_id=request.run_id, scores=scores, checksum=request.checksum)
