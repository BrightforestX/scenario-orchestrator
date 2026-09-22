"""Draw stage: seeded NumPy parameter draws from typed bounds."""

import numpy as np
import structlog

from orchestrator.pipeline.models import DrawRequest, DrawResponse, ScenarioDraw
from orchestrator.pipeline.run_state import upsert_scenario_run
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)


def generate_draws(request: DrawRequest) -> DrawResponse:
    """Generate seeded parameter draws using NumPy."""
    rng = np.random.default_rng(request.seed)
    draws: list[ScenarioDraw] = []

    for index in range(request.num_samples):
        spec = request.specs[index] if request.specs and index < len(request.specs) else None
        params: dict[str, float] = {}
        for bound in request.bounds:
            if bound.dtype == "int":
                value = int(rng.integers(int(bound.min), int(bound.max) + 1))
                params[bound.name] = float(value)
            else:
                params[bound.name] = float(rng.uniform(bound.min, bound.max))

        default_id = f"{request.run_id}_draw_{index}"
        scenario_id = spec.scenario_id if spec and spec.scenario_id else default_id
        draw_index = spec.draw_index if spec and spec.draw_index is not None else index
        draws.append(
            ScenarioDraw(
                scenario_id=scenario_id,
                draw_index=draw_index,
                params=params,
                seed=request.seed,
            )
        )

    return DrawResponse(run_id=request.run_id, seed=request.seed, draws=draws)


async def persist_draws(request: DrawRequest, client: SurrealDBClient) -> DrawResponse:
    """Generate draws and persist to SurrealDB."""
    response = generate_draws(request)
    for draw in response.draws:
        await client.query(
            "CREATE scenario_draw CONTENT $content",
            {
                "content": {
                    "run_id": request.run_id,
                    "draw_index": draw.draw_index,
                    "scenario_id": draw.scenario_id,
                    "params": draw.params,
                    "seed": draw.seed,
                }
            },
        )
    await upsert_scenario_run(
        client, request.run_id, {"status": "drawing", "seed": request.seed}
    )
    logger.info("Draws persisted", run_id=request.run_id, count=len(response.draws))
    return response
