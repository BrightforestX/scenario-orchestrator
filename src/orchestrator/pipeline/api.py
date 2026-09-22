"""FastAPI /v1 pipeline routes."""

from collections.abc import Callable
from functools import lru_cache

import structlog
from fastapi import APIRouter, HTTPException

from orchestrator.config import PipelineSettings, get_settings
from orchestrator.pipeline.commit import run_commit, run_compensate
from orchestrator.pipeline.draw import generate_draws, persist_draws
from orchestrator.pipeline.extract import run_extract
from orchestrator.pipeline.load import run_load
from orchestrator.pipeline.models import (
    CommitRequest,
    CommitResponse,
    CompensateRequest,
    CompensateResponse,
    DrawRequest,
    DrawResponse,
    ExtractRequest,
    ExtractResponse,
    LoadRequest,
    LoadResponse,
    ScoreRequest,
    ScoreResponse,
)
from orchestrator.pipeline.score import run_score
from orchestrator.surrealdb.client import SurrealDBClient

logger = structlog.get_logger(__name__)


@lru_cache
def _pipeline_settings() -> PipelineSettings:
    return get_settings().pipeline


def create_pipeline_router(
    client_factory: Callable[[], SurrealDBClient] | None = None,
) -> APIRouter:
    """Create versioned pipeline API router."""
    router = APIRouter(prefix="/v1", tags=["pipeline"])

    async def get_client() -> SurrealDBClient:
        if client_factory is not None:
            return client_factory()
        settings = get_settings()
        client = SurrealDBClient(settings.surrealdb)
        await client.connect()
        return client

    @router.post("/extract", response_model=ExtractResponse)
    async def extract(request: ExtractRequest) -> ExtractResponse:
        settings = _pipeline_settings()
        client = await get_client()
        try:
            return await run_extract(request, settings, client)
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc

    @router.post("/load", response_model=LoadResponse)
    async def load(request: LoadRequest) -> LoadResponse:
        client = await get_client()
        try:
            return await run_load(request, client)
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/draw", response_model=DrawResponse)
    async def draw(request: DrawRequest, persist: bool = False) -> DrawResponse:
        if persist:
            client = await get_client()
            return await persist_draws(request, client)
        return generate_draws(request)

    @router.post("/score", response_model=ScoreResponse)
    async def score(request: ScoreRequest) -> ScoreResponse:
        client = await get_client()
        try:
            return await run_score(request, client)
        except Exception as exc:
            logger.error("Score failed", run_id=request.run_id, error=str(exc))
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/commit", response_model=CommitResponse)
    async def commit(request: CommitRequest) -> CommitResponse:
        client = await get_client()
        return await run_commit(request, client)

    @router.post("/compensate", response_model=CompensateResponse)
    async def compensate(request: CompensateRequest) -> CompensateResponse:
        client = await get_client()
        return await run_compensate(request, client)

    return router
