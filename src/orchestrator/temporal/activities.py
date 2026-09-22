"""Temporal activities for the durable scenario analysis pipeline."""

from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from temporalio import activity

from orchestrator.config import TemporalSettings, get_settings

logger = structlog.get_logger(__name__)


@dataclass
class PipelineInput:
    run_id: str
    fixture_path: str | None = None
    sql: str | None = None
    seed: int = 42
    bounds: list[dict[str, Any]] | None = None
    num_samples: int = 3
    min_score: float = 0.0
    batch_size: int = 50


@activity.defn
async def run_trino_query(input: PipelineInput) -> dict[str, Any]:
    """Extract via pipeline API (Trino SQL or fixture)."""
    settings = get_settings().temporal
    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=120.0) as client:
        response = await client.post(
            "/v1/extract",
            json={
                "run_id": input.run_id,
                "fixture_path": input.fixture_path,
                "sql": input.sql,
            },
        )
        response.raise_for_status()
        return response.json()


@activity.defn
async def transform_jsonl(extract_result: dict[str, Any]) -> dict[str, Any]:
    """Pass-through transform hook; returns extract metadata for downstream load."""
    return extract_result


@activity.defn
async def load_jsonl_to_surreal(
    extract_result: dict[str, Any],
    batch_size: int = 50,
) -> dict[str, Any]:
    """Load JSONL in batches until complete (heartbeat-friendly resume)."""
    settings = get_settings().temporal
    run_id = extract_result["run_id"]
    path = extract_result["path"]
    batch_index = 0
    total_loaded = 0

    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=120.0) as client:
        while True:
            activity.heartbeat({"run_id": run_id, "batch_index": batch_index})
            response = await client.post(
                "/v1/load",
                json={
                    "run_id": run_id,
                    "path": path,
                    "table": "scenario_entity",
                    "batch_index": batch_index,
                    "batch_size": batch_size,
                },
            )
            response.raise_for_status()
            payload = response.json()
            total_loaded += payload["loaded"]
            next_batch = payload.get("next_batch_index")
            if next_batch is None:
                break
            batch_index = next_batch

    return {"run_id": run_id, "loaded": total_loaded, "checksum": extract_result.get("checksum")}


@activity.defn
async def generate_draws(input: PipelineInput, checksum: str | None) -> dict[str, Any]:
    """Generate seeded parameter draws via pipeline API."""
    settings = get_settings().temporal
    bounds = input.bounds or [{"name": "multiplier", "min": 0.5, "max": 2.0, "dtype": "float"}]
    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=60.0) as client:
        response = await client.post(
            "/v1/draw?persist=true",
            json={
                "run_id": input.run_id,
                "seed": input.seed,
                "bounds": bounds,
                "num_samples": input.num_samples,
            },
        )
        response.raise_for_status()
        result = response.json()
        result["checksum"] = checksum
        return result


@activity.defn
async def score_scenarios(draw_result: dict[str, Any]) -> dict[str, Any]:
    """Score scenarios via SurrealQL fn::score_v1 — no keep/revert here."""
    settings = get_settings().temporal
    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=60.0) as client:
        response = await client.post(
            "/v1/score",
            json={
                "run_id": draw_result["run_id"],
                "draws": draw_result["draws"],
                "checksum": draw_result.get("checksum"),
                "persist_shortlist": True,
            },
        )
        response.raise_for_status()
        return response.json()


@activity.defn
async def apply_keep_decision(
    score_result: dict[str, Any],
    min_score: float = 0.0,
) -> dict[str, Any]:
    """Apply keep-or-revert via TypeScript gate HTTP (or inline for tests)."""
    settings: TemporalSettings = get_settings().temporal
    run_id = score_result["run_id"]
    scores = score_result["scores"]

    if settings.use_inline_gate:
        # Test-only path: criteria applied without TS gate service.
        decisions = []
        winners = []
        for score in scores:
            keep = float(score["score"]) >= min_score
            decisions.append(
                {
                    "scenario_id": score["scenario_id"],
                    "draw_index": score["draw_index"],
                    "decision": "keep" if keep else "revert",
                    "reason": "min_score" if keep else "below_min_score",
                    "score": score["score"],
                }
            )
            if keep:
                winners.append(
                    {
                        "scenario_id": score["scenario_id"],
                        "draw_index": score["draw_index"],
                        "score": score["score"],
                        "metrics": score.get("metrics", {}),
                    }
                )
        return {"run_id": run_id, "decisions": decisions, "winners": winners}

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            settings.gate_url,
            json={
                "run_id": run_id,
                "scores": scores,
                "criteria": {"min_score": min_score},
            },
        )
        response.raise_for_status()
        return response.json()


@activity.defn
async def commit_winners(gate_result: dict[str, Any]) -> dict[str, Any]:
    """Persist gate-approved winners."""
    settings = get_settings().temporal
    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=30.0) as client:
        response = await client.post(
            "/v1/commit",
            json={"run_id": gate_result["run_id"], "winners": gate_result["winners"]},
        )
        response.raise_for_status()
        return response.json()


@activity.defn
async def compensate_run(run_id: str) -> dict[str, Any]:
    """Compensate staged winners for a run."""
    settings = get_settings().temporal
    async with httpx.AsyncClient(base_url=settings.pipeline_api_url, timeout=30.0) as client:
        response = await client.post("/v1/compensate", json={"run_id": run_id})
        response.raise_for_status()
        return response.json()
