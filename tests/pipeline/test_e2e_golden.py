"""Golden fixture end-to-end pipeline tests (no live Trino/Baseten)."""

import json
from pathlib import Path

import pytest

from orchestrator.config import PipelineSettings
from orchestrator.pipeline.commit import run_commit, run_compensate
from orchestrator.pipeline.draw import generate_draws
from orchestrator.pipeline.extract import run_extract
from orchestrator.pipeline.load import run_load
from orchestrator.pipeline.models import (
    CommitRequest,
    CompensateRequest,
    DrawRequest,
    ExtractRequest,
    LoadRequest,
    ParameterBound,
    ScoreRequest,
    WinnerRecord,
)
from orchestrator.pipeline.score import run_score
from orchestrator.surrealdb.client import SurrealDBClient

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "golden"


async def _run_pipeline_through_score(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    golden_fixture_path: Path,
    golden_bounds: list[dict[str, object]],
    run_id: str = "golden-run-001",
    seed: int = 42,
) -> tuple[str, list]:
    extract = await run_extract(
        ExtractRequest(
            run_id=run_id,
            fixture_path=str(golden_fixture_path),
            staging_dir=pipeline_settings.staging_dir,
        ),
        pipeline_settings,
        pipeline_db,
    )

    batch_index = 0
    while True:
        load = await run_load(
            LoadRequest(
                run_id=run_id,
                path=extract.path,
                table="scenario_entity",
                batch_index=batch_index,
                batch_size=2,
            ),
            pipeline_db,
        )
        if load.next_batch_index is None:
            break
        batch_index = load.next_batch_index

    bounds = [ParameterBound.model_validate(b) for b in golden_bounds]
    draws = generate_draws(
        DrawRequest(run_id=run_id, seed=seed, bounds=bounds, num_samples=3)
    )
    scores = await run_score(
        ScoreRequest(
            run_id=run_id,
            draws=draws.draws,
            checksum=extract.checksum,
            persist_shortlist=True,
        ),
        pipeline_db,
    )
    return extract.checksum, scores.scores


@pytest.mark.asyncio
async def test_golden_fixture_e2e_score_and_commit(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    golden_fixture_path: Path,
    golden_bounds: list[dict[str, object]],
    golden_criteria: dict[str, float],
) -> None:
    checksum, scores = await _run_pipeline_through_score(
        pipeline_db,
        pipeline_settings,
        golden_fixture_path,
        golden_bounds,
        seed=3,
    )
    assert checksum
    assert len(scores) == 3

    winners = [
        WinnerRecord(
            scenario_id=s.scenario_id,
            draw_index=s.draw_index,
            score=s.score,
            metrics=s.metrics,
        )
        for s in scores
        if s.score >= golden_criteria["min_score"]
    ]
    assert any(s.score < golden_criteria["min_score"] for s in scores)
    assert len(winners) < len(scores)

    commit = await run_commit(
        CommitRequest(run_id="golden-run-001", winners=winners),
        pipeline_db,
    )
    assert commit.committed == len(winners)


@pytest.mark.asyncio
async def test_idempotent_load_resume_no_duplicates(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    golden_fixture_path: Path,
) -> None:
    run_id = "idempotent-run"
    extract = await run_extract(
        ExtractRequest(
            run_id=run_id,
            fixture_path=str(golden_fixture_path),
            staging_dir=pipeline_settings.staging_dir,
        ),
        pipeline_settings,
        pipeline_db,
    )

    for batch_index in (0, 1):
        await run_load(
            LoadRequest(
                run_id=run_id,
                path=extract.path,
                batch_index=batch_index,
                batch_size=2,
            ),
            pipeline_db,
        )

    # Simulate mid-load resume: re-run batch 1 only
    await run_load(
        LoadRequest(
            run_id=run_id,
            path=extract.path,
            batch_index=1,
            batch_size=2,
        ),
        pipeline_db,
    )

    rows = await pipeline_db.query(
        "SELECT count() AS count FROM scenario_entity WHERE run_id = $run_id GROUP ALL",
        {"run_id": run_id},
    )
    count = rows[0]["count"] if isinstance(rows, list) and rows else 0
    assert count == 3


@pytest.mark.asyncio
async def test_scoring_reproducible_same_checksum_seed(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    golden_fixture_path: Path,
    golden_bounds: list[dict[str, object]],
) -> None:
    checksum_a, scores_a = await _run_pipeline_through_score(
        pipeline_db,
        pipeline_settings,
        golden_fixture_path,
        golden_bounds,
        run_id="repro-a",
        seed=999,
    )
    checksum_b, scores_b = await _run_pipeline_through_score(
        pipeline_db,
        pipeline_settings,
        golden_fixture_path,
        golden_bounds,
        run_id="repro-b",
        seed=999,
    )
    assert checksum_a == checksum_b
    assert [s.score for s in scores_a] == [s.score for s in scores_b]


@pytest.mark.asyncio
async def test_compensate_deletes_staged_winners(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    golden_fixture_path: Path,
    golden_bounds: list[dict[str, object]],
) -> None:
    run_id = "compensate-run"
    _, scores = await _run_pipeline_through_score(
        pipeline_db,
        pipeline_settings,
        golden_fixture_path,
        golden_bounds,
        run_id=run_id,
    )
    await run_commit(
        CommitRequest(
            run_id=run_id,
            winners=[
                WinnerRecord(
                    scenario_id=s.scenario_id,
                    draw_index=s.draw_index,
                    score=s.score,
                    metrics=s.metrics,
                )
                for s in scores
            ],
        ),
        pipeline_db,
    )

    # Stage uncommitted winners then compensate
    await pipeline_db.query(
        "CREATE scenario_winner CONTENT $content",
        {
            "content": {
                "run_id": run_id,
                "scenario_id": "staged",
                "draw_index": 99,
                "score": 0.0,
                "committed": False,
            }
        },
    )
    result = await run_compensate(CompensateRequest(run_id=run_id), pipeline_db)
    assert result.deleted >= 1

    remaining = await pipeline_db.query(
        "SELECT * FROM scenario_winner WHERE run_id = $run_id AND committed = false",
        {"run_id": run_id},
    )
    assert remaining == [] or (isinstance(remaining, list) and len(remaining) == 0)


@pytest.mark.asyncio
async def test_openapi_fact_jsonl_contract(
    pipeline_db: SurrealDBClient,
    pipeline_settings: PipelineSettings,
    tmp_path: Path,
) -> None:
    fact_path = tmp_path / "facts.jsonl"
    fact_path.write_text(
        json.dumps(
            {
                "id": "openapi_fact:f1",
                "source": "fixture",
                "pulled_at": "2026-09-21T00:00:00Z",
                "payload_hash": "sha256:abc",
                "payload": {"value": 1.0},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    run_id = "openapi-run"
    extract = await run_extract(
        ExtractRequest(
            run_id=run_id,
            fixture_path=str(fact_path),
            staging_dir=pipeline_settings.staging_dir,
        ),
        pipeline_settings,
        pipeline_db,
    )
    await run_load(
        LoadRequest(run_id=run_id, path=extract.path, table="openapi_fact", batch_size=10),
        pipeline_db,
    )
    rows = await pipeline_db.query(
        "SELECT * FROM openapi_fact WHERE run_id = $run_id",
        {"run_id": run_id},
    )
    assert isinstance(rows, list) and len(rows) == 1
    assert rows[0]["payload_hash"] == "sha256:abc"
