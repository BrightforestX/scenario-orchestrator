"""Pydantic models for the durable scenario analysis pipeline API."""

from typing import Any, Literal

from pydantic import BaseModel, Field


class ExtractRequest(BaseModel):
    """Extract data from Trino SQL or a fixture/staging path."""

    run_id: str = Field(description="Unique pipeline run identifier")
    sql: str | None = Field(default=None, description="Trino SQL query (optional)")
    fixture_path: str | None = Field(default=None, description="Local fixture JSONL path")
    staging_dir: str | None = Field(default=None, description="Directory for output JSONL")


class ExtractResponse(BaseModel):
    path: str
    checksum: str
    run_id: str
    record_count: int


class LoadRequest(BaseModel):
    run_id: str
    path: str
    table: Literal["openapi_fact", "scenario_entity"] = "scenario_entity"
    batch_index: int = 0
    batch_size: int = Field(default=100, ge=1, le=1000)


class LoadResponse(BaseModel):
    run_id: str
    loaded: int
    batch_index: int
    total_batches: int
    next_batch_index: int | None
    records_total: int


class ParameterBound(BaseModel):
    name: str
    min: float
    max: float
    dtype: Literal["float", "int"] = "float"


class DrawSpec(BaseModel):
    scenario_id: str | None = None
    draw_index: int | None = None


class DrawRequest(BaseModel):
    run_id: str
    seed: int
    bounds: list[ParameterBound]
    num_samples: int = Field(default=1, ge=1, le=10000)
    specs: list[DrawSpec] | None = None


class ScenarioDraw(BaseModel):
    scenario_id: str
    draw_index: int
    params: dict[str, float]
    seed: int


class DrawResponse(BaseModel):
    run_id: str
    seed: int
    draws: list[ScenarioDraw]


class ScoreRequest(BaseModel):
    run_id: str
    draws: list[ScenarioDraw]
    checksum: str | None = None
    persist_shortlist: bool = True


class ScenarioScore(BaseModel):
    scenario_id: str
    draw_index: int
    score: float
    metrics: dict[str, Any] = Field(default_factory=dict)


class ScoreResponse(BaseModel):
    run_id: str
    scores: list[ScenarioScore]
    checksum: str | None = None


class WinnerRecord(BaseModel):
    scenario_id: str
    draw_index: int
    score: float
    metrics: dict[str, Any] = Field(default_factory=dict)


class CommitRequest(BaseModel):
    run_id: str
    winners: list[WinnerRecord]


class CommitResponse(BaseModel):
    run_id: str
    committed: int


class CompensateRequest(BaseModel):
    run_id: str


class CompensateResponse(BaseModel):
    run_id: str
    deleted: int


class GateCriteria(BaseModel):
    min_score: float = 0.0
    max_variance: float | None = None


class GateDecision(BaseModel):
    scenario_id: str
    draw_index: int
    decision: Literal["keep", "revert"]
    reason: str
    score: float


class GateRequest(BaseModel):
    run_id: str
    scores: list[ScenarioScore]
    criteria: GateCriteria


class GateResponse(BaseModel):
    run_id: str
    decisions: list[GateDecision]
    winners: list[WinnerRecord]
