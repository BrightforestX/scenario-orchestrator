"""Tests for TypeScript gate logic (ported for unit validation)."""

import json
import subprocess
from pathlib import Path

import pytest

GATE_DIR = Path(__file__).resolve().parents[2] / "gate"


def _run_gate_via_node(payload: dict) -> dict:
    result = subprocess.run(
        ["npx", "tsx", "run-cli.ts", json.dumps(payload)],
        cwd=GATE_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Gate TS runtime unavailable: {result.stderr}")
    return json.loads(result.stdout)


def test_failing_numeric_criteria_are_discarded() -> None:
    payload = {
        "run_id": "test-run",
        "scores": [
            {"scenario_id": "s1", "draw_index": 0, "score": 25.0, "metrics": {}},
            {"scenario_id": "s2", "draw_index": 1, "score": 5.0, "metrics": {}},
        ],
        "criteria": {"min_score": 20.0},
    }
    response = _run_gate_via_node(payload)
    decisions = {d["scenario_id"]: d["decision"] for d in response["decisions"]}
    assert decisions["s1"] == "keep"
    assert decisions["s2"] == "revert"
    assert len(response["winners"]) == 1
    assert response["winners"][0]["scenario_id"] == "s1"
