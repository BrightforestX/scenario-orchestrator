"""Tests for seeded parameter draws."""

from orchestrator.pipeline.draw import generate_draws
from orchestrator.pipeline.models import DrawRequest, ParameterBound


def test_draws_are_reproducible_with_same_seed() -> None:
    bounds = [ParameterBound(name="multiplier", min=0.5, max=2.0)]
    req = DrawRequest(run_id="run-a", seed=12345, bounds=bounds, num_samples=3)
    first = generate_draws(req)
    second = generate_draws(req)
    assert first.draws == second.draws


def test_draws_differ_with_different_seed() -> None:
    bounds = [ParameterBound(name="multiplier", min=0.5, max=2.0)]
    a = generate_draws(DrawRequest(run_id="run-a", seed=1, bounds=bounds, num_samples=2))
    b = generate_draws(DrawRequest(run_id="run-a", seed=2, bounds=bounds, num_samples=2))
    assert a.draws[0].params != b.draws[0].params
