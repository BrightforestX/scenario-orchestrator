"""ScenarioPipelineWorkflow — durable orchestration for scenario analysis."""

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from orchestrator.temporal.activities import (
        PipelineInput,
        apply_keep_decision,
        commit_winners,
        generate_draws,
        load_jsonl_to_surreal,
        run_trino_query,
        score_scenarios,
        transform_jsonl,
    )


@workflow.defn
class ScenarioPipelineWorkflow:
    """Durable pipeline: extract → load → draw → score → gate → commit."""

    @workflow.run
    async def run(self, input: PipelineInput) -> dict[str, Any]:
        retry = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

        extract_result = await workflow.execute_activity(
            run_trino_query,
            input,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=retry,
        )

        transformed = await workflow.execute_activity(
            transform_jsonl,
            extract_result,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=retry,
        )

        load_result = await workflow.execute_activity(
            load_jsonl_to_surreal,
            args=[transformed, input.batch_size],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=retry,
        )

        draw_result = await workflow.execute_activity(
            generate_draws,
            args=[input, load_result.get("checksum")],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry,
        )

        score_result = await workflow.execute_activity(
            score_scenarios,
            draw_result,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry,
        )

        gate_result = await workflow.execute_activity(
            apply_keep_decision,
            args=[score_result, input.min_score],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=retry,
        )

        commit_result = await workflow.execute_activity(
            commit_winners,
            gate_result,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=retry,
        )

        return {
            "run_id": input.run_id,
            "extract": extract_result,
            "load": load_result,
            "draw": {"seed": draw_result.get("seed"), "count": len(draw_result.get("draws", []))},
            "score_count": len(score_result.get("scores", [])),
            "winners": len(gate_result.get("winners", [])),
            "committed": commit_result.get("committed", 0),
        }
