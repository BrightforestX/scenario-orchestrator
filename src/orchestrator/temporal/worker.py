"""Temporal worker entrypoint for ScenarioPipelineWorkflow."""

import asyncio

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from orchestrator.config import get_settings
from orchestrator.temporal.activities import (
    apply_keep_decision,
    commit_winners,
    compensate_run,
    generate_draws,
    load_jsonl_to_surreal,
    run_trino_query,
    score_scenarios,
    transform_jsonl,
)
from orchestrator.temporal.workflow import ScenarioPipelineWorkflow

logger = structlog.get_logger(__name__)


async def run_worker() -> None:
    settings = get_settings().temporal
    client = await Client.connect(settings.host, namespace=settings.namespace)
    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[ScenarioPipelineWorkflow],
        activities=[
            run_trino_query,
            transform_jsonl,
            load_jsonl_to_surreal,
            generate_draws,
            score_scenarios,
            apply_keep_decision,
            commit_winners,
            compensate_run,
        ],
    )
    logger.info("Starting Temporal worker", task_queue=settings.task_queue)
    await worker.run()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
