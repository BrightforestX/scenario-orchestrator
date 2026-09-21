"""Helper functions for SurrealDB operations."""

from typing import Any

import structlog

from orchestrator.surrealdb.client import SurrealDBClient
from orchestrator.surrealdb.schema import Scenario, ScenarioCreate

logger = structlog.get_logger(__name__)


async def batch_create_scenarios(
    client: SurrealDBClient, scenarios: list[ScenarioCreate]
) -> list[Scenario]:
    """Create multiple scenarios at once.

    Args:
        client: SurrealDB client instance
        scenarios: List of scenarios to create

    Returns:
        List of created scenarios
    """
    scenarios_data = [s.model_dump(exclude_none=True) for s in scenarios]

    result = await client.query(
        "RETURN fn::batch_create_scenarios($scenarios)",
        {"scenarios": scenarios_data},
    )

    created = []
    if result and len(result) > 0 and result[0].get("result"):
        for item in result[0]["result"]:
            if isinstance(item, list) and len(item) > 0:
                created.append(Scenario.from_surreal(item[0]))
            elif isinstance(item, dict):
                created.append(Scenario.from_surreal(item))

    logger.info("Batch created scenarios", count=len(created))
    return created


async def cancel_pending_scenarios(client: SurrealDBClient) -> int:
    """Cancel all pending scenarios.

    Args:
        client: SurrealDB client instance

    Returns:
        Number of scenarios cancelled
    """
    result = await client.query("RETURN fn::cancel_pending_scenarios()")

    count = 0
    if result and len(result) > 0 and result[0].get("result"):
        count = result[0]["result"].get("count", 0)

    logger.info("Cancelled pending scenarios", count=count)
    return count


async def get_execution_stats(client: SurrealDBClient) -> dict[str, Any]:
    """Get execution statistics.

    Args:
        client: SurrealDB client instance

    Returns:
        Dictionary of statistics
    """
    return await client.get_stats()


async def check_dependencies_met(client: SurrealDBClient, scenario_id: str) -> bool:
    """Check if all dependencies for a scenario are met.

    Args:
        client: SurrealDB client instance
        scenario_id: ID of the scenario to check

    Returns:
        True if all dependencies are completed
    """
    if not scenario_id.startswith("scenario:"):
        scenario_id = f"scenario:{scenario_id}"

    result = await client.query(
        "RETURN fn::check_dependencies_met($scenario_id)",
        {"scenario_id": scenario_id},
    )

    if result and len(result) > 0:
        return bool(result[0].get("result", False))
    return False


async def create_scenario_dependency(
    client: SurrealDBClient,
    from_scenario: str,
    to_scenario: str,
    condition: str | None = None,
) -> bool:
    """Create a dependency between two scenarios.

    Args:
        client: SurrealDB client instance
        from_scenario: ID of the dependent scenario
        to_scenario: ID of the scenario it depends on
        condition: Optional condition string

    Returns:
        True if dependency was created
    """
    if not from_scenario.startswith("scenario:"):
        from_scenario = f"scenario:{from_scenario}"
    if not to_scenario.startswith("scenario:"):
        to_scenario = f"scenario:{to_scenario}"

    query = """
        RELATE $from->depends_on->$to SET condition = $condition
    """
    result = await client.query(
        query,
        {"from": from_scenario, "to": to_scenario, "condition": condition},
    )

    success = bool(result)
    logger.info(
        "Created scenario dependency",
        from_scenario=from_scenario,
        to_scenario=to_scenario,
        success=success,
    )
    return success


async def get_scenario_with_dependencies(
    client: SurrealDBClient, scenario_id: str
) -> dict[str, Any] | None:
    """Get a scenario with its dependencies and dependents.

    Args:
        client: SurrealDB client instance
        scenario_id: ID of the scenario

    Returns:
        Scenario data with dependencies, or None if not found
    """
    if not scenario_id.startswith("scenario:"):
        scenario_id = f"scenario:{scenario_id}"

    result = await client.query(
        "RETURN fn::get_scenario_with_deps($scenario_id)",
        {"scenario_id": scenario_id},
    )

    if result and len(result) > 0 and result[0].get("result"):
        data = result[0]["result"]
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return data
    return None


async def generate_permutations(
    client: SurrealDBClient,
    base_config: dict[str, Any],
    interest_sets: list[list[dict[str, Any]]],
) -> list[Scenario]:
    """Generate scenario permutations from a base config and interest sets.

    Args:
        client: SurrealDB client instance
        base_config: Base scenario configuration
        interest_sets: List of interest arrays (each array becomes a scenario)

    Returns:
        List of created scenarios
    """
    result = await client.query(
        "RETURN fn::generate_permutations($base_config, $interest_sets)",
        {"base_config": base_config, "interest_sets": interest_sets},
    )

    scenarios = []
    if result and len(result) > 0 and result[0].get("result"):
        for item in result[0]["result"]:
            if isinstance(item, list) and len(item) > 0:
                scenarios.append(Scenario.from_surreal(item[0]))
            elif isinstance(item, dict):
                scenarios.append(Scenario.from_surreal(item))

    logger.info("Generated scenario permutations", count=len(scenarios))
    return scenarios
