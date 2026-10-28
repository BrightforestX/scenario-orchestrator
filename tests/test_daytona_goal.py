"""Tests for Daytona coding-agent /goal harness."""

from orchestrator.executors.daytona import build_goal_prefix, harness_coding_agent_command
from orchestrator.surrealdb.schema import CodingAgentGoal


class TestCodingAgentGoalHarness:
    def test_build_goal_prefix_with_proof_commands(self) -> None:
        goal = CodingAgentGoal(
            measurable_end="all tests green",
            proof_commands=["pytest -q", "mypy --strict src"],
            max_turns=12,
        )
        prefix = build_goal_prefix(goal)
        assert prefix == (
            "/goal all tests green; "
            "proven by pytest -q exit 0; mypy --strict src exit 0; "
            "stop after 12 turns"
        )

    def test_build_goal_prefix_default_proof(self) -> None:
        goal = CodingAgentGoal(measurable_end="feature shipped", max_turns=5)
        assert "proven by pytest exit 0" in build_goal_prefix(goal)

    def test_harness_prefixes_cursor_agent(self) -> None:
        goal = CodingAgentGoal(measurable_end="done", max_turns=3)
        raw = "cursor agent --model composer"
        harnessed = harness_coding_agent_command(raw, goal)
        assert harnessed.startswith("/goal done;")
        assert harnessed.endswith(raw)

    def test_harness_skips_non_agent_commands(self) -> None:
        goal = CodingAgentGoal(measurable_end="done", max_turns=3)
        assert harness_coding_agent_command("echo hello", goal) == "echo hello"

    def test_harness_idempotent_when_already_goaled(self) -> None:
        goal = CodingAgentGoal(measurable_end="done", max_turns=3)
        already = "/goal done; proven by pytest exit 0; stop after 3 turns; cursor agent"
        assert harness_coding_agent_command(already, goal) == already
