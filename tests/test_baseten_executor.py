"""Tests for Baseten executor."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.executors.baseten import BasetenExecutor, ChainletError
from orchestrator.surrealdb.schema import ChainletConfig, Scenario


class TestBasetenExecutor:
    """Tests for BasetenExecutor."""

    def test_build_prompt_with_template(
        self, baseten_executor: BasetenExecutor
    ) -> None:
        """Test prompt building with a template."""
        scenario = Scenario(
            id="scenario:test",
            name="Test",
            interests=[
                {"name": "question", "type": "prompt", "value": "What is AI?"},
                {"name": "context", "type": "data", "value": "Background info"},
            ],
            chainlet_config=ChainletConfig(
                model="test",
                prompt_template="Question: {question}\nContext: {context}",
            ),
        )

        prompt = baseten_executor._build_prompt(scenario, scenario.chainlet_config)

        assert "Question: What is AI?" in prompt
        assert "Context: Background info" in prompt

    def test_build_prompt_without_template(
        self, baseten_executor: BasetenExecutor
    ) -> None:
        """Test prompt building without a template."""
        scenario = Scenario(
            id="scenario:test",
            name="Test",
            interests=[
                {"type": "prompt", "value": "Hello world"},
                {"type": "code", "value": "print('test')"},
            ],
            chainlet_config=ChainletConfig(model="test"),
        )

        prompt = baseten_executor._build_prompt(scenario, scenario.chainlet_config)

        assert "Hello world" in prompt
        assert "```" in prompt
        assert "print('test')" in prompt

    def test_build_request(self, baseten_executor: BasetenExecutor) -> None:
        """Test request building."""
        scenario = Scenario(
            id="scenario:test123",
            name="Test Scenario",
            interests=[{"type": "prompt", "value": "Test prompt"}],
            chainlet_config=ChainletConfig(
                model="llama-3",
                parameters={"max_tokens": 100},
            ),
        )

        request = baseten_executor._build_request(scenario)

        assert "prompt" in request
        assert request["model"] == "llama-3"
        assert request["parameters"] == {"max_tokens": 100}
        assert request["metadata"]["scenario_id"] == "scenario:test123"
        assert request["metadata"]["scenario_name"] == "Test Scenario"

    @pytest.mark.asyncio
    async def test_execute_no_endpoint(
        self, baseten_executor: BasetenExecutor, sample_scenario: Scenario
    ) -> None:
        """Test execution fails without endpoint."""
        baseten_executor.settings.chain_endpoint = ""

        with pytest.raises(ChainletError, match="No chainlet endpoint configured"):
            await baseten_executor.execute(sample_scenario)

    @pytest.mark.asyncio
    async def test_execute_success(
        self, baseten_executor: BasetenExecutor, sample_scenario: Scenario
    ) -> None:
        """Test successful execution with mocked HTTP client."""
        baseten_executor.settings.chain_endpoint = "http://test/predict"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"output": "Test response", "success": True}

        with patch.object(
            baseten_executor, "_get_client"
        ) as mock_get_client:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_get_client.return_value = mock_client

            request, result, latency = await baseten_executor.execute(sample_scenario)

            assert result["success"] is True
            assert result["output"] == "Test response"
            assert latency >= 0
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_http_error(
        self, baseten_executor: BasetenExecutor, sample_scenario: Scenario
    ) -> None:
        """Test execution handles HTTP errors."""
        baseten_executor.settings.chain_endpoint = "http://test/predict"

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch.object(
            baseten_executor, "_get_client"
        ) as mock_get_client:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_get_client.return_value = mock_client

            with pytest.raises(ChainletError) as exc_info:
                await baseten_executor.execute(sample_scenario)

            assert "500" in str(exc_info.value)
            assert exc_info.value.status_code == 500
