"""Baseten chainlet executor for scenario inference."""

import time
from typing import Any

import httpx
import structlog

from orchestrator.config import BasetenSettings
from orchestrator.surrealdb.schema import ChainletConfig, Scenario

logger = structlog.get_logger(__name__)


class ChainletError(Exception):
    """Error from chainlet execution."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class BasetenExecutor:
    """Executes scenarios via Baseten chainlets.

    This executor sends scenario data to a Baseten chainlet endpoint
    for inference using open source models (Llama, Mistral, etc.).
    """

    def __init__(self, settings: BasetenSettings) -> None:
        """Initialize the Baseten executor.

        Args:
            settings: Baseten configuration settings
        """
        self.settings = settings
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.timeout_seconds),
                headers={
                    "Authorization": f"Api-Key {self.settings.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def _build_request(self, scenario: Scenario) -> dict[str, Any]:
        """Build the request payload for the chainlet.

        Args:
            scenario: The scenario to execute

        Returns:
            Request payload dictionary
        """
        config = scenario.chainlet_config

        # Build the prompt from interests
        prompt = self._build_prompt(scenario, config)

        request: dict[str, Any] = {
            "prompt": prompt,
            "parameters": config.parameters,
        }

        # Add model if specified
        if config.model:
            request["model"] = config.model

        # Add scenario metadata for tracing
        request["metadata"] = {
            "scenario_id": scenario.id,
            "scenario_name": scenario.name,
            "interests_count": len(scenario.interests),
        }

        return request

    def _build_prompt(self, scenario: Scenario, config: ChainletConfig) -> str:
        """Build the prompt from scenario interests.

        Args:
            scenario: The scenario containing interests
            config: Chainlet configuration with optional template

        Returns:
            The constructed prompt string
        """
        # If there's a template, use it
        if config.prompt_template:
            # Replace placeholders with interest values
            prompt = config.prompt_template
            for i, interest in enumerate(scenario.interests):
                placeholder = f"{{interest_{i}}}"
                value = interest.get("value", "")
                if isinstance(value, str):
                    prompt = prompt.replace(placeholder, value)
                else:
                    prompt = prompt.replace(placeholder, str(value))

                # Also support named placeholders
                name = interest.get("name", "")
                if name:
                    named_placeholder = f"{{{name}}}"
                    if isinstance(value, str):
                        prompt = prompt.replace(named_placeholder, value)
                    else:
                        prompt = prompt.replace(named_placeholder, str(value))

            return prompt

        # Default: concatenate interest values
        parts = []
        for interest in scenario.interests:
            interest_type = interest.get("type", "")
            value = interest.get("value", "")

            if interest_type == "prompt":
                parts.append(str(value))
            elif interest_type == "code":
                parts.append(f"```\n{value}\n```")
            elif interest_type == "data":
                parts.append(f"Data: {value}")
            elif interest_type == "config":
                parts.append(f"Config: {value}")
            else:
                parts.append(str(value))

        return "\n\n".join(parts)

    async def execute(
        self, scenario: Scenario, endpoint_override: str | None = None
    ) -> tuple[dict[str, Any], dict[str, Any], int]:
        """Execute a scenario via the chainlet.

        Args:
            scenario: The scenario to execute
            endpoint_override: Optional endpoint URL override

        Returns:
            Tuple of (request, result, latency_ms)

        Raises:
            ChainletError: If the chainlet request fails
        """
        client = await self._get_client()
        endpoint = endpoint_override or self.settings.chain_endpoint

        if not endpoint:
            raise ChainletError("No chainlet endpoint configured")

        request = self._build_request(scenario)

        logger.info(
            "Executing chainlet",
            scenario_id=scenario.id,
            endpoint=endpoint,
            model=scenario.chainlet_config.model,
        )

        start_time = time.monotonic()

        try:
            response = await client.post(endpoint, json=request)
            latency_ms = int((time.monotonic() - start_time) * 1000)

            if response.status_code != 200:
                error_text = response.text
                logger.error(
                    "Chainlet request failed",
                    scenario_id=scenario.id,
                    status_code=response.status_code,
                    error=error_text,
                )
                raise ChainletError(
                    f"Chainlet returned {response.status_code}: {error_text}",
                    status_code=response.status_code,
                )

            result = response.json()

            logger.info(
                "Chainlet execution completed",
                scenario_id=scenario.id,
                latency_ms=latency_ms,
            )

            return request, result, latency_ms

        except httpx.TimeoutException as e:
            latency_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                "Chainlet request timed out",
                scenario_id=scenario.id,
                timeout_seconds=self.settings.timeout_seconds,
                latency_ms=latency_ms,
            )
            raise ChainletError(f"Request timed out after {latency_ms}ms") from e

        except httpx.RequestError as e:
            logger.error(
                "Chainlet request error",
                scenario_id=scenario.id,
                error=str(e),
            )
            raise ChainletError(f"Request failed: {str(e)}") from e

    async def health_check(self, endpoint_override: str | None = None) -> bool:
        """Check if the chainlet endpoint is healthy.

        Args:
            endpoint_override: Optional endpoint URL override

        Returns:
            True if healthy, False otherwise
        """
        client = await self._get_client()
        endpoint = endpoint_override or self.settings.chain_endpoint

        if not endpoint:
            return False

        # Try a simple request - many Baseten endpoints have a /health path
        health_endpoint = endpoint.rstrip("/predict") + "/health"

        try:
            response = await client.get(health_endpoint, timeout=5.0)
            return response.status_code == 200
        except Exception:
            # If no health endpoint, try the main endpoint with minimal payload
            try:
                response = await client.post(
                    endpoint,
                    json={"prompt": "test", "parameters": {"max_tokens": 1}},
                    timeout=10.0,
                )
                return response.status_code == 200
            except Exception:
                return False
