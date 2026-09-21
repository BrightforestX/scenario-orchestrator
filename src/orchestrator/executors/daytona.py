"""Daytona sandbox executor for code execution."""

import asyncio
from typing import Any

import httpx
import structlog

from orchestrator.config import DaytonaSettings
from orchestrator.surrealdb.schema import SandboxConfig, Scenario

logger = structlog.get_logger(__name__)


class SandboxError(Exception):
    """Error from sandbox operations."""

    def __init__(self, message: str, sandbox_id: str | None = None) -> None:
        super().__init__(message)
        self.sandbox_id = sandbox_id


class DaytonaExecutor:
    """Executes code in Daytona sandboxes.

    This executor creates ephemeral sandboxes for running code,
    uploads files, executes commands, and captures output.
    """

    def __init__(self, settings: DaytonaSettings) -> None:
        """Initialize the Daytona executor.

        Args:
            settings: Daytona configuration settings
        """
        self.settings = settings
        self._client: httpx.AsyncClient | None = None
        # Daytona MCP base URL - typically accessed via MCP protocol
        self._base_url = "https://api.daytona.io"

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0),  # 5 minute timeout for sandbox ops
                headers={
                    "Authorization": f"Bearer {self.settings.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def create_sandbox(
        self,
        scenario: Scenario,
        config: SandboxConfig,
    ) -> str:
        """Create a new sandbox for the scenario.

        Args:
            scenario: The scenario being executed
            config: Sandbox configuration

        Returns:
            The sandbox ID

        Raises:
            SandboxError: If sandbox creation fails
        """
        client = await self._get_client()

        # Build sandbox creation request
        request = {
            "name": f"scenario-{scenario.id or 'unknown'}",
            "target": self.settings.target_region,
            "cpu": config.cpu,
            "memory": config.memory,
            "disk": config.disk,
            "autoStopInterval": self.settings.auto_stop_minutes,
            "autoArchiveInterval": self.settings.auto_archive_minutes,
            "env": config.env,
            "labels": {
                "scenario_id": scenario.id or "",
                "scenario_name": scenario.name,
                "orchestrator": "scenario-orchestrator",
            },
        }

        logger.info(
            "Creating Daytona sandbox",
            scenario_id=scenario.id,
            cpu=config.cpu,
            memory=config.memory,
        )

        try:
            response = await client.post(
                f"{self._base_url}/v1/sandbox",
                json=request,
            )

            if response.status_code not in [200, 201]:
                raise SandboxError(
                    f"Failed to create sandbox: {response.status_code} - {response.text}"
                )

            result = response.json()
            sandbox_id = result.get("id", "")

            logger.info(
                "Sandbox created",
                scenario_id=scenario.id,
                sandbox_id=sandbox_id,
            )

            # Wait for sandbox to be ready
            await self._wait_for_ready(sandbox_id)

            return sandbox_id

        except httpx.RequestError as e:
            raise SandboxError(f"Failed to create sandbox: {str(e)}") from e

    async def _wait_for_ready(
        self, sandbox_id: str, timeout_seconds: int = 120
    ) -> None:
        """Wait for a sandbox to be ready.

        Args:
            sandbox_id: The sandbox ID to wait for
            timeout_seconds: Maximum time to wait

        Raises:
            SandboxError: If sandbox doesn't become ready
        """
        client = await self._get_client()
        start_time = asyncio.get_event_loop().time()

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout_seconds:
                raise SandboxError(
                    f"Sandbox did not become ready within {timeout_seconds}s",
                    sandbox_id=sandbox_id,
                )

            try:
                response = await client.get(f"{self._base_url}/v1/sandbox/{sandbox_id}")
                if response.status_code == 200:
                    status = response.json().get("state", "")
                    if status == "running":
                        return
                    elif status in ["failed", "error"]:
                        raise SandboxError(
                            f"Sandbox failed to start: {status}",
                            sandbox_id=sandbox_id,
                        )
            except httpx.RequestError:
                pass

            await asyncio.sleep(2)

    async def upload_files(
        self, sandbox_id: str, files: dict[str, str]
    ) -> None:
        """Upload files to the sandbox.

        Args:
            sandbox_id: The sandbox ID
            files: Dictionary of path -> content

        Raises:
            SandboxError: If file upload fails
        """
        client = await self._get_client()

        for file_path, content in files.items():
            try:
                response = await client.post(
                    f"{self._base_url}/v1/sandbox/{sandbox_id}/files",
                    json={
                        "filePath": file_path,
                        "content": content,
                        "encoding": "utf-8",
                        "overwrite": True,
                    },
                )

                if response.status_code not in [200, 201]:
                    raise SandboxError(
                        f"Failed to upload {file_path}: {response.text}",
                        sandbox_id=sandbox_id,
                    )

                logger.debug(
                    "Uploaded file",
                    sandbox_id=sandbox_id,
                    file_path=file_path,
                )

            except httpx.RequestError as e:
                raise SandboxError(
                    f"Failed to upload {file_path}: {str(e)}",
                    sandbox_id=sandbox_id,
                ) from e

    async def execute_command(
        self,
        sandbox_id: str,
        command: str,
        timeout_seconds: int = 300,
    ) -> tuple[str, int]:
        """Execute a command in the sandbox.

        Args:
            sandbox_id: The sandbox ID
            command: The command to execute
            timeout_seconds: Command timeout

        Returns:
            Tuple of (output, exit_code)

        Raises:
            SandboxError: If command execution fails
        """
        client = await self._get_client()

        logger.info(
            "Executing command in sandbox",
            sandbox_id=sandbox_id,
            command=command[:100],
        )

        try:
            response = await client.post(
                f"{self._base_url}/v1/sandbox/{sandbox_id}/exec",
                json={"command": command},
                timeout=httpx.Timeout(float(timeout_seconds)),
            )

            if response.status_code != 200:
                raise SandboxError(
                    f"Command execution failed: {response.text}",
                    sandbox_id=sandbox_id,
                )

            result = response.json()
            output = result.get("output", "")
            exit_code = result.get("exitCode", -1)

            logger.info(
                "Command completed",
                sandbox_id=sandbox_id,
                exit_code=exit_code,
                output_length=len(output),
            )

            return output, exit_code

        except httpx.TimeoutException:
            raise SandboxError(
                f"Command timed out after {timeout_seconds}s",
                sandbox_id=sandbox_id,
            )
        except httpx.RequestError as e:
            raise SandboxError(
                f"Command execution failed: {str(e)}",
                sandbox_id=sandbox_id,
            ) from e

    async def destroy_sandbox(self, sandbox_id: str) -> None:
        """Destroy a sandbox.

        Args:
            sandbox_id: The sandbox ID to destroy
        """
        client = await self._get_client()

        try:
            response = await client.delete(
                f"{self._base_url}/v1/sandbox/{sandbox_id}"
            )

            if response.status_code not in [200, 204, 404]:
                logger.warning(
                    "Failed to destroy sandbox",
                    sandbox_id=sandbox_id,
                    status_code=response.status_code,
                )
            else:
                logger.info("Sandbox destroyed", sandbox_id=sandbox_id)

        except httpx.RequestError as e:
            logger.warning(
                "Error destroying sandbox",
                sandbox_id=sandbox_id,
                error=str(e),
            )

    async def execute(
        self, scenario: Scenario
    ) -> tuple[str, str]:
        """Execute a scenario's sandbox configuration.

        Args:
            scenario: The scenario with sandbox config

        Returns:
            Tuple of (sandbox_id, output)

        Raises:
            SandboxError: If execution fails
        """
        config = scenario.sandbox_config
        if not config or not config.enabled:
            raise SandboxError("Sandbox config not enabled for scenario")

        sandbox_id = ""

        try:
            # Create sandbox
            sandbox_id = await self.create_sandbox(scenario, config)

            # Upload files if any
            if config.files:
                await self.upload_files(sandbox_id, config.files)

            # Execute commands
            outputs: list[str] = []
            for command in config.commands:
                output, exit_code = await self.execute_command(sandbox_id, command)
                outputs.append(f"$ {command}\n{output}")

                if exit_code != 0:
                    logger.warning(
                        "Command returned non-zero exit code",
                        sandbox_id=sandbox_id,
                        command=command[:50],
                        exit_code=exit_code,
                    )

            combined_output = "\n\n".join(outputs)

            return sandbox_id, combined_output

        finally:
            # Always try to destroy the sandbox
            if sandbox_id:
                await self.destroy_sandbox(sandbox_id)

    async def execute_and_keep(
        self, scenario: Scenario
    ) -> tuple[str, str]:
        """Execute scenario but keep the sandbox running.

        Useful for debugging or long-running scenarios.

        Args:
            scenario: The scenario with sandbox config

        Returns:
            Tuple of (sandbox_id, output)
        """
        config = scenario.sandbox_config
        if not config or not config.enabled:
            raise SandboxError("Sandbox config not enabled for scenario")

        # Create sandbox
        sandbox_id = await self.create_sandbox(scenario, config)

        # Upload files if any
        if config.files:
            await self.upload_files(sandbox_id, config.files)

        # Execute commands
        outputs: list[str] = []
        for command in config.commands:
            output, exit_code = await self.execute_command(sandbox_id, command)
            outputs.append(f"$ {command}\n{output}")

        combined_output = "\n\n".join(outputs)

        logger.info(
            "Sandbox execution complete (keeping sandbox)",
            sandbox_id=sandbox_id,
        )

        return sandbox_id, combined_output
