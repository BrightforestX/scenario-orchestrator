"""Buzz Nostr event publisher for scenario results."""

import asyncio
import hashlib
import json
import time
from typing import Any

import structlog
import websockets
from websockets.asyncio.client import ClientConnection

from orchestrator.config import BuzzSettings
from orchestrator.surrealdb.schema import ExecutionResult, Scenario

logger = structlog.get_logger(__name__)

# Buzz/Nostr event kinds
KIND_JOB_REQUEST = 43001  # Agent job request
KIND_STREAM_MESSAGE = 9  # Chat message in a Stream channel


class NostrEvent:
    """Represents a Nostr event."""

    def __init__(
        self,
        kind: int,
        content: str,
        tags: list[list[str]],
        pubkey: str,
        created_at: int | None = None,
    ) -> None:
        """Initialize a Nostr event.

        Args:
            kind: Event kind number
            content: Event content (JSON or plain text)
            tags: Event tags
            pubkey: Author's public key (hex)
            created_at: Unix timestamp (defaults to now)
        """
        self.kind = kind
        self.content = content
        self.tags = tags
        self.pubkey = pubkey
        self.created_at = created_at or int(time.time())
        self.id = ""
        self.sig = ""

    def serialize_for_id(self) -> str:
        """Serialize event for ID calculation."""
        return json.dumps(
            [
                0,
                self.pubkey,
                self.created_at,
                self.kind,
                self.tags,
                self.content,
            ],
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def calculate_id(self) -> str:
        """Calculate the event ID."""
        serialized = self.serialize_for_id()
        self.id = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        return self.id

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "pubkey": self.pubkey,
            "created_at": self.created_at,
            "kind": self.kind,
            "tags": self.tags,
            "content": self.content,
            "sig": self.sig,
        }


def sign_event(event: NostrEvent, private_key_hex: str) -> str:
    """Sign a Nostr event with a private key.

    Args:
        event: The event to sign
        private_key_hex: Private key in hex format

    Returns:
        Signature in hex format
    """
    try:
        import secp256k1
    except ImportError:
        logger.warning("secp256k1 not available, using dummy signature")
        return "0" * 128

    # Ensure event ID is calculated
    if not event.id:
        event.calculate_id()

    # Convert hex strings to bytes
    private_key_bytes = bytes.fromhex(private_key_hex)
    message_bytes = bytes.fromhex(event.id)

    # Create key and sign
    key = secp256k1.PrivateKey(private_key_bytes)
    sig = key.schnorr_sign(message_bytes, None, raw=True)

    return sig.hex()


def get_pubkey_from_private(private_key_hex: str) -> str:
    """Get public key from private key.

    Args:
        private_key_hex: Private key in hex format

    Returns:
        Public key in hex format (32 bytes, x-only)
    """
    try:
        import secp256k1
    except ImportError:
        logger.warning("secp256k1 not available, using dummy pubkey")
        return "0" * 64

    private_key_bytes = bytes.fromhex(private_key_hex)
    key = secp256k1.PrivateKey(private_key_bytes)

    # Get x-only public key (32 bytes)
    pubkey = key.pubkey.serialize(compressed=True)
    # Remove the prefix byte (02 or 03) to get x-only
    return pubkey[1:].hex()


class BuzzPublisher:
    """Publishes scenario execution results to Buzz channels.

    This publisher connects to the Buzz relay via WebSocket and
    publishes execution results as signed Nostr events.
    """

    def __init__(self, settings: BuzzSettings) -> None:
        """Initialize the Buzz publisher.

        Args:
            settings: Buzz configuration settings
        """
        self.settings = settings
        self._ws: ClientConnection | None = None
        self._connected = False
        self._lock = asyncio.Lock()

        # Derive public key from private key
        if settings.private_key:
            self._pubkey = get_pubkey_from_private(settings.private_key)
        else:
            self._pubkey = ""

    async def connect(self) -> None:
        """Connect to the Buzz relay."""
        async with self._lock:
            if self._connected:
                return

            try:
                self._ws = await websockets.connect(self.settings.relay_url)
                self._connected = True
                logger.info("Connected to Buzz relay", url=self.settings.relay_url)
            except Exception as e:
                logger.error("Failed to connect to Buzz relay", error=str(e))
                raise

    async def disconnect(self) -> None:
        """Disconnect from the Buzz relay."""
        async with self._lock:
            if self._ws:
                await self._ws.close()
                self._ws = None
            self._connected = False
            logger.info("Disconnected from Buzz relay")

    async def _ensure_connected(self) -> ClientConnection:
        """Ensure we're connected to the relay."""
        if not self._connected or self._ws is None:
            await self.connect()
        assert self._ws is not None
        return self._ws

    def _build_content(
        self,
        scenario: Scenario,
        result: ExecutionResult,
    ) -> str:
        """Build the event content from scenario and result.

        Args:
            scenario: The executed scenario
            result: The execution result

        Returns:
            JSON content string
        """
        config = scenario.buzz_config

        # If there's a custom message template, use it
        if config and config.custom_message:
            content = config.custom_message
            # Replace placeholders
            content = content.replace("{scenario_name}", scenario.name)
            content = content.replace("{scenario_id}", scenario.id or "")
            content = content.replace("{success}", str(result.success))
            if result.error:
                content = content.replace("{error}", result.error)
            return content

        # Build default content
        parts = [f"**Scenario Execution: {scenario.name}**"]

        if result.success:
            parts.append("Status: ✅ Completed")
        else:
            parts.append(f"Status: ❌ Failed - {result.error or 'Unknown error'}")

        # Include chainlet result if configured
        if config and config.include_chainlet_result and result.chainlet_result:
            output = result.chainlet_result.get("output", "")
            if output:
                parts.append(f"\n**Model Output:**\n```\n{output[:2000]}\n```")

        # Include sandbox output if configured
        if config and config.include_sandbox_output and result.sandbox_output:
            parts.append(f"\n**Sandbox Output:**\n```\n{result.sandbox_output[:2000]}\n```")

        # Add latency info
        if result.latency_ms:
            parts.append(f"\n_Completed in {result.latency_ms}ms_")

        return "\n".join(parts)

    def _build_tags(
        self,
        scenario: Scenario,
        result: ExecutionResult,
        channel: str,
    ) -> list[list[str]]:
        """Build event tags.

        Args:
            scenario: The executed scenario
            result: The execution result
            channel: Target channel

        Returns:
            List of tag arrays
        """
        tags = [
            # Channel reference (for Buzz routing)
            ["h", channel],
            # Scenario metadata
            ["scenario_id", scenario.id or ""],
            ["scenario_name", scenario.name],
            ["execution_id", result.execution_id],
            ["success", str(result.success).lower()],
        ]

        # Add execution reference if available
        if result.buzz_event_id:
            tags.append(["e", result.buzz_event_id, "", "reply"])

        return tags

    async def publish(
        self,
        scenario: Scenario,
        result: ExecutionResult,
    ) -> str | None:
        """Publish an execution result to Buzz.

        Args:
            scenario: The executed scenario
            result: The execution result

        Returns:
            The event ID if successful, None otherwise
        """
        if not self.settings.private_key:
            logger.warning("No private key configured, cannot publish to Buzz")
            return None

        ws = await self._ensure_connected()

        # Determine target channel
        channel = self.settings.default_channel
        if scenario.buzz_config and scenario.buzz_config.channel:
            channel = scenario.buzz_config.channel

        # Build the event
        content = self._build_content(scenario, result)
        tags = self._build_tags(scenario, result, channel)

        event = NostrEvent(
            kind=KIND_STREAM_MESSAGE,
            content=content,
            tags=tags,
            pubkey=self._pubkey,
        )

        # Calculate ID and sign
        event.calculate_id()
        event.sig = sign_event(event, self.settings.private_key)

        # Send to relay
        message = json.dumps(["EVENT", event.to_dict()])

        try:
            await ws.send(message)

            # Wait for OK response
            response = await asyncio.wait_for(ws.recv(), timeout=10.0)
            response_data = json.loads(response)

            if response_data[0] == "OK" and response_data[1] == event.id:
                if response_data[2]:  # Success
                    logger.info(
                        "Published to Buzz",
                        event_id=event.id,
                        channel=channel,
                        scenario_id=scenario.id,
                    )
                    return event.id
                else:
                    logger.warning(
                        "Buzz rejected event",
                        event_id=event.id,
                        reason=response_data[3] if len(response_data) > 3 else "unknown",
                    )
                    return None

            return event.id

        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for Buzz response", event_id=event.id)
            return event.id  # Assume success on timeout

        except Exception as e:
            logger.error(
                "Failed to publish to Buzz",
                error=str(e),
                scenario_id=scenario.id,
            )
            return None

    async def publish_job_request(
        self,
        scenario: Scenario,
        job_description: str,
        additional_tags: list[list[str]] | None = None,
    ) -> str | None:
        """Publish a job request event.

        This creates a KIND_JOB_REQUEST event that can trigger
        Buzz agents to take action.

        Args:
            scenario: The scenario requesting the job
            job_description: Description of the job
            additional_tags: Additional tags to include

        Returns:
            The event ID if successful
        """
        if not self.settings.private_key:
            logger.warning("No private key configured")
            return None

        ws = await self._ensure_connected()

        tags = [
            ["scenario_id", scenario.id or ""],
            ["scenario_name", scenario.name],
        ]

        if additional_tags:
            tags.extend(additional_tags)

        event = NostrEvent(
            kind=KIND_JOB_REQUEST,
            content=json.dumps({
                "description": job_description,
                "scenario": scenario.name,
                "interests": scenario.interests,
            }),
            tags=tags,
            pubkey=self._pubkey,
        )

        event.calculate_id()
        event.sig = sign_event(event, self.settings.private_key)

        message = json.dumps(["EVENT", event.to_dict()])

        try:
            await ws.send(message)
            logger.info(
                "Published job request to Buzz",
                event_id=event.id,
                scenario_id=scenario.id,
            )
            return event.id

        except Exception as e:
            logger.error(
                "Failed to publish job request",
                error=str(e),
                scenario_id=scenario.id,
            )
            return None
