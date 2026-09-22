"""Checksum utilities for JSONL pipeline artifacts."""

import hashlib
from pathlib import Path


def sha256_file(path: Path) -> str:
    """Return hex SHA-256 checksum of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Return hex SHA-256 checksum of bytes."""
    return hashlib.sha256(data).hexdigest()
