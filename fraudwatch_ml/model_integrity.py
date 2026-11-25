"""Runtime integrity checks for exported model bundles."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path


SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash one file without loading it entirely into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(path: Path) -> str:
    """Hash file contents and relative names for one immutable artifact tree."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_file():
        return sha256_file(path)

    aggregate = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file())
    if not files:
        raise ValueError(f"Artifact path contains no files: {path}")
    for item in files:
        relative = item.relative_to(path).as_posix()
        digest = sha256_file(item)
        size = item.stat().st_size
        aggregate.update(f"{relative}\0{size}\0{digest}\n".encode())
    return aggregate.hexdigest()


def verify_portable_model_digest(bundle: Path, manifest: dict) -> str:
    """Hash model bytes and require them to match the exported manifest."""
    declared = manifest.get("model_digest")
    if not isinstance(declared, str) or SHA256_PATTERN.fullmatch(declared) is None:
        raise RuntimeError("Exported model manifest has an invalid model digest")
    actual = f"sha256:{sha256_tree(bundle / 'model')}"
    if actual != declared:
        raise RuntimeError("Exported model bytes differ from the manifest digest")
    return actual
