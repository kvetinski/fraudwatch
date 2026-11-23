#!/usr/bin/env python3
"""Derive deterministic application and model image identities."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODEL_MANIFEST = ROOT / "artifacts" / "candidate-model" / "manifest.json"
SHA256 = re.compile(r"^sha256:([0-9a-f]{64})$")
IMAGE_SOURCE_PATHS = (
    ".dockerignore",
    "requirements/runtime.lock",
    "fraudwatch_ml",
    "services/ingestion",
    "services/scoring",
    "services/monitor",
    "scripts/migrate_database.py",
    "scripts/model_monitor.py",
    "scripts/quarantine.py",
    "scripts/terminal_reconciliation.py",
    "infra/migrations",
    "infra/docker/python.Dockerfile",
    "infra/docker/python.Dockerfile.dockerignore",
)


def _git(*args: str, root: Path = ROOT) -> bytes:
    return subprocess.check_output(
        ["git", *args], cwd=root, stderr=subprocess.DEVNULL
    )


def source_identity(root: Path = ROOT) -> tuple[str, str]:
    """Return the revision label and collision-resistant local image tag."""
    revision = _git("rev-parse", "HEAD", root=root).decode().strip()
    tracked_diff = _git(
        "diff",
        "--binary",
        "--no-ext-diff",
        "HEAD",
        "--",
        *IMAGE_SOURCE_PATHS,
        root=root,
    )
    untracked = _git(
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
        "--",
        *IMAGE_SOURCE_PATHS,
        root=root,
    ).split(b"\0")
    untracked = sorted(path for path in untracked if path)
    if not tracked_diff and not untracked:
        return revision, revision[:12]

    digest = hashlib.sha256()
    digest.update(revision.encode())
    digest.update(tracked_diff)
    for raw_path in untracked:
        path = root / raw_path.decode()
        digest.update(raw_path)
        digest.update(b"\0")
        digest.update(path.read_bytes())
    dirty = digest.hexdigest()[:12]
    return f"{revision}+dirty.{dirty}", f"{revision[:12]}-dirty-{dirty}"


def model_digest(manifest_path: Path = MODEL_MANIFEST) -> str:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Missing exported model manifest: {manifest_path}. "
            "Run make app-export-model first."
        ) from exc
    value = manifest.get("model_digest", "")
    if not SHA256.fullmatch(value):
        raise SystemExit(f"Invalid model_digest in {manifest_path}: {value!r}")
    return value


def tree_digest(root: Path) -> str:
    """Hash paths and bytes so every embedded bundle change affects its tag."""
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        contents = path.read_bytes()
        digest.update(len(contents).to_bytes(8, "big"))
        digest.update(contents)
    return f"sha256:{digest.hexdigest()}"


def image_identity(root: Path = ROOT, *, include_model: bool = True) -> dict[str, str]:
    revision, app_tag = source_identity(root)
    values = {
        "FRAUDWATCH_BUILD_REVISION": revision,
        "FRAUDWATCH_APP_TAG": app_tag,
    }
    if not include_model:
        return values
    bundle = root / "artifacts" / "candidate-model"
    digest = model_digest(bundle / "manifest.json")
    bundle_digest = tree_digest(bundle)
    digest_hex = SHA256.fullmatch(digest).group(1)  # validated by model_digest
    bundle_hex = SHA256.fullmatch(bundle_digest).group(1)
    values.update({
        "FRAUDWATCH_MODEL_DIGEST": digest,
        "FRAUDWATCH_MODEL_BUNDLE_DIGEST": bundle_digest,
        "FRAUDWATCH_SCORING_TAG": (
            f"{app_tag}-model-{digest_hex[:12]}-bundle-{bundle_hex[:12]}"
        ),
    })
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format", choices=("shell", "json"), default="shell"
    )
    parser.add_argument(
        "--source-only", action="store_true",
        help="omit model-derived values for model-independent images",
    )
    args = parser.parse_args()
    values = image_identity(include_model=not args.source_only)
    if args.format == "json":
        print(json.dumps(values, indent=2, sort_keys=True))
        return
    for key, value in values.items():
        print(f"export {key}={shlex.quote(value)}")


if __name__ == "__main__":
    main()
