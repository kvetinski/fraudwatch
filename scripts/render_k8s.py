#!/usr/bin/env python3
"""Render a Kustomize tree with exact local FraudWatch image identities."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from scripts.image_identity import ROOT, image_identity
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from image_identity import ROOT, image_identity


APP_IMAGES = {
    "fraudwatch/ingestion:development": ("ingestion", "FRAUDWATCH_APP_TAG"),
    "fraudwatch/labels:development": ("labels", "FRAUDWATCH_APP_TAG"),
    "fraudwatch/monitor:development": ("monitor", "FRAUDWATCH_APP_TAG"),
    "fraudwatch/operations:development": ("operations", "FRAUDWATCH_APP_TAG"),
    "fraudwatch/scoring:development": ("scoring", "FRAUDWATCH_SCORING_TAG"),
    "fraudwatch/model-monitor:development": (
        "model-monitor",
        "FRAUDWATCH_SCORING_TAG",
    ),
}


def replace_image_tags(manifest: str, identities: dict[str, str]) -> str:
    rendered = manifest
    for placeholder, (repository, identity_key) in APP_IMAGES.items():
        if placeholder not in rendered:
            continue
        if identity_key not in identities:
            raise RuntimeError(
                f"Manifest needs {identity_key}, but model identity was not loaded"
            )
        rendered = rendered.replace(
            placeholder, f"fraudwatch/{repository}:{identities[identity_key]}"
        )
    unresolved = [image for image in APP_IMAGES if image in rendered]
    if unresolved:
        raise RuntimeError(f"Unresolved application images: {unresolved}")
    return rendered


def render(path: Path, *, root: Path = ROOT, kubectl: str = "kubectl") -> str:
    absolute = path if path.is_absolute() else root / path
    manifest = subprocess.check_output(
        [kubectl, "kustomize", str(absolute)], text=True
    )
    needs_model = any(
        placeholder in manifest
        for placeholder, (_, key) in APP_IMAGES.items()
        if key == "FRAUDWATCH_SCORING_TAG"
    )
    return replace_image_tags(
        manifest, image_identity(root, include_model=needs_model)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rendered = render(args.path)
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)


if __name__ == "__main__":
    main()
