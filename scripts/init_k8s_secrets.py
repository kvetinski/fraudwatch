#!/usr/bin/env python3
"""Create or safely extend a mode-0600 local Kubernetes secret env file."""

from __future__ import annotations

import argparse
import os
import secrets
from pathlib import Path
from urllib.parse import quote


def initialize(path: Path) -> None:
    password = secrets.token_hex(24)
    grafana_password = secrets.token_urlsafe(24)
    encoded_password = quote(password, safe="")
    database_url = (
        f"postgres://fraudwatch:{encoded_password}"
        "@postgres:5432/fraudwatch?sslmode=disable"
    )
    contents = "\n".join(
        (
            "POSTGRES_DB=fraudwatch",
            "POSTGRES_USER=fraudwatch",
            f"POSTGRES_PASSWORD={password}",
            f"DATABASE_URL={database_url}",
            f"DATABASE_URL_READ_ONLY={database_url}"
            "&options=-cdefault_transaction_read_only%3Don",
            f"GRAFANA_ADMIN_PASSWORD={grafana_password}",
            "",
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(contents)


def ensure(path: Path) -> bool:
    """Create the file, or add newly required local secrets without rotating it."""
    if not path.exists():
        initialize(path)
        return True

    contents = path.read_text(encoding="utf-8")
    keys = {
        line.split("=", 1)[0].strip()
        for line in contents.splitlines()
        if line.strip() and not line.lstrip().startswith("#") and "=" in line
    }
    if "GRAFANA_ADMIN_PASSWORD" in keys:
        os.chmod(path, 0o600)
        return False

    separator = "" if not contents or contents.endswith("\n") else "\n"
    updated = (
        contents
        + separator
        + f"GRAFANA_ADMIN_PASSWORD={secrets.token_urlsafe(24)}\n"
    )
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(updated)
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    changed = ensure(args.path)
    action = "Created or upgraded" if changed else "Already current"
    print(f"{action}: {args.path} (mode 0600)")


if __name__ == "__main__":
    main()
