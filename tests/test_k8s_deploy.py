from pathlib import Path

import pytest

from scripts.deploy_k8s import load_env_keys, timeout_seconds
from scripts.init_k8s_secrets import ensure, initialize


def test_generated_secret_file_is_private_and_valid(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)

    values = load_env_keys(path)

    assert path.stat().st_mode & 0o777 == 0o600
    assert values["POSTGRES_PASSWORD"] in values["DATABASE_URL"]
    assert "default_transaction_read_only%3Don" in values["DATABASE_URL_READ_ONLY"]
    assert len(values["GRAFANA_ADMIN_PASSWORD"]) >= 24


def test_secret_initializer_refuses_to_overwrite(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)

    with pytest.raises(FileExistsError):
        initialize(path)


def test_secret_ensure_upgrades_without_rotating_database_password(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)
    original = path.read_text(encoding="utf-8")
    without_grafana = "\n".join(
        line
        for line in original.splitlines()
        if not line.startswith("GRAFANA_ADMIN_PASSWORD=")
    ) + "\n"
    path.write_text(without_grafana, encoding="utf-8")

    assert ensure(path) is True
    upgraded = load_env_keys(path)
    assert upgraded["POSTGRES_PASSWORD"] in original
    assert len(upgraded["GRAFANA_ADMIN_PASSWORD"]) >= 24
    assert path.stat().st_mode & 0o777 == 0o600
    assert ensure(path) is False


def test_secret_validation_rejects_example_placeholders(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    path.write_text(
        "\n".join(
            (
                "POSTGRES_DB=fraudwatch",
                "POSTGRES_USER=fraudwatch",
                "POSTGRES_PASSWORD=replace-with-a-local-password",
                "DATABASE_URL=postgres://example",
                "DATABASE_URL_READ_ONLY=postgres://example?default_transaction_read_only=on",
                "GRAFANA_ADMIN_PASSWORD=replace-with-a-local-password",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="placeholder"):
        load_env_keys(path)


def test_kubernetes_timeout_parser_is_explicit():
    assert timeout_seconds("90s") == 90
    assert timeout_seconds("5m") == 300
    with pytest.raises(RuntimeError, match="Unsupported"):
        timeout_seconds("1h")
