import os
import secrets
import stat
from pathlib import Path
from uuid import UUID

import pytest
from dotenv import dotenv_values
from invoke import Context

import tasks

SECRET_NAMES = {
    "INFRAHUB_INITIAL_ADMIN_PASSWORD",
    "INFRAHUB_INITIAL_ADMIN_TOKEN",
    "INFRAHUB_API_TOKEN",
    "INFRAHUB_SECURITY_SECRET_KEY",
    "SEMAPHORE_ADMIN_PASSWORD",
}


def test_initialize_secrets_generates_strong_values_with_secure_permissions(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"

    generated = set(tasks._initialize_secrets(env_path))
    values = dotenv_values(env_path)

    assert generated == SECRET_NAMES
    assert set(values) == SECRET_NAMES
    assert all(
        values[name] is not None and len(values[name] or "") >= 40
        for name in ("INFRAHUB_INITIAL_ADMIN_PASSWORD", "INFRAHUB_SECURITY_SECRET_KEY", "SEMAPHORE_ADMIN_PASSWORD")
    )
    assert UUID(values["INFRAHUB_INITIAL_ADMIN_TOKEN"] or "").version == 4
    assert values["INFRAHUB_INITIAL_ADMIN_TOKEN"] == values["INFRAHUB_API_TOKEN"]
    assert stat.S_IMODE(env_path.stat().st_mode) == 0o600


def test_initialize_secrets_is_idempotent_and_preserves_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    preserved_value = "already-configured-locally"
    env_path.write_text(f"INFRAHUB_INITIAL_ADMIN_PASSWORD={preserved_value}\n")

    first_generated = tasks._initialize_secrets(env_path)
    first_content = env_path.read_bytes()
    first_mtime = env_path.stat().st_mtime_ns
    second_generated = tasks._initialize_secrets(env_path)

    assert "INFRAHUB_INITIAL_ADMIN_PASSWORD" not in first_generated
    assert dotenv_values(env_path)["INFRAHUB_INITIAL_ADMIN_PASSWORD"] == preserved_value
    assert second_generated == ()
    assert env_path.read_bytes() == first_content
    assert env_path.stat().st_mtime_ns == first_mtime


def test_load_environment_preserves_exported_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_path = tmp_path / ".env"
    file_value = secrets.token_urlsafe(32)
    exported_value = secrets.token_urlsafe(32)
    env_path.write_text(f"SEMAPHORE_ADMIN_PASSWORD={file_value}\n")
    monkeypatch.setenv("SEMAPHORE_ADMIN_PASSWORD", exported_value)

    tasks._load_environment(env_path)

    assert os.environ["SEMAPHORE_ADMIN_PASSWORD"] == exported_value


def test_init_secrets_output_is_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(tasks, "ENV_FILE_PATH", tmp_path / ".env")

    tasks.init_secrets.body(Context())
    output = capsys.readouterr().out
    values = dotenv_values(tmp_path / ".env")

    assert all(value not in output for value in values.values() if value)
    assert "Credential values were not displayed" in output


def test_compose_credentials_are_required_environment_variables() -> None:
    expected_counts = {
        "INFRAHUB_INITIAL_ADMIN_PASSWORD": 1,
        "INFRAHUB_INITIAL_ADMIN_TOKEN": 2,
        "INFRAHUB_API_TOKEN": 3,
        "INFRAHUB_SECURITY_SECRET_KEY": 3,
        "SEMAPHORE_ADMIN_PASSWORD": 1,
    }
    actual_counts = dict.fromkeys(expected_counts, 0)

    for filename in ("docker-compose.yml", "docker-compose.override.yml"):
        compose_text = (tasks.MAIN_DIRECTORY_PATH / filename).read_text()
        assert "${INFRAHUB_INITIAL_AGENT_TOKEN:?" not in compose_text
        for line in compose_text.splitlines():
            key, separator, value = line.strip().partition(":")
            if separator and key in expected_counts:
                assert value.strip() == f"${{{key}:?Run uv run invoke init-secrets}}"
                actual_counts[key] += 1

    assert actual_counts == expected_counts


def test_inventory_uses_environment_token_and_example_has_no_secret_values() -> None:
    inventory_lines = (tasks.MAIN_DIRECTORY_PATH / "ansible/inventory.yml").read_text().splitlines()
    assert not any(line.strip().startswith("token:") for line in inventory_lines)

    example_assignments = {
        key: value
        for line in (tasks.MAIN_DIRECTORY_PATH / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
        for key, separator, value in (line.partition("="),)
        if separator
    }
    assert set(example_assignments) == SECRET_NAMES | {
        "CLOUDVISION_SERVERS",
        "CLOUDVISION_TOKEN",
        "CLOUDVISION_VERIFY_CERTS",
    }
    assert not any(example_assignments[name] for name in SECRET_NAMES | {"CLOUDVISION_TOKEN"})
    assert example_assignments["CLOUDVISION_SERVERS"] == "www.cv-prod-euwest-2.arista.io"
    assert example_assignments["CLOUDVISION_VERIFY_CERTS"] == "true"
