import json
import secrets
from collections.abc import Callable

import httpx
import pytest

import tasks


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> tasks._SemaphoreClient:
    client = tasks._SemaphoreClient("http://semaphore.test")
    client._client.close()
    client._client = httpx.Client(base_url="http://semaphore.test", transport=httpx.MockTransport(handler))
    return client


def test_anta_environment_payload_does_not_store_device_password() -> None:
    payload = tasks._anta_environment_payload(7, "/workspace/anta")

    assert payload["name"] == "ANTA"
    assert payload["project_id"] == 7
    assert json.loads(str(payload["json"])) == {
        "fabric_name": "Fabric-L3LS-Multi-Domain",
        "anta_user": "admin",
    }
    assert json.loads(str(payload["env"])) == {
        "INFRAHUB_BRANCH": "main",
        "ANTA_WORKSPACE": "/workspace/anta",
    }


def test_remove_environment_extra_vars_removes_password_and_preserves_other_values(
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_value = secrets.token_urlsafe(32)
    prefix_value = "anta/"
    updates: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "id": 11,
                    "name": "ANTA",
                    "project_id": 7,
                    "json": json.dumps(
                        {
                            "fabric_name": "Fabric-L3LS-Multi-Domain",
                            "anta_user": "admin",
                            "anta_password": secret_value,
                            "operator_override": "preserved",
                        }
                    ),
                    "env": json.dumps({"INFRAHUB_BRANCH": "validation"}),
                    "secret_storage_id": 3,
                    "secret_storage_key_prefix": prefix_value,
                },
            )
        updates.append(json.loads(request.content))
        return httpx.Response(204)

    client = _client(handler)
    removed = client.remove_environment_extra_vars(7, 11, {"anta_password"})

    assert removed == ("anta_password",)
    assert len(updates) == 1
    assert json.loads(str(updates[0]["json"])) == {
        "fabric_name": "Fabric-L3LS-Multi-Domain",
        "anta_user": "admin",
        "operator_override": "preserved",
    }
    assert json.loads(str(updates[0]["env"])) == {"INFRAHUB_BRANCH": "validation"}
    assert updates[0]["secret_storage_id"] == 3
    assert updates[0]["secret_storage_key_prefix"] == prefix_value
    assert secret_value not in capsys.readouterr().out


def test_remove_environment_extra_vars_is_idempotent() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(
            200,
            json={
                "id": 11,
                "name": "ANTA",
                "project_id": 7,
                "json": json.dumps({"fabric_name": "Fabric-L3LS-Multi-Domain", "anta_user": "admin"}),
                "env": "{}",
            },
        )

    client = _client(handler)

    assert client.remove_environment_extra_vars(7, 11, {"anta_password"}) == ()
    assert methods == ["GET"]


def test_compose_forwards_only_an_empty_external_anta_password() -> None:
    compose_text = (tasks.MAIN_DIRECTORY_PATH / "docker-compose.override.yml").read_text()

    assert 'SEMAPHORE_FORWARDED_ENV_VARS: \'["INFRAHUB_ADDRESS","INFRAHUB_API_TOKEN","ANTA_PASSWORD"]\'' in compose_text
    assert "ANTA_PASSWORD: ${ANTA_PASSWORD:-}" in compose_text
    assert "ANTA_PASSWORD: admin" not in compose_text
