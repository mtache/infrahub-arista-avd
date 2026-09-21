import json
import secrets
from collections.abc import Callable

import httpx
import pytest
import yaml

import tasks

ANTA_PLAYBOOK_PATH = tasks.MAIN_DIRECTORY_PATH / "ansible" / "test.yml"


def _anta_playbook() -> list[dict[str, object]]:
    playbook = yaml.safe_load(ANTA_PLAYBOOK_PATH.read_text(encoding="utf-8"))
    assert isinstance(playbook, list)
    return playbook


def _named_tasks(play: dict[str, object], section: str) -> dict[str, dict[str, object]]:
    task_list = play[section]
    assert isinstance(task_list, list)
    return {str(task["name"]): task for task in task_list}


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


def test_anta_playbook_fails_closed_for_invalid_fabric_or_inventory() -> None:
    discovery_play = _anta_playbook()[0]
    tasks_by_name = _named_tasks(discovery_play, "tasks")

    require_fabric = tasks_by_name["Require fabric_name"]["ansible.builtin.assert"]
    assert require_fabric["that"] == ["fabric_name is defined", "fabric_name | length > 0"]

    require_match = tasks_by_name["Require exactly one matching fabric"]["ansible.builtin.assert"]
    assert require_match["that"] == ["fabric_lookup.response.NetworkFabric.edges | length == 1"]

    require_devices = tasks_by_name["Require matching devices"]["ansible.builtin.assert"]
    assert require_devices["that"] == ["infrahub_anta_devices | length > 0"]

    require_address = tasks_by_name["Require a management address on every ANTA device"]["ansible.builtin.assert"]
    assert require_address["that"] == [
        "item.mgmt_ip is not none",
        "item.mgmt_ip.node is not none",
        "item.mgmt_ip.node.address.value | length > 0",
    ]


def test_anta_playbook_fails_before_writes_for_missing_credentials_or_catalogs() -> None:
    execution_play = _anta_playbook()[1]
    play_vars = execution_play["vars"]
    expected_lookup = "{{ lookup('env', 'ANTA_PASSWORD') }}"
    assert play_vars["anta_password"] == expected_lookup

    pre_tasks = execution_play["pre_tasks"]
    assert [task["name"] for task in pre_tasks[:2]] == ["Require ANTA credentials", "Recreate ANTA run directory"]
    tasks_by_name = _named_tasks(execution_play, "pre_tasks")

    require_credentials = tasks_by_name["Require ANTA credentials"]["ansible.builtin.assert"]
    assert require_credentials["that"] == [
        "anta_user is defined",
        "anta_user | length > 0",
        "anta_password is defined",
        "anta_password | length > 0",
    ]

    require_catalog = tasks_by_name["Require a populated ANTA catalog"]["ansible.builtin.assert"]
    assert require_catalog["that"] == [
        "anta_catalog_artifact.text is defined",
        "(anta_catalog_artifact.text | regex_search('(?m)^anta\\\\.tests\\\\.[^:]+:')) is not none",
    ]
