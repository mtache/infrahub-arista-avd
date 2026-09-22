import json
from pathlib import Path

import yaml
from invoke import Context
from pytest import MonkeyPatch

import tasks

ROOT = Path(__file__).parents[2]
PLAYBOOK_PATH = ROOT / "ansible" / "test.yml"
INVENTORY_PATH = ROOT / "ansible" / "inventory.yml"


def _playbook() -> list[dict[str, object]]:
    content = yaml.safe_load(PLAYBOOK_PATH.read_text(encoding="utf-8"))
    assert isinstance(content, list)
    return content


def _named_tasks(play: dict[str, object], section: str) -> dict[str, dict[str, object]]:
    task_list = play[section]
    assert isinstance(task_list, list)
    return {str(task["name"]): task for task in task_list}


def test_infrahub_inventory_is_main_only_and_exposes_anta_fields() -> None:
    inventory = yaml.safe_load(INVENTORY_PATH.read_text(encoding="utf-8"))

    assert inventory["branch"] == "main"
    assert "token" not in inventory
    assert inventory["hostnames"] == ["name"]
    includes = inventory["nodes"]["DcimDevice"]["include"]
    assert "mgmt_ip.address" in includes
    assert "pod.parent.name" in includes
    assert inventory["compose"]["infrahub_fabric_name"] == "pod.parent.name"
    assert "mgmt_ip.address" in inventory["compose"]["ansible_host"]


def test_playbook_requires_exact_fabric_scope_and_valid_target_inventory() -> None:
    selection_play = _playbook()[0]
    tasks_by_name = _named_tasks(selection_play, "tasks")

    require_fabric = tasks_by_name["Require a fabric name"]["ansible.builtin.assert"]
    assert require_fabric["that"] == [
        "fabric_name is defined",
        "fabric_name | string | trim | length > 0",
    ]

    add_hosts = tasks_by_name["Add devices from the requested fabric to the ANTA target group"]
    assert add_hosts["when"] == "hostvars[item].infrahub_fabric_name | default('') == anta_selected_fabric"
    assert add_hosts["ansible.builtin.add_host"]["groups"] == "infrahub_anta_targets"

    require_devices = tasks_by_name["Require matching devices"]["ansible.builtin.assert"]
    assert require_devices["that"] == ["groups['infrahub_anta_targets'] | default([]) | length > 0"]

    require_inventory = tasks_by_name["Require inventory identifiers and management addresses"]
    assert require_inventory["ansible.builtin.assert"]["that"] == [
        "hostvars[item].id is defined",
        "hostvars[item].id | string | length > 0",
        "hostvars[item].ansible_host is defined",
        "hostvars[item].ansible_host | string | length > 0",
    ]


def test_credentials_support_manual_precedence_and_explicit_empty_password() -> None:
    execution_play = _playbook()[1]
    tasks_by_name = _named_tasks(execution_play, "pre_tasks")

    require_credentials = tasks_by_name["Require ANTA credentials from Semaphore or the environment"]
    credential_checks = require_credentials["ansible.builtin.assert"]["that"]
    assert "anta_user is defined" in credential_checks[0]
    assert "ANTA_USER" in credential_checks[0]
    assert "anta_password is defined" in credential_checks[1]
    assert "ANTA_PASSWORD" in credential_checks[1]
    assert "length > 0" not in credential_checks[1]
    assert require_credentials["no_log"] is True

    resolve_credentials = tasks_by_name["Resolve ANTA credentials for each target"]
    facts = resolve_credentials["ansible.builtin.set_fact"]
    assert facts["anta_user"].startswith("{{ anta_user if anta_user is defined")
    assert facts["anta_password"].startswith("{{ anta_password if anta_password is defined")
    assert resolve_credentials["no_log"] is True


def test_artifacts_are_fetched_from_main_and_rejected_when_not_populated() -> None:
    execution_play = _playbook()[1]
    tasks_by_name = _named_tasks(execution_play, "pre_tasks")

    fetch = tasks_by_name["Fetch the merged ANTA catalog from Infrahub main"]
    module = fetch["opsmill.infrahub.artifact_fetch"]
    assert module["artifact_name"] == "{{ anta_artifact_name }}"
    assert module["target_id"] == "{{ id }}"
    assert module["branch"] == "main"

    require_catalog = tasks_by_name["Require a populated ANTA catalog"]["ansible.builtin.assert"]
    assert require_catalog["that"] == [
        "anta_catalog_artifact.text is defined",
        "(anta_catalog_artifact.text | regex_search('(?m)^anta\\.tests\\.[^:]+:')) is not none",
    ]


def test_anta_role_uses_user_catalogs_and_propagates_all_failures() -> None:
    execution_play = _playbook()[1]
    outer_task = execution_play["tasks"][0]
    role_task = outer_task["block"][0]

    assert role_task["ansible.builtin.import_role"]["name"] == "arista.avd.anta_runner"
    assert role_task["vars"]["avd_catalogs_enabled"] is False
    assert role_task["vars"]["user_catalogs_enabled"] is True
    assert role_task["vars"]["anta_runner_tags"] == "{{ groups['infrahub_anta_targets'] }}"
    assert "ignore_errors" not in role_task
    assert "failed_when" not in role_task
    assert "rescue" not in outer_task

    report_task = outer_task["always"][0]
    report_values = report_task["ansible.builtin.debug"]["msg"]
    assert set(report_values) == {"summary", "json_report", "markdown_report", "csv_report"}


def test_semaphore_environment_and_compose_never_seed_device_credentials() -> None:
    payload = tasks._anta_environment_payload(7, "/opt/semaphore/clab-staging/anta")

    assert payload["name"] == "ANTA"
    assert payload["project_id"] == 7
    assert json.loads(str(payload["json"])) == {
        "fabric_name": "",
        "anta_workspace": "/opt/semaphore/clab-staging/anta",
    }
    assert "anta_user" not in str(payload)
    assert "anta_password" not in str(payload)

    compose = yaml.safe_load((ROOT / "docker-compose.override.yml").read_text(encoding="utf-8"))
    environment = compose["services"]["semaphore"]["environment"]
    assert "ANTA_USER" in environment and environment["ANTA_USER"] is None
    assert "ANTA_PASSWORD" in environment and environment["ANTA_PASSWORD"] is None
    forwarded = json.loads(environment["SEMAPHORE_FORWARDED_ENV_VARS"])
    assert {"ANTA_USER", "ANTA_PASSWORD"}.issubset(forwarded)


def test_init_semaphore_registers_anta_with_the_infrahub_inventory(monkeypatch: MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeSemaphoreClient:
        def __init__(self, _base_url: str) -> None:
            pass

        def wait_until_ready(self) -> None:
            pass

        def login(self, _admin: str, _password: str) -> None:
            pass

        def find_or_create(
            self,
            _list_url: str,
            _create_url: str,
            name: str,
            payload: dict[str, object],
        ) -> int:
            calls.append((name, payload))
            return len(calls)

    monkeypatch.setattr(tasks, "_SemaphoreClient", FakeSemaphoreClient)
    monkeypatch.setattr(tasks, "ensure_clab_staging_dir", lambda: ROOT / "lab" / "clab-staging")
    monkeypatch.setattr(tasks, "_semaphore_staging_host_path", lambda *_args: "/host/clab-staging")

    tasks.init_semaphore.body(Context())

    resources = dict(calls)
    resource_ids = {name: index for index, (name, _payload) in enumerate(calls, start=1)}
    anta_environment = resources["ANTA"]
    assert "anta_user" not in str(anta_environment)
    assert "anta_password" not in str(anta_environment)

    anta_template = resources["Validate with ANTA"]
    assert anta_template["playbook"] == "test.yml"
    assert anta_template["inventory_id"] == resource_ids["Infrahub"]
    assert anta_template["environment_id"] == resource_ids["ANTA"]


def test_anta_dependencies_are_pinned_to_avd_630() -> None:
    requirements = yaml.safe_load((ROOT / "ansible" / "galaxy-requirements.yml").read_text(encoding="utf-8"))
    versions = {item["name"]: str(item["version"]) for item in requirements["collections"]}

    assert versions["opsmill.infrahub"] == "1.9.0"
    assert versions["arista.avd"] == "6.3.0"
    dockerfile = (ROOT / "semaphore" / "Dockerfile").read_text(encoding="utf-8")
    assert '"pyavd[ansible]==6.3.0"' in dockerfile
    assert "arista.avd:6.3.0" in dockerfile
