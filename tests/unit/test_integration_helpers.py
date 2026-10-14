"""Offline regressions for bounded integration reads and storage pressure."""

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from infrahub_sdk.exceptions import GraphQLError

if TYPE_CHECKING:
    from pathlib import Path

from tests.integration.helpers import read_with_retry, storage_content, wait_until
from tests.integration.test_seed_selection import _branch_agnostic_seed_kinds


def test_seed_introspection_includes_profiles_and_templates() -> None:
    document = {
        "nodes": [{"namespace": "Network", "name": "Fabric", "branch": "aware"}],
        "profiles": [{"namespace": "Profile", "name": "DcimInterface", "branch": "agnostic"}],
        "templates": [{"namespace": "Template", "name": "DcimDevice", "branch": "agnostic"}],
    }
    assert _branch_agnostic_seed_kinds(document, {"NetworkFabric", "ProfileDcimInterface", "TemplateDcimDevice"}) == [
        "ProfileDcimInterface",
        "TemplateDcimDevice",
    ]


@pytest.mark.asyncio
async def test_artifact_poll_rejects_stale_storage_without_downloading_contents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    def artifact(identifier: str, status: str = "Ready") -> dict:
        return {
            "CoreArtifact": {
                "edges": [
                    {
                        "node": {
                            "status": {"value": status},
                            "storage_id": {"value": identifier},
                            "object": {"node": {"display_label": "leaf-1"}},
                        }
                    }
                ]
            }
        }

    client = Mock()
    client.execute_graphql = AsyncMock(side_effect=[artifact("old"), artifact("new", "Pending"), artifact("new")])
    client.object_store.get = AsyncMock()
    monkeypatch.setattr(pipeline, "POLL_INTERVAL", 0.001)
    assert await pipeline._wait_for_new_artifact_storage(client, "anta", "ANTA", {"leaf-1"}, {"leaf-1": "old"}) == {
        "leaf-1": "new"
    }
    assert client.execute_graphql.await_count == 3
    client.object_store.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_task_failures_are_immediate_and_prior_failures_are_ignored() -> None:
    from tests.integration.test_e2e_pipeline import _execution_tasks

    previous = Mock(id="prior", state="FAILED")
    current = Mock(id="current", state="RUNNING")
    client = Mock()
    client.task.filter = AsyncMock(return_value=[previous, current])
    client.task.get = AsyncMock(return_value="failure detail")
    assert await _execution_tasks(client, "scenario", {"prior"}) == [current]
    current.state = "FAILED"
    with pytest.raises(AssertionError, match="failure detail"):
        await _execution_tasks(client, "scenario", {"prior"})
    client.task.get.assert_awaited_once_with("current", include_logs=True)


@pytest.mark.asyncio
async def test_nested_generator_without_branch_is_part_of_the_new_execution() -> None:
    from tests.integration.test_e2e_pipeline import _execution_tasks, _pipeline_tasks

    old = Mock(id="old", branch=None, state="FAILED")
    root = Mock(id="root", branch="scenario", state="COMPLETED")
    child = Mock(id="child", branch=None, state="RUNNING")
    foreign = Mock(id="foreign", branch="other-scenario", state="FAILED")
    client = Mock()
    client.task.filter = AsyncMock(side_effect=[[], [old], [root], [old, child, foreign]])
    before = {task.id for task in await _pipeline_tasks(client, "scenario")}
    assert await _execution_tasks(client, "scenario", before) == [root, child]
    child.state = "FAILED"
    client.task.filter.side_effect = [[root], [old, child, foreign]]
    client.task.get = AsyncMock(return_value="nested failure detail")
    with pytest.raises(AssertionError, match="nested failure detail"):
        await _execution_tasks(client, "scenario", before)
    client.task.get.assert_awaited_once_with("child", include_logs=True)


@pytest.mark.asyncio
async def test_task_metadata_pages_do_not_truncate_or_request_task_counts() -> None:
    from infrahub_sdk.task import TaskFilter

    from tests.integration.test_e2e_pipeline import _task_pages

    tasks = [Mock(id=str(index)) for index in range(201)]
    client = Mock()
    client.task.filter = AsyncMock(side_effect=[tasks[:200], tasks[200:]])
    assert await _task_pages(client, TaskFilter(branch="scenario")) == tasks
    assert [call.kwargs["offset"] for call in client.task.filter.await_args_list] == [0, 200]
    assert all(
        call.kwargs["limit"] == 200 and call.kwargs["timeout"] == 15 for call in client.task.filter.await_args_list
    )


@pytest.mark.asyncio
async def test_fabric_kickoffs_preserve_every_target_and_wait_for_each_cascade(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    client = Mock()
    client.get = AsyncMock(return_value=Mock(id="generator"))
    client.all = AsyncMock(
        return_value=[
            SimpleNamespace(id=identifier, name=SimpleNamespace(value=identifier)) for identifier in ["B", "A"]
        ]
    )
    events = []

    async def run(query: str, variables: dict, branch_name: str) -> dict:
        assert "CoreGeneratorDefinitionRun" in query and branch_name == pipeline.PIPELINE_BRANCH
        events.append(("run", variables["nodes"]))
        return {"CoreGeneratorDefinitionRun": {"ok": True}}

    async def settled(client: object, branch: str, before: set[str]) -> None:
        assert client is not None and branch == pipeline.PIPELINE_BRANCH and before == set()
        events.append(("settled", None))

    client.execute_graphql = AsyncMock(side_effect=run)
    monkeypatch.setattr(pipeline, "_pipeline_tasks", AsyncMock(return_value=[]))
    monkeypatch.setattr(pipeline, "_wait_for_tasks", settled)
    monkeypatch.setattr(pipeline, "expected_super_spine_count", AsyncMock(return_value=1))
    monkeypatch.setattr(pipeline, "wait_until", AsyncMock(return_value=[Mock()]))
    await pipeline.TestE2EPipeline()._prepare_fabric(client)
    assert events == [("run", ["A"]), ("settled", None), ("run", ["B"]), ("settled", None)]


@pytest.mark.asyncio
async def test_poll_has_a_deadline_even_when_fetch_never_returns() -> None:
    async def blocked() -> None:
        await asyncio.Event().wait()

    with pytest.raises(AssertionError, match="timed out"):
        await wait_until(blocked, bool, timeout=0.02, interval=1, describe="blocked read")


@pytest.mark.asyncio
async def test_inner_read_deadline_reports_its_actual_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration import helpers

    monkeypatch.setattr(helpers, "READ_TIMEOUT", 0.02)

    async def blocked() -> None:
        await asyncio.Event().wait()

    with pytest.raises(AssertionError, match=r"integration read timed out after 0\.02s"):
        await wait_until(blocked, bool, timeout=1, interval=1, describe="longer poll")


@pytest.mark.asyncio
async def test_grouped_design_parity_paginates_without_per_node_reads() -> None:
    from tests.integration.helpers import device_design_mismatches

    def collection(nodes: list[dict], count: int | None = None) -> dict:
        return {"count": len(nodes) if count is None else count, "edges": [{"node": node} for node in nodes]}

    def page(offset: int) -> dict:
        first = offset == 0
        return {
            "NetworkFabric": collection(
                [{"id": "fabric", "underlay_routing_protocol": {"value": "ebgp"}}] if first else []
            ),
            "NetworkPod": collection(
                [
                    {
                        "id": "pod",
                        "name": {"value": "Pod"},
                        "parent": {"node": {"__typename": "NetworkFabric", "id": "fabric"}},
                    }
                ]
                if first
                else []
            ),
            "LocationRack": collection(
                [{"id": "rack", "name": {"value": "Rack"}, "pod": {"node": {"id": "pod"}}}] if first else []
            ),
            "NetworkPodDeviceDesign": collection([]),
            "NetworkRackDeviceDesign": collection(
                [{"role": {"value": "leaf"}, "device_quantity": {"value": 201}, "rack": {"node": {"id": "rack"}}}]
                if first
                else []
            ),
            "DcimDevice": collection(
                [
                    {
                        "id": str(index),
                        "role": {"value": "leaf"},
                        "rack": {"node": {"id": "rack"}},
                        "pod": {"node": {"id": "pod"}},
                    }
                    for index in range(offset, min(offset + 200, 201))
                ],
                count=201,
            ),
        }

    client = Mock()
    client.execute_graphql = AsyncMock(side_effect=[page(0), page(200)])
    assert await device_design_mismatches(client, "baseline") == []
    assert [call.kwargs["variables"]["offset"] for call in client.execute_graphql.await_args_list] == [0, 200]
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_poll_propagates_permanent_graphql_errors() -> None:
    fetch = AsyncMock(side_effect=GraphQLError(errors=[{"message": "At most, one relationship expected"}]))
    with pytest.raises(GraphQLError, match="one relationship"):
        await wait_until(fetch, bool, timeout=1, interval=1, describe="cardinality")
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_transient_read_respects_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    response = httpx.Response(429, headers={"Retry-After": "9"}, request=httpx.Request("GET", "http://test/content"))
    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)
    fetch = AsyncMock(side_effect=[httpx.HTTPStatusError("limited", request=response.request, response=response), "ok"])
    assert await read_with_retry(fetch) == "ok"
    sleep.assert_awaited_once_with(9)


@pytest.mark.asyncio
async def test_graphql_load_shedding_is_a_bounded_transient_read(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)
    shedding = GraphQLError(errors=[{"message": "Server is shedding load", "extensions": {"code": 429}}])
    fetch = AsyncMock(side_effect=[shedding, "ok"])
    assert await read_with_retry(fetch) == "ok"
    sleep.assert_awaited_once_with(4)
    fetch = AsyncMock(side_effect=shedding)
    with pytest.raises(GraphQLError):
        await read_with_retry(fetch)
    assert fetch.await_count == 4


@pytest.mark.asyncio
async def test_graphql_transient_and_permanent_errors_together_are_not_retried() -> None:
    fetch = AsyncMock(
        side_effect=GraphQLError(
            errors=[
                {"message": "shedding", "extensions": {"code": 429}},
                {"message": "At most, one relationship expected"},
            ]
        )
    )
    with pytest.raises(GraphQLError, match="one relationship"):
        await read_with_retry(fetch)
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_storage_cache_deduplicates_and_limits_concurrency() -> None:
    active = peak = 0

    async def download(identifier: str) -> str:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return identifier

    client = Mock()
    client.object_store.get = AsyncMock(side_effect=download)
    identifiers = ["same", "same", "b", "c", "d"]
    assert await asyncio.gather(*(storage_content(client, identifier) for identifier in identifiers)) == identifiers
    assert client.object_store.get.await_count == 4
    assert peak == 2
    assert await storage_content(client, "same") == "same"
    assert client.object_store.get.await_count == 4


def test_failed_container_startup_still_preserves_logs_and_tears_down(tmp_path: Path) -> None:
    from tests.integration.stack import IntegrationStack

    compose = Mock()
    compose.start.side_effect = RuntimeError("startup timeout")
    compose.get_logs.return_value = ("startup logs", "stderr")
    fixture = IntegrationStack.infrahub_app.__wrapped__(IntegrationStack(), compose, tmp_path)
    with pytest.raises(RuntimeError, match="startup timeout"):
        next(fixture)
    compose.stop.assert_called_once()
    assert (tmp_path / "instance.log").read_text() == "startup logs\nstderr"


@pytest.mark.asyncio
async def test_dci_targets_only_the_explicit_fabric_in_deterministic_order() -> None:
    from tests.integration.test_e2e_pipeline import _dci_leaf_candidates

    def device(name: str, fabric: str) -> dict:
        return {
            "node": {
                "id": name,
                "name": {"value": name},
                "asn": {"node": {"asn": {"value": 65101}}},
                "pod": {"node": {"parent": {"node": {"id": fabric, "name": {"value": fabric}}}}},
            }
        }

    client = Mock()
    client.execute_graphql = AsyncMock(
        return_value={
            "DcimDevice": {
                "edges": [
                    device("foreign", "Fabric-L3LS-MultiPod-B"),
                    device("leaf-2", "Fabric-L3LS-MultiPod-A"),
                    device("leaf-1", "Fabric-L3LS-MultiPod-A"),
                ]
            }
        }
    )
    candidates = await _dci_leaf_candidates(client, "dci")
    assert [device["device_name"] for device in candidates] == ["leaf-1", "leaf-2"]


@pytest.mark.asyncio
async def test_dci_never_replaces_an_existing_pool() -> None:
    from tests.integration.test_e2e_pipeline import _ensure_dci_pool

    client = Mock()
    client.get = AsyncMock(return_value=Mock(dci_pool=Mock(id="existing")))
    client.create = AsyncMock()
    with pytest.raises(AssertionError, match="existing DCI pool"):
        await _ensure_dci_pool(client, "dci", "fabric")
    client.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_structured_config_readiness_counts_devices_without_an_artifact() -> None:
    from tests.integration.test_e2e_pipeline import _devices_without_structured_config

    client = Mock()
    client.execute_graphql = AsyncMock(
        return_value={
            "DcimDevice": {
                "edges": [
                    {
                        "node": {
                            "name": {"value": "unready"},
                            "role": {"value": "leaf"},
                            "avd_artifact": {"node": None},
                        }
                    }
                ]
            }
        }
    )
    assert await _devices_without_structured_config(client, "baseline") == {"total": 1, "without": ["unready (leaf)"]}


@pytest.mark.asyncio
async def test_poll_does_not_retry_pending_mutations() -> None:
    response = httpx.Response(429, request=httpx.Request("POST", "http://test/generate"))
    action = AsyncMock(side_effect=httpx.HTTPStatusError("limited", request=response.request, response=response))
    fetch = AsyncMock(return_value=False)
    with pytest.raises(httpx.HTTPStatusError):
        await wait_until(fetch, bool, timeout=1, interval=1, describe="generation", on_pending=action)
    action.assert_awaited_once()
    fetch.assert_awaited_once()


def test_stack_makes_only_public_bind_configs_readable(tmp_path: Path) -> None:
    from tests.integration.stack import IntegrationStack

    for name in ("haproxy.cfg", "prometheus.yml", ".env"):
        config = tmp_path / name
        config.write_text("configuration")
        config.chmod(0o600)
    compose = Mock()
    compose.start.side_effect = RuntimeError("startup timeout")
    compose.get_logs.return_value = ("", "")
    fixture = IntegrationStack.infrahub_app.__wrapped__(IntegrationStack(), compose, tmp_path)
    with pytest.raises(RuntimeError, match="startup timeout"):
        next(fixture)
    assert (tmp_path / "haproxy.cfg").stat().st_mode & 0o777 == 0o644
    assert (tmp_path / "prometheus.yml").stat().st_mode & 0o777 == 0o644
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600
