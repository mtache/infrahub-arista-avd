"""Offline regressions for bounded integration reads and storage pressure."""

import asyncio
from copy import deepcopy
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


@pytest.mark.asyncio
async def test_targeted_artifact_generation_uses_artifact_ids_and_keeps_every_device() -> None:
    from tests.integration.test_e2e_pipeline import _artifact_ids_for_targets

    client = Mock(
        execute_graphql=AsyncMock(
            return_value={
                "CoreArtifact": {
                    "edges": [
                        {"node": {"id": "catalog-a", "object": {"node": {"id": "device-a"}}}},
                        {"node": {"id": "catalog-b", "object": {"node": {"id": "device-b"}}}},
                        {"node": {"id": "catalog-c", "object": {"node": {"id": "foreign-device"}}}},
                    ]
                }
            }
        )
    )
    assert await _artifact_ids_for_targets(client, "scenario", "AVD ANTA Catalog", {"device-b", "device-a"}) == [
        "catalog-a",
        "catalog-b",
    ]
    with pytest.raises(AssertionError, match="missing-device"):
        await _artifact_ids_for_targets(client, "scenario", "AVD ANTA Catalog", {"missing-device"})


@pytest.mark.asyncio
async def test_rack_snapshot_reads_all_pages_and_does_not_duplicate_finished_collections() -> None:
    from tests.integration.test_e2e_pipeline import _snapshot_collections

    client = Mock(
        execute_graphql=AsyncMock(
            side_effect=[
                {
                    "CoreGeneratorInstance": {"count": 1, "edges": [{"node": {"id": "generator"}}]},
                    "InterfacePhysical": {"count": 3, "edges": [{"node": {"id": "a"}}]},
                },
                {
                    "CoreGeneratorInstance": {"count": 1, "edges": []},
                    "InterfacePhysical": {
                        "count": 3,
                        "edges": [{"node": {"id": "b"}}, {"node": {"id": "c"}}],
                    },
                },
            ]
        )
    )
    data = await _snapshot_collections(client, "scenario", "query", contents=True)
    assert [edge["node"]["id"] for edge in data["InterfacePhysical"]["edges"]] == ["a", "b", "c"]
    assert len(data["CoreGeneratorInstance"]["edges"]) == 1
    assert client.execute_graphql.await_args_list[1].kwargs["variables"] == {
        "offset": 1,
        "limit": 500,
        "details": True,
    }


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
async def test_artifact_tasks_without_branch_are_also_observed() -> None:
    from tests.integration.test_e2e_pipeline import _execution_tasks

    artifact = Mock(id="artifact", branch=None, state="RUNNING")
    client = Mock()
    client.task.filter = AsyncMock(side_effect=[[], [artifact]])
    assert await _execution_tasks(client, "scenario", set()) == [artifact]
    assert client.task.filter.await_args_list[1].args[0].workflow == [
        "generator-run",
        "artifact-generate",
        "request_artifact_definitions_generate",
    ]


@pytest.mark.asyncio
async def test_artifact_poll_keeps_a_pending_definition_request_visible() -> None:
    from tests.integration.test_e2e_pipeline import _execution_tasks

    request = Mock(id="definition-request", branch=None, state="SCHEDULED")
    client = Mock()
    client.task.filter = AsyncMock(side_effect=[[], [request]])
    assert await _execution_tasks(client, "scenario", set()) == [request]


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
async def test_fabric_kickoffs_preserve_every_target_with_at_most_two_cascades(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    client = Mock()
    client.get = AsyncMock(return_value=Mock(id="generator"))
    client.all = AsyncMock(
        return_value=[
            SimpleNamespace(id=identifier, name=SimpleNamespace(value=identifier)) for identifier in ["C", "B", "A"]
        ]
    )
    events = []

    async def run(query: str, variables: dict, branch_name: str) -> dict:
        assert "CoreGeneratorDefinitionRun" in query and branch_name == pipeline.PIPELINE_BRANCH
        events.append(("run", variables["nodes"]))
        return {"CoreGeneratorDefinitionRun": {"ok": True}}

    async def settled(client: object, branch: str, before: set[str], *, expected_runs: dict[str, int]) -> None:
        assert client is not None and branch == pipeline.PIPELINE_BRANCH and before == set()
        count = len(events[-1][1])
        assert expected_runs == {pipeline.GENERATOR_FABRIC: count, pipeline.GENERATOR_AVD_STRUCTURED_CONFIG: count}
        events.append(("settled", None))

    client.execute_graphql = AsyncMock(side_effect=run)
    monkeypatch.setattr(pipeline, "_pipeline_tasks", AsyncMock(return_value=[]))
    monkeypatch.setattr(pipeline, "_wait_for_tasks", settled)
    monkeypatch.setattr(pipeline, "expected_super_spine_count", AsyncMock(return_value=1))
    monkeypatch.setattr(pipeline, "wait_until", AsyncMock(return_value=[Mock()]))
    await pipeline.TestE2EPipeline()._prepare_fabric(client)
    assert events == [("run", ["A", "B"]), ("settled", None), ("run", ["C"]), ("settled", None)]


@pytest.mark.asyncio
async def test_task_barrier_waits_for_delayed_cascades_and_their_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    def task(identifier: str, name: str, state: str = "COMPLETED") -> SimpleNamespace:
        return SimpleNamespace(id=identifier, title=f"Run generator {name}", workflow="generator-run", state=state)

    rack = task("rack", pipeline.GENERATOR_RACK)
    first = task("first", pipeline.GENERATOR_AVD_STRUCTURED_CONFIG)
    delayed = task("delayed", pipeline.GENERATOR_AVD_STRUCTURED_CONFIG, "RUNNING")
    completed = task("delayed", pipeline.GENERATOR_AVD_STRUCTURED_CONFIG)
    reads = AsyncMock(
        side_effect=[
            [rack, first],
            [rack, first],
            [rack, first],
            [rack, first, delayed],
            [rack, first, completed],
            [rack, first, completed],
        ]
    )
    monkeypatch.setattr(pipeline, "_execution_tasks", reads)

    async def fast_poll(**kwargs: object) -> object:
        return await wait_until(**{**kwargs, "interval": 0.001})

    monkeypatch.setattr(pipeline, "wait_until", fast_poll)
    await pipeline._wait_for_tasks(
        Mock(),
        "scenario",
        {"old"},
        expected_runs={pipeline.GENERATOR_RACK: 1, pipeline.GENERATOR_AVD_STRUCTURED_CONFIG: 2},
    )
    assert reads.await_count == 6


@pytest.mark.asyncio
async def test_rack_snapshot_requires_local_structured_instances_for_every_fabric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    def instance(identifier: str, name: str) -> dict:
        return {
            "node": {
                "id": identifier,
                "name": {"value": identifier},
                "status": {"value": "Ready"},
                "object": {"node": {"id": identifier, "display_label": identifier}},
                "definition": {"node": {"id": name, "name": {"value": name}}},
            }
        }

    instances = [instance("rack", pipeline.GENERATOR_RACK), instance("a", pipeline.GENERATOR_AVD_STRUCTURED_CONFIG)]
    metadata = {
        "CoreGeneratorInstance": {"edges": instances},
        "NetworkFabric": {
            "edges": [
                {"node": {"id": identifier, "name": {"value": identifier}, "avd_hostvars_ready": {"value": True}}}
                for identifier in ("a", "b")
            ]
        },
        "LocationRack": {
            "edges": [{"node": {"id": "rack", "name": {"value": "rack"}, "generation_complete": {"value": True}}}]
        },
        "AvdStructuredConfigFile": {
            "edges": [
                {
                    "node": {
                        "id": "file",
                        "file_name": {"value": "device.json"},
                        "checksum": {"value": "hash"},
                        "file_size": {"value": 100},
                        "storage_id": {"value": "stored"},
                        "artifact": {"node": {"name": {"value": "device"}}},
                    }
                }
            ]
        },
    }
    monkeypatch.setattr(pipeline, "_snapshot_collections", AsyncMock(return_value=metadata))
    first = await pipeline._rack_rerun_snapshot(Mock(), "scenario", contents=False)
    assert not first["ready"] and first["missing_structured_targets"] == ["b"]
    assert len(first["state"]["generator_instances"]) == 2
    instances.append(instance("b", pipeline.GENERATOR_AVD_STRUCTURED_CONFIG))
    second = await pipeline._rack_rerun_snapshot(Mock(), "scenario", contents=False)
    assert second["ready"] and not second["missing_structured_targets"]
    assert len(second["state"]["generator_instances"]) == 3

    metadata["InterfacePhysical"] = {
        "edges": [
            {
                "node": {
                    "id": "port",
                    "device": {"node": {"name": {"value": "leaf"}}},
                    "name": {"value": "Ethernet1"},
                    "ip_address": {"node": {"id": "ip", "address": {"value": "10.0.0.1/31"}}},
                    "ip_addresses": {"edges": []},
                }
            }
        ]
    }
    monkeypatch.setattr(
        pipeline,
        "_structured_config_ethernet_ip_refs",
        AsyncMock(return_value=[{"artifact": "leaf", "interface": "Ethernet1", "ip_address": "10.0.0.1/31"}]),
    )
    complete = await pipeline._rack_rerun_snapshot(Mock(), "scenario")
    assert complete["state"]["physical_interface_ip_assignments"] == [
        {"device": "leaf", "interface": "Ethernet1", "ip_address": "10.0.0.1/31", "ip_addresses": []}
    ]
    metadata["InterfacePhysical"]["edges"][0]["node"]["ip_address"]["node"] = None
    with pytest.raises(AssertionError, match="physical primary IP backfill missing"):
        await pipeline._rack_rerun_snapshot(Mock(), "scenario")


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", ["default", "overlapping"])
async def test_ip_uniqueness_reads_primary_assignments_and_deduplicates_inverse_relations(namespace: str) -> None:
    from tests.integration.test_e2e_pipeline import _ipam_duplicate_report

    address = {
        "id": "ip",
        "address": {"value": "10.0.0.1/31"},
        "ip_namespace": {"node": {"name": {"value": namespace}}},
    }
    client = Mock(
        execute_graphql=AsyncMock(
            return_value={
                "IpamIPAddress": {
                    "count": 1,
                    "edges": [{"node": {**address, "interface": {"node": {"id": "left"}}}}],
                },
                "DcimInterface": {
                    "count": 2,
                    "edges": [
                        {"node": {"id": identifier, "ip_address": {"node": address}}}
                        for identifier in ("left", "right")
                    ],
                },
            }
        )
    )
    report = await _ipam_duplicate_report(client, "scenario")
    assert report["assigned"] == report["primary_assigned"] == 2
    assert not report["duplicate_ip_keys"]
    assert report["duplicate_interface_ips"] == {f"10.0.0.1/31 [{namespace}]": ["left", "right"]}
    assert "DcimInterface(offset:" in client.execute_graphql.await_args.kwargs["query"]


def _mlag_vtep_interfaces() -> list[dict]:
    domain = {"id": "mlag", "peers": {"edges": [{"node": {"id": peer}} for peer in ("leaf-a", "leaf-b")]}}
    return [
        {
            "id": f"{peer}-vtep",
            "__typename": "InterfaceVirtual",
            "name": {"value": "Loopback1"},
            "role": {"value": "vtep_loopback"},
            "ip_address": {"node": {"id": "shared-ip"}},
            "device": {
                "node": {
                    "id": peer,
                    "vtep_loopback_ip": {"node": {"id": "shared-ip"}},
                    "mlag_domain": {"node": deepcopy(domain)},
                }
            },
        }
        for peer in ("leaf-a", "leaf-b")
    ]


@pytest.mark.asyncio
async def test_primary_ip_report_validates_the_modeled_mlag_anycast_vtep() -> None:
    from tests.integration.test_e2e_pipeline import _ipam_duplicate_report

    address = {
        "id": "shared-ip",
        "address": {"value": "10.0.1.1/32"},
        "ip_namespace": {"node": {"name": {"value": "default"}}},
    }
    interfaces = _mlag_vtep_interfaces()
    for interface in interfaces:
        interface["ip_address"]["node"] = address
    client = Mock(
        execute_graphql=AsyncMock(
            return_value={
                "IpamIPAddress": {"count": 1, "edges": [{"node": address}]},
                "DcimInterface": {"count": 2, "edges": [{"node": interface} for interface in interfaces]},
            }
        )
    )
    report = await _ipam_duplicate_report(client, "scenario")
    assert report["primary_assigned"] == report["assigned"] == 2
    assert report["duplicate_ip_keys"] == report["duplicate_interface_ips"] == {}
    assert report["shared_mlag_vteps"] == {"10.0.1.1/32 [default]": ["leaf-a-vtep", "leaf-b-vtep"]}


@pytest.mark.parametrize(
    "fault",
    ["physical", "loopback0", "role", "domain", "peers", "vtep_ip", "ip", "same_device", "third", "inverse_only"],
)
def test_anycast_vtep_validation_rejects_unmodeled_sharing(fault: str) -> None:
    from tests.integration.test_e2e_pipeline import _is_shared_mlag_vtep

    interfaces = _mlag_vtep_interfaces()
    second = interfaces[1]
    device = second["device"]["node"]
    if fault == "physical":
        second["__typename"] = "InterfacePhysical"
    elif fault == "loopback0":
        second["name"]["value"] = "Loopback0"
    elif fault == "role":
        second["role"]["value"] = "loopback"
    elif fault == "domain":
        device["mlag_domain"]["node"]["id"] = "another-domain"
    elif fault == "peers":
        device["mlag_domain"]["node"]["peers"]["edges"].append({"node": {"id": "leaf-c"}})
    elif fault == "vtep_ip":
        device["vtep_loopback_ip"]["node"]["id"] = "another-ip"
    elif fault == "ip":
        device["vtep_loopback_ip"]["node"]["id"] = second["ip_address"]["node"]["id"] = "another-ip"
    elif fault == "same_device":
        device["id"] = "leaf-a"
    elif fault == "third":
        interfaces.append(deepcopy(second))
    elif fault == "inverse_only":
        interfaces[1] = None
    assert not _is_shared_mlag_vtep(interfaces)


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


@pytest.mark.parametrize(
    ("underlay", "role", "quantity", "matches"),
    [
        ("ebgp", "leaf", 201, True),
        ("ebgp", "border_leaf", 201, True),
        ("ebgp", "border_leaf", 200, False),
        ("ebgp", "border_leaf", 202, False),
        ("none", "border_leaf", 201, False),
        ("none", "l2leaf", 201, True),
    ],
)
@pytest.mark.asyncio
async def test_grouped_design_parity_paginates_without_per_node_reads(
    underlay: str, role: str, quantity: int, matches: bool
) -> None:
    from tests.integration.helpers import device_design_mismatches

    def collection(nodes: list[dict], count: int | None = None) -> dict:
        return {"count": len(nodes) if count is None else count, "edges": [{"node": node} for node in nodes]}

    def page(offset: int) -> dict:
        first = offset == 0
        return {
            "NetworkFabric": collection(
                [{"id": "fabric", "underlay_routing_protocol": {"value": underlay}}] if first else []
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
                [{"role": {"value": "leaf"}, "device_quantity": {"value": quantity}, "rack": {"node": {"id": "rack"}}}]
                if first
                else []
            ),
            "DcimDevice": collection(
                [
                    {
                        "id": str(index),
                        "role": {"value": role},
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
    mismatches = await device_design_mismatches(client, "baseline")
    assert (mismatches == []) is matches
    if not matches:
        assert f"design={quantity}" in mismatches[0]
        assert role in mismatches[0]
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


@pytest.mark.asyncio
async def test_file_content_uses_the_branch_aware_file_download_endpoint() -> None:
    client = Mock(address="http://test")
    client._get = AsyncMock(
        return_value=httpx.Response(200, text='{"hostvars": true}', request=httpx.Request("GET", "http://test"))
    )
    client.object_store.get = AsyncMock()
    assert await storage_content(client, "file-storage", file_branch="rack-rerun") == '{"hostvars": true}'
    client._get.assert_awaited_once_with("http://test/api/storage/files/by-storage-id/file-storage?branch=rack-rerun")
    client.object_store.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_scenario_cleanup_restores_only_known_shared_fields_and_its_new_dci_pool() -> None:
    from tests.integration.test_e2e_pipeline import DCI_POOL_NAME, _restore_shared_fabric_fields

    original = {
        "NetworkFabric": {
            "edges": [{"node": {"id": "fabric", "anta_enabled": {"value": True}, "dci_pool": {"node": None}}}]
        },
        "CoreIPPrefixPool": {"edges": [{"node": {"id": "existing", "name": {"value": "existing"}}}]},
    }
    current = {
        "NetworkFabric": {
            "edges": [{"node": {"id": "fabric", "anta_enabled": {"value": False}, "dci_pool": {"node": {"id": "new"}}}}]
        },
        "CoreIPPrefixPool": {
            "edges": original["CoreIPPrefixPool"]["edges"]
            + [
                {"node": {"id": "new", "name": {"value": DCI_POOL_NAME}}},
                {"node": {"id": "unrelated", "name": {"value": "unrelated"}}},
            ]
        },
    }
    client = Mock()
    client.execute_graphql = AsyncMock(side_effect=[current, {"NetworkFabricUpsert": {"ok": True}}])
    client.delete = AsyncMock()
    await _restore_shared_fabric_fields(client, "main", original)
    assert client.execute_graphql.await_args.kwargs["variables"] == {
        "data": {"id": "fabric", "anta_enabled": {"value": True}, "dci_pool": None}
    }
    client.delete.assert_awaited_once_with(kind="CoreIPPrefixPool", id="new", branch="main")


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


@pytest.mark.asyncio
async def test_artifact_definitions_settle_before_the_next_definition(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration import test_e2e_pipeline as pipeline

    client = Mock(address="http://test")
    events = []

    async def post(url: str, payload: dict) -> Mock:
        assert payload == {"nodes": []}
        events.append(url.rsplit("/", 1)[-1])
        return Mock()

    async def settled(client: object, branch: str, before: set[str]) -> None:
        assert branch == "scenario" and before == {"old"}
        events.append("settled")

    client._post = AsyncMock(side_effect=post)
    monkeypatch.setattr(pipeline, "_pipeline_tasks", AsyncMock(return_value=[Mock(id="old")]))
    monkeypatch.setattr(pipeline, "_wait_for_tasks", settled)
    await pipeline._trigger_all_artifacts(client, "scenario", [Mock(id="first"), Mock(id="second")])
    assert events == ["first?branch=scenario", "settled", "second?branch=scenario", "settled"]


@pytest.mark.asyncio
async def test_scenario_isolation_retries_transient_reads_and_ignores_graphql_order() -> None:
    from tests.integration.test_e2e_pipeline import _scenario_isolation_state

    client = Mock()
    client.execute_graphql = AsyncMock(
        side_effect=[
            GraphQLError(errors=[{"message": "busy", "extensions": {"code": 429}}], query=""),
            {"DcimDevice": {"edges": [{"node": {"id": "b"}}, {"node": {"id": "a"}}]}},
            {"DcimDevice": {"edges": [{"node": {"id": "a"}}, {"node": {"id": "b"}}]}},
        ]
    )
    assert await _scenario_isolation_state(client, "main") == await _scenario_isolation_state(client, "main")
    assert client.execute_graphql.await_count == 3


def test_artifact_definition_is_ready_only_when_all_of_its_outputs_are_stored() -> None:
    from tests.integration.test_e2e_pipeline import _ready_artifact_names

    def artifact(name: str, status: str = "Ready", storage: str | None = "stored") -> SimpleNamespace:
        return SimpleNamespace(
            name=SimpleNamespace(value=name),
            status=SimpleNamespace(value=status),
            storage_id=SimpleNamespace(value=storage),
        )

    assert _ready_artifact_names(
        [
            artifact("complete"),
            artifact("complete"),
            artifact("partial"),
            artifact("partial", "Pending"),
            artifact("missing-storage", storage=None),
        ]
    ) == {"complete"}
