from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from tests.integration.helpers import (
    _read_with_retries,
    dci_allocation_mismatches,
    dci_link_matches_endpoints,
    device_design_mismatches,
    is_unrouted_l2_mlag_domain,
    task_failure_diagnostic,
    wait_until,
    wait_until_stable,
)
from tests.integration.test_e2e_pipeline import (
    _branch_tasks_ready_since,
    _dci_link_identity,
    _fetch_ready_artifact_contents_by_target,
    _rack_snapshot_diff,
    _run_generator_for_nodes,
)


def _http_error(status_code: int, *, headers: dict[str, str] | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://infrahub.test/read")
    response = httpx.Response(status_code, headers=headers, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


@pytest.mark.asyncio
async def test_read_retries_respect_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch = AsyncMock(side_effect=[_http_error(429, headers={"Retry-After": "7"}), "ready"])
    sleep = AsyncMock()
    monkeypatch.setattr("tests.integration.helpers.asyncio.sleep", sleep)

    result = await _read_with_retries(fetch, deadline=time.monotonic() + 60)

    assert result == "ready"
    assert fetch.await_count == 2
    sleep.assert_awaited_once_with(7.0)


@pytest.mark.asyncio
async def test_read_retries_use_bounded_backoff_for_transient_statuses(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch = AsyncMock(side_effect=[_http_error(429), _http_error(502), _http_error(504), _http_error(503)])
    sleep = AsyncMock()
    monkeypatch.setattr("tests.integration.helpers.asyncio.sleep", sleep)

    with pytest.raises(httpx.HTTPStatusError, match="HTTP 503"):
        await _read_with_retries(fetch, deadline=time.monotonic() + 60)

    assert fetch.await_count == 4
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4]


@pytest.mark.asyncio
async def test_read_does_not_retry_permanent_status_or_past_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    permanent_fetch = AsyncMock(side_effect=_http_error(500))
    sleep = AsyncMock()
    monkeypatch.setattr("tests.integration.helpers.asyncio.sleep", sleep)

    with pytest.raises(httpx.HTTPStatusError, match="HTTP 500"):
        await _read_with_retries(permanent_fetch, deadline=time.monotonic() + 60)
    permanent_fetch.assert_awaited_once()
    sleep.assert_not_awaited()

    deadline_fetch = AsyncMock(side_effect=_http_error(429, headers={"Retry-After": "5"}))
    with pytest.raises(TimeoutError, match="would exceed the remaining poll deadline"):
        await _read_with_retries(deadline_fetch, deadline=time.monotonic() + 1)
    deadline_fetch.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_wait_until_stable_requires_two_identical_ready_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch = AsyncMock(side_effect=[{"checksum": "first"}, {"checksum": "stable"}, {"checksum": "stable"}])
    sleep = AsyncMock()
    monkeypatch.setattr("tests.integration.helpers.asyncio.sleep", sleep)

    result = await wait_until_stable(
        fetch,
        ready=lambda _: True,
        timeout=60,
        interval=10,
        describe="test snapshot",
    )

    assert result == {"checksum": "stable"}
    assert fetch.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [10, 10]


@pytest.mark.asyncio
async def test_wait_until_bounds_poll_sleep_by_remaining_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monotonic = iter([100.0, 100.0, 101.0])
    monkeypatch.setattr(
        "tests.integration.helpers.time",
        SimpleNamespace(monotonic=lambda: next(monotonic)),
    )
    monkeypatch.setattr("tests.integration.helpers._read_with_retries", AsyncMock(return_value=False))
    sleep = AsyncMock()
    monkeypatch.setattr("tests.integration.helpers.asyncio.sleep", sleep)

    with pytest.raises(AssertionError, match="timed out after 1s"):
        await wait_until(
            fetch=AsyncMock(),
            ready=bool,
            timeout=1,
            interval=10,
            describe="deadline test",
        )

    sleep.assert_awaited_once_with(1.0)


def _design(role: str, quantity: int, parent_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        role=SimpleNamespace(value=role),
        device_quantity=SimpleNamespace(value=quantity),
        rack=SimpleNamespace(id="rack-1", name=SimpleNamespace(value=parent_name)),
        pod=SimpleNamespace(id="pod-1", name=SimpleNamespace(value=parent_name)),
    )


def _device(role: str, *, rack_id: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        role=SimpleNamespace(value=role),
        rack=SimpleNamespace(id=rack_id),
        pod=SimpleNamespace(id="pod-1"),
    )


async def _design_mismatches(underlay: str, rack_roles: list[str], leaf_quantity: int) -> list[str]:
    fabric = SimpleNamespace(underlay_routing_protocol=SimpleNamespace(value=underlay))
    pod = SimpleNamespace(id="pod-1", parent=SimpleNamespace(id="fabric-1"))
    rack = SimpleNamespace(id="rack-1", name=SimpleNamespace(value="Rack-1"), pod=pod)
    pod_spine_roles = {
        "ebgp": "spine",
        "none": "l2spine",
        "ospf": "l3spine",
        "isis-ldp": "p",
    }
    devices = [_device(pod_spine_roles[underlay], rack_id=None)]
    devices.extend(_device(role, rack_id="rack-1") for role in rack_roles)
    pod_design = _design("spine", 1, "Pod-1")
    rack_design = _design("leaf", leaf_quantity, "Rack-1")

    async def all_nodes(kind: str, **_: object) -> list:
        return {
            "NetworkPod": [pod],
            "LocationRack": [rack],
            "DcimDevice": devices,
            "NetworkPodDeviceDesign": [pod_design],
            "NetworkRackDeviceDesign": [rack_design],
        }[kind]

    client = SimpleNamespace(
        all=AsyncMock(side_effect=all_nodes),
        get=AsyncMock(
            side_effect=lambda kind, **_: {"NetworkFabric": fabric, "NetworkPod": pod, "LocationRack": rack}[kind]
        ),
    )
    return await device_design_mismatches(client, "branch")


@pytest.mark.asyncio
async def test_l3ls_leaf_design_counts_border_leafs_and_keeps_exact_quantity() -> None:
    assert await _design_mismatches("ebgp", ["leaf", "border_leaf"], 2) == []

    mismatches = await _design_mismatches("ebgp", ["leaf", "border_leaf", "border_leaf"], 2)
    assert mismatches == [
        "LocationRack Rack-1 design_role=leaf device_roles={'leaf': 1, 'border_leaf': 2}: expected=2 observed=3"
    ]


@pytest.mark.parametrize(
    ("underlay", "rack_roles"),
    [("none", ["l2leaf", "l2leaf"]), ("ospf", ["l2leaf"])],
)
@pytest.mark.asyncio
async def test_non_l3ls_device_design_mappings_are_preserved(underlay: str, rack_roles: list[str]) -> None:
    assert await _design_mismatches(underlay, rack_roles, len(rack_roles)) == []


def test_only_unrouted_two_peer_l2spine_domains_are_exempt() -> None:
    devices = {
        "spine-1": {"role": "l2spine", "fabric_id": "l2-fabric"},
        "spine-2": {"role": "l2spine", "fabric_id": "l2-fabric"},
        "leaf-1": {"role": "leaf", "fabric_id": "l3-fabric"},
        "leaf-2": {"role": "border_leaf", "fabric_id": "l3-fabric"},
    }
    fabrics = {
        "l2-fabric": {"underlay_routing_protocol": "none"},
        "l3-fabric": {"underlay_routing_protocol": "ebgp"},
    }

    assert is_unrouted_l2_mlag_domain({"peer_ids": ["spine-1", "spine-2"]}, devices, fabrics)
    assert not is_unrouted_l2_mlag_domain({"peer_ids": ["leaf-1", "leaf-2"]}, devices, fabrics)
    assert not is_unrouted_l2_mlag_domain({"peer_ids": ["spine-1"]}, devices, fabrics)
    assert not is_unrouted_l2_mlag_domain({"peer_ids": ["spine-1", "spine-2"]}, devices, {})

    fabrics["l2-fabric"]["underlay_routing_protocol"] = "ebgp"
    assert not is_unrouted_l2_mlag_domain({"peer_ids": ["spine-1", "spine-2"]}, devices, fabrics)


def test_dci_link_selection_uses_device_and_paired_interface() -> None:
    endpoints = [("leaf-dc1", "Ethernet999"), ("leaf-dc2", "Ethernet999")]

    assert dci_link_matches_endpoints(
        {"nodes": ["leaf-dc2", "leaf-dc1"], "interfaces": ["Ethernet999", "Ethernet999"]}, endpoints
    )
    assert not dci_link_matches_endpoints(
        {"nodes": ["leaf-dc1", "leaf-dc2"], "interfaces": ["Ethernet5", "Ethernet5"]}, endpoints
    )
    assert not dci_link_matches_endpoints({"nodes": ["leaf-dc1", "leaf-dc2"], "interfaces": ["Ethernet999"]}, endpoints)


def test_dci_link_identity_is_independent_of_graphql_endpoint_order() -> None:
    forward = {
        "nodes": ["leaf-dc1", "leaf-dc2"],
        "interfaces": ["Ethernet999", "Ethernet999"],
        "as": [65001, 65001],
        "ip": ["172.16.0.8/31", "172.16.0.9/31"],
        "include_in_underlay_protocol": True,
    }
    reversed_order = {
        "nodes": ["leaf-dc2", "leaf-dc1"],
        "interfaces": ["Ethernet999", "Ethernet999"],
        "as": [65001, 65001],
        "ip": ["172.16.0.9/31", "172.16.0.8/31"],
        "include_in_underlay_protocol": True,
    }

    assert _dci_link_identity(forward) == _dci_link_identity(reversed_order)


def test_rack_snapshot_diff_keeps_divergent_device_and_interface_fields() -> None:
    previous = {
        "interfaces": [
            {"device": "leaf-1", "name": "Ethernet1", "mtu": 1500, "description": "uplink"},
            {"device": "leaf-1", "name": "Ethernet2", "mtu": 1500, "description": "peer"},
        ],
        "ipam_addresses": [{"id": "ip-1", "device": "leaf-1", "interface": "Ethernet1", "address": "192.0.2.1/31"}],
    }
    current = {
        "interfaces": [
            {"device": "leaf-1", "name": "Ethernet2", "mtu": 9214, "description": "peer"},
            {"device": "leaf-1", "name": "Ethernet1", "mtu": 1500, "description": "changed"},
        ],
        "ipam_addresses": [{"id": "ip-1", "device": "leaf-1", "interface": "Ethernet1", "address": "192.0.2.3/31"}],
    }

    diff = _rack_snapshot_diff(previous, current)

    assert "state.interfaces[leaf-1/Ethernet1].description: 'uplink' -> 'changed'" in diff
    assert "state.interfaces[leaf-1/Ethernet2].mtu: 1500 -> 9214" in diff
    assert "state.ipam_addresses[leaf-1/Ethernet1].address: '192.0.2.1/31' -> '192.0.2.3/31'" in diff
    assert " added " not in diff
    assert " removed " not in diff


def test_dci_allocation_uses_available_pool_prefix_without_seed_overlap() -> None:
    assert (
        dci_allocation_mismatches(
            ["172.16.0.8/31", "172.16.0.9/31"],
            "172.16.0.0/28",
            ["172.16.0.0/31", "172.16.0.2/31", "172.16.0.4/31", "172.16.0.6/31"],
        )
        == []
    )

    mismatches = dci_allocation_mismatches(
        ["172.16.0.0/31", "172.16.0.1/31"],
        "172.16.0.0/28",
        ["172.16.0.0/31"],
    )
    assert any("overlaps seed DCI allocations" in mismatch for mismatch in mismatches)


def test_failed_task_diagnostic_includes_target_and_error() -> None:
    task = SimpleNamespace(
        id="task-123",
        title="generate rack",
        state=SimpleNamespace(value="CRASHED"),
        branch="e2e-pipeline",
        related_nodes=[SimpleNamespace(kind="LocationRack", id="rack-123")],
        logs=[SimpleNamespace(severity="ERROR", message="generator failed")],
    )

    diagnostic = task_failure_diagnostic(task)

    assert diagnostic is not None
    assert "CRASHED task task-123" in diagnostic
    assert "LocationRack:rack-123" in diagnostic
    assert "generator failed" in diagnostic


@pytest.mark.asyncio
async def test_anta_artifact_read_is_filtered_to_target_device_ids() -> None:
    client = SimpleNamespace(
        execute_graphql=AsyncMock(
            return_value={
                "CoreArtifact": {
                    "edges": [
                        {
                            "node": {
                                "status": {"value": "Ready"},
                                "storage_id": {"value": "storage-target"},
                                "object": {"node": {"id": "device-target", "display_label": "target"}},
                            }
                        },
                        {
                            "node": {
                                "status": {"value": "Ready"},
                                "storage_id": {"value": "storage-other"},
                                "object": {"node": {"id": "device-other", "display_label": "other"}},
                            }
                        },
                    ]
                }
            }
        ),
        object_store=SimpleNamespace(get=AsyncMock(return_value="target catalog")),
        address="http://infrahub.test",
    )

    result = await _fetch_ready_artifact_contents_by_target(
        client,
        "branch",
        "avd_anta_catalog",
        {"target": "device-target"},
    )

    assert result == {"target": "target catalog"}
    request = client.execute_graphql.await_args.kwargs
    assert "object__ids: $target_ids" in request["query"]
    assert request["variables"] == {"artifact_name": "avd_anta_catalog", "target_ids": ["device-target"]}
    client.object_store.get.assert_awaited_once_with(identifier="storage-target")


@pytest.mark.asyncio
async def test_generator_launch_timeout_is_observed_without_resubmission(capsys: pytest.CaptureFixture[str]) -> None:
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(id="generator-id")),
        execute_graphql=AsyncMock(side_effect=TimeoutError),
    )

    await _run_generator_for_nodes(client, "dci-branch", "generate-fabric", ["fabric-id"])

    client.execute_graphql.assert_awaited_once()
    assert "observing tasks and outputs before any retry" in capsys.readouterr().out


def test_branch_task_ready_requires_new_completed_tasks_for_each_target() -> None:
    snapshot = {
        "task_failures": [],
        "tasks": [{"id": "prior", "state": "COMPLETED", "targets": ["DcimDevice:device-a"]}],
    }
    target_ids = ["device-a", "device-b"]

    assert not _branch_tasks_ready_since(snapshot, {"prior"}, target_ids)

    snapshot["tasks"].extend(
        [
            {"id": "new-a", "state": "COMPLETED", "targets": ["DcimDevice:device-a"]},
            {"id": "new-b", "state": "RUNNING", "targets": ["DcimDevice:device-b"]},
        ]
    )
    assert not _branch_tasks_ready_since(snapshot, {"prior"}, target_ids)

    snapshot["tasks"][-1]["state"] = "COMPLETED"
    assert _branch_tasks_ready_since(snapshot, {"prior"}, target_ids)

    snapshot["task_failures"] = [{"id": "new-failed", "diagnostic": "FAILED task task-failed error=run failed"}]
    with pytest.raises(AssertionError, match="FAILED task task-failed"):
        _branch_tasks_ready_since(snapshot, {"prior"}, target_ids)
