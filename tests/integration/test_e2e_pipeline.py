"""End-to-end pipeline integration test (trigger-driven).

Boots a real Infrahub stack via ``infrahub-testcontainers`` and exercises the
pipeline the way it actually runs in production: the ``triggers.yml`` event rules
are loaded, work happens on a branch, and only the **fabric** generator is
invoked explicitly. Everything downstream cascades via ``CoreNodeTriggerRule``:

    generate-fabric  (updates each pod's checksum)
      -> trigger-pod-generator      -> spines created (+ racks bumped)
      -> trigger-rack-generator     -> leaves created (+ hostvars generated)
      -> avd_hostvars_ready True    -> AVD structured config generated
      -> structured-config created  -> backfill runs

The trigger rules are ``branch_scope: "other_branches"``, so the cascade only
fires off ``main`` — hence a dedicated branch. Base data (schema, objects,
triggers, repository) is loaded on ``main``; the generator cascade runs on the
branch, mirroring the real change/proposed-change workflow.

The container stack is class-scoped, so it boots once and the ordered component
tests share it. Each downstream test simply waits (bounded) for the stage's
output to appear — it does not run that generator itself. All methods share one
class-scoped event loop so the single ``client`` is reused safely.

Heavy: run in CI's ``integration-tests-full`` job, which is manual-dispatch only.
Select locally with ``-m e2e``; exclude with ``-m "not e2e"``.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from ipaddress import IPv4Interface, IPv4Network
from operator import itemgetter
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import yaml
from infrahub_sdk.protocols import CoreGenericRepository
from infrahub_sdk.task.models import TaskFilter
from infrahub_sdk.testing.docker import TestInfrahubDockerClient
from infrahub_sdk.testing.repository import GitRepo

from .helpers import (
    ALL_ARTIFACT_NAMES,
    ANTA_DISABLED_MARKER,
    ARISTA_DEVICE_TYPES,
    ARISTA_TEMPLATE_INTERFACE_COUNTS,
    ARTIFACT_AVD_ANTA_CATALOG,
    ARTIFACT_AVD_EOS_CONFIG,
    ARTIFACT_CONTAINERLAB_TOPOLOGY,
    ARTIFACT_TIMEOUT,
    FABRIC_EXPECTED_ROLES,
    GENERATOR_FABRIC,
    GENERATOR_RACK,
    GENERATOR_TIMEOUT,
    GROUP_TIMEOUT,
    L2LS_MLAG_ROLES,
    POLL_INTERVAL,
    READ_TIMEOUT,
    REPO_SYNC_INTERVAL,
    REPO_SYNC_RETRIES,
    dci_allocation_mismatches,
    dci_link_matches_endpoints,
    device_design_mismatches,
    expected_super_spine_count,
    fabric_deployment_report,
    is_unrouted_l2_mlag_domain,
    task_failure_diagnostic,
    wait_until,
    wait_until_stable,
)

if TYPE_CHECKING:
    from pathlib import Path

    from infrahub_sdk import InfrahubClient

REPO_NAME = "e2e-repository"
# The cascade runs on this branch; trigger rules only fire off `main`.
PIPELINE_BRANCH = "e2e-pipeline"
GENERATOR_AVD_HOSTVAR = "generate-avd-device-hostvar"
SERVER_CABLING_RACK = "Rack-B2-1"
SERVER_CABLING_SERVER = "e2e-server-b2-1"
SERVER_CABLING_TEMPLATE = "multipod-b-compute-server-dual"
SERVER_CABLING_LEAF_NAMES = ("leaf-pod-b2-1-1", "leaf-pod-b2-1-2")
DCI_FABRIC_NAME = "Fabric-L3LS-Multi-Domain"
DCI_FABRIC_DEVICE_NAMES = (
    "leaf-infrahub-dc1-1-1",
    "leaf-infrahub-dc1-1-2",
    "leaf-infrahub-dc2-1-1",
    "leaf-infrahub-dc2-1-2",
)
DCI_DEVICE_NAMES = ("leaf-infrahub-dc1-1-1", "leaf-infrahub-dc2-1-1")
DCI_POOL_NAME = "Fabric-L3LS-Multi-Domain-DCI-Pool"
DCI_POOL_RESOURCE = "172.16.0.0/28"
DCI_SEED_LINK_ENDPOINTS = {
    "Fabric-L3LS-Multi-Domain-DCI-1": {
        ("leaf-infrahub-dc1-1-1", "Ethernet5"),
        ("leaf-infrahub-dc2-1-1", "Ethernet5"),
    },
    "Fabric-L3LS-Multi-Domain-DCI-2": {
        ("leaf-infrahub-dc1-1-1", "Ethernet6"),
        ("leaf-infrahub-dc2-1-2", "Ethernet6"),
    },
    "Fabric-L3LS-Multi-Domain-DCI-3": {
        ("leaf-infrahub-dc1-1-2", "Ethernet5"),
        ("leaf-infrahub-dc2-1-2", "Ethernet5"),
    },
    "Fabric-L3LS-Multi-Domain-DCI-4": {
        ("leaf-infrahub-dc1-1-2", "Ethernet6"),
        ("leaf-infrahub-dc2-1-1", "Ethernet6"),
    },
}
DCI_LINK_NAME = "e2e-dci-link"
DCI_INTERFACE_NAME = "Ethernet999"
FILTERED_ANTA_FABRIC = "Fabric-L3LS-Multi-Domain"
FILTERED_ANTA_TESTS = ("VerifyInterfaceDiscards", "VerifyLoggingErrors", "VerifyRunningConfigDiffs")
PROPOSED_CHANGE_ANTA_FABRIC = "Fabric-L3LS-MultiPod-A"


@pytest.mark.e2e
class TestE2EPipeline(TestInfrahubDockerClient):
    """Trigger-driven design-to-artifact pipeline against a real Infrahub instance.

    One test per pipeline component; they run in order and share the class-scoped
    stack. Downstream components appear via ``triggers.yml`` event rules after the
    fabric generator runs — the tests observe each stage, they do not drive it.
    Running a single test in isolation will fail because it relies on the state
    produced by the earlier ones.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _client_timeout(self) -> None:
        # Raise the infrahubctl client timeout: the fabric generator issues large
        # GraphQL reads/writes that can exceed the 60s default under load.
        os.environ.setdefault("INFRAHUB_TIMEOUT", "300")

    @staticmethod
    def _address(infrahub_port: int) -> str:
        return f"http://localhost:{infrahub_port}"

    # --- Component 1: schema (main) ----------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_schema(self, default_branch: str, client: InfrahubClient, schemas: list[dict]) -> None:
        """Load this repository's schemas and wait for convergence (FR-002)."""
        await client.schema.wait_until_converged(branch=default_branch)
        resp = await client.schema.load(schemas=schemas, branch=default_branch, wait_until_converged=True)
        assert resp.errors == {}, f"schema load errors: {resp.errors}"
        await client.schema.wait_until_converged(branch=default_branch)

    # --- Component 2: objects (main) ---------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_objects(self, default_branch: str, client: InfrahubClient, infrahub_port: int) -> None:
        """Load all seed fabrics through ``invoke load-objects`` (FR-003)."""
        result = self.execute_command(
            command=f"invoke load-objects --fabrics=all --branch={shlex.quote(default_branch)}",
            address=self._address(infrahub_port),
        )
        print(result.stdout, flush=True)
        if result.stderr:
            print(result.stderr, flush=True)
        assert result.returncode == 0, f"object load failed:\n{result.stdout}\n{result.stderr}"
        assert await client.all(kind="OrganizationManufacturer", branch=default_branch), "no manufacturers loaded"
        device_types = {dt.name.value for dt in await client.all(kind="DcimDeviceType", branch=default_branch)}
        assert device_types, "no device types loaded"
        # Issue #70: Arista device types + object templates with the right port layout.
        missing_types = set(ARISTA_DEVICE_TYPES) - device_types
        assert not missing_types, f"Arista device types not loaded: {missing_types}"
        template_counts = await _template_interface_counts(
            client, default_branch, list(ARISTA_TEMPLATE_INTERFACE_COUNTS)
        )
        assert template_counts == ARISTA_TEMPLATE_INTERFACE_COUNTS, (
            f"Arista object templates wrong/absent: expected {ARISTA_TEMPLATE_INTERFACE_COUNTS}, got {template_counts}"
        )

    # --- Component 3: target groups (main) ---------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_target_groups_populated(self, default_branch: str, client: InfrahubClient) -> None:
        """The generators' target groups are populated by seed membership (FR-004)."""
        for kind in ("NetworkFabric", "NetworkPod", "LocationRack"):
            await wait_until(
                fetch=lambda k=kind: client.all(kind=k, branch=default_branch),
                ready=lambda objs: len(objs) > 0,
                timeout=GROUP_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"seed objects of kind {kind}",
            )

    # --- Component 4: repository (main) ------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_register_repository(
        self, default_branch: str, client: InfrahubClient, root_directory: Path, remote_repos_dir: Path
    ) -> None:
        """Register this repository and wait for in-sync, failing fast on error (FR-005)."""
        repo = GitRepo(name=REPO_NAME, src_directory=root_directory, dst_directory=remote_repos_dir)
        await repo.add_to_infrahub(client=client)
        in_sync = await repo.wait_for_sync_to_complete(
            client=client, interval=REPO_SYNC_INTERVAL, retries=REPO_SYNC_RETRIES
        )
        if not in_sync:
            synced = await client.get(kind=CoreGenericRepository, name__value=REPO_NAME, branch=default_branch)
            msg = f"repository '{REPO_NAME}' did not reach in-sync; status={synced.sync_status.value}"
            raise AssertionError(msg)

    # --- Component 5: trigger rules (main, after repo sync) ----------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_triggers(self, infrahub_port: int) -> None:
        """Load the generator event-trigger rules so the cascade can fire on a branch.

        Loaded *after* the repository sync: the ``CoreGeneratorAction`` objects
        reference generators by name (generate-pod, generate-rack, ...) which only
        exist once the repository has registered its generator definitions. This
        mirrors the `inv load` order (objects -> repository -> wait -> triggers).
        """
        result = self.execute_command(
            command="infrahubctl object load triggers.yml", address=self._address(infrahub_port)
        )
        print(result.stdout, flush=True)
        if result.stderr:
            print(result.stderr, flush=True)
        assert result.returncode == 0, f"trigger load failed:\n{result.stdout}\n{result.stderr}"

    # --- Component 6: branch + enable ANTA ---------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_create_branch_and_enable_anta(self, client: InfrahubClient) -> None:
        """Create the working branch (triggers fire off `main`) and enable ANTA before the cascade."""
        await client.branch.create(branch_name=PIPELINE_BRANCH, sync_with_git=False)
        # Enable ANTA on the branch fabrics so the catalog is populated (not the
        # disabled marker) once the cascade reaches structured-config generation.
        fabrics = await client.all(kind="NetworkFabric", branch=PIPELINE_BRANCH)
        assert fabrics, "no fabrics found on the pipeline branch"
        for fabric in fabrics:
            if hasattr(fabric, "anta_enabled"):
                fabric.anta_enabled.value = True
                await fabric.save()
        filtered_fabric = next(fabric for fabric in fabrics if fabric.name.value == FILTERED_ANTA_FABRIC)
        assert filtered_fabric.avd_catalogs_filters.value == list(FILTERED_ANTA_TESTS)

    # --- Component 7: fabric generator (the only explicit kick-off) --------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_fabric_generator_creates_super_spines(self, client: InfrahubClient) -> None:
        """Kick off ONLY the fabric generator via the API; assert super-spines appear (FR-006/FR-008).

        Uses the ``CoreGeneratorDefinitionRun`` mutation — the same server-side
        mechanism the repo's own generators use to chain (see ``_trigger_generator``
        in ``src/solution_arista_avd/generator.py``) and how the triggered pod/rack
        generators run. No infrahubctl subprocess; it runs in the task-worker against
        the synced repo. Fire-and-forget, so success is confirmed by polling for the
        produced devices, which also proves the trigger cascade started.
        """
        gen_def = await client.get(kind="CoreGeneratorDefinition", name__value=GENERATOR_FABRIC, branch=PIPELINE_BRANCH)
        fabrics = await client.all(kind="NetworkFabric", branch=PIPELINE_BRANCH)
        fabric_ids = [fabric.id for fabric in fabrics]
        assert fabric_ids, "no fabrics found on the pipeline branch"
        await client.execute_graphql(
            query="""
            mutation RunGenerator($id: String!, $nodes: [String!]!) {
                CoreGeneratorDefinitionRun(data: { id: $id, nodes: $nodes }) {
                    ok
                }
            }
            """,
            variables={"id": gen_def.id, "nodes": fabric_ids},
            branch_name=PIPELINE_BRANCH,
        )

        expected = await expected_super_spine_count(client, PIPELINE_BRANCH)
        super_spines = await wait_until(
            fetch=lambda: client.filters(kind="DcimDevice", role__value="super_spine", branch=PIPELINE_BRANCH),
            ready=lambda d: len(d) >= expected > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"super_spine devices (expected {expected})",
        )
        print(f"super_spine devices: {len(super_spines)}", flush=True)

    # --- Component 8: pod trigger cascade ----------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_pod_trigger_creates_spines(self, client: InfrahubClient) -> None:
        """The fabric generator bumped the pods, firing the pod generator via triggers."""
        spines = await wait_until(
            fetch=lambda: client.filters(kind="DcimDevice", role__value="spine", branch=PIPELINE_BRANCH),
            ready=lambda d: len(d) > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="spine devices (pod generator fired via trigger)",
        )
        print(f"spine devices: {len(spines)}", flush=True)

    # --- Component 9: rack trigger cascade ---------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_rack_trigger_creates_leaves(self, client: InfrahubClient) -> None:
        """The pod generator bumped the racks, firing the rack generator via triggers."""
        leaves = await wait_until(
            fetch=lambda: client.filters(kind="DcimDevice", role__value="leaf", branch=PIPELINE_BRANCH),
            ready=lambda d: len(d) > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="leaf devices (rack generator fired via trigger)",
        )
        print(f"leaf devices: {len(leaves)}", flush=True)

    # --- Component 9a: device designs drive the generated device set -------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_devices_match_device_designs(self, client: InfrahubClient) -> None:
        """Every pod/rack produced exactly the devices its device designs declare.

        Device designs are the only source of device sizing, so this is the parity
        guard for the whole chain: a design quantity that stops being honoured, or
        devices created for a role with no design, fails here.
        """
        mismatches = await wait_until(
            fetch=lambda: device_design_mismatches(client, PIPELINE_BRANCH),
            ready=lambda problems: problems == [],
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="devices matching device designs",
        )
        assert mismatches == [], "device/design mismatches: " + "; ".join(mismatches)

    # --- Component 9b: BGP ASN nodes (regression 002) ----------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_asn_nodes_created_and_linked(self, client: InfrahubClient) -> None:
        """RoutingAsn nodes are created, fabric-owned, and linked to every L3 device (C1);
        each MLAG pair shares one ASN node across both leaves and the domain (C2).

        This is the regression guard for feature 002: before the fix, fabric
        generation produced zero Routing.Asn nodes.
        """
        report = await wait_until(
            fetch=lambda: _asn_report(client, PIPELINE_BRANCH),
            ready=lambda r: (
                len(r["asns"]) > 0
                and all(
                    d["asn_node_id"]
                    for d in r["devices"]
                    if d["role"] in ("super_spine", "spine", "leaf", "border_leaf")
                )
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="RoutingAsn nodes created and linked to every L3 device",
        )

        # SC-001 / C1: nodes exist and every one is owned by a fabric.
        assert report["asns"], "no RoutingAsn nodes created by fabric generation"
        assert all(a["fabric_id"] for a in report["asns"]), "a RoutingAsn node has no owning fabric"

        # SC-002: every L3 device (super-spine/spine/leaf) resolves an ASN node.
        l3 = [d for d in report["devices"] if d["role"] in ("super_spine", "spine", "leaf", "border_leaf")]
        assert l3, "no L3 devices found"
        unlinked = [d["name"] for d in l3 if not d["asn_node_id"]]
        assert not unlinked, f"L3 devices without an ASN node: {unlinked}"

        # C2 / SC-004: each MLAG domain shares one ASN node with both of its peers.
        asn_by_device = {d["id"]: d["asn_node_id"] for d in report["devices"]}
        devices_by_id = {device["id"]: device for device in report["devices"]}
        for dom in report["domains"]:
            dom_asn = dom["asn_node_id"]
            if is_unrouted_l2_mlag_domain(dom, devices_by_id, report["fabrics"]):
                assert dom_asn is None, f"pure-L2 MLAG domain {dom['domain_id']} unexpectedly has an ASN node"
                unnumbered_peers = [peer_id for peer_id in dom["peer_ids"] if asn_by_device.get(peer_id)]
                assert not unnumbered_peers, (
                    f"pure-L2 MLAG domain {dom['domain_id']} peers unexpectedly have ASN nodes: {unnumbered_peers}"
                )
                continue
            assert dom_asn, f"MLAG domain {dom['domain_id']} has no ASN node"
            peer_asns = {asn_by_device.get(pid) for pid in dom["peer_ids"]}
            assert peer_asns == {dom_asn}, (
                f"MLAG domain {dom['domain_id']} does not share one ASN node with its peers: "
                f"domain={dom_asn} peers={peer_asns}"
            )

        ebgp_fabric_ids = {
            fabric_id
            for fabric_id, fabric in report["fabrics"].items()
            if fabric["underlay_routing_protocol"] == "ebgp"
        }
        super_spines_by_fabric: dict[str, set[str]] = {}
        spines_by_pod: dict[str, set[str]] = {}
        for device in report["devices"]:
            if not device["asn_node_id"] or device["fabric_id"] not in ebgp_fabric_ids:
                continue
            if device["role"] == "super_spine":
                super_spines_by_fabric.setdefault(device["fabric_id"], set()).add(device["asn_node_id"])
            if device["role"] == "spine":
                spines_by_pod.setdefault(device["pod_id"], set()).add(device["asn_node_id"])

        assert all(len(asn_ids) == 1 for asn_ids in super_spines_by_fabric.values()), (
            f"eBGP super-spines do not share one ASN per fabric: {super_spines_by_fabric}"
        )
        assert all(len(asn_ids) == 1 for asn_ids in spines_by_pod.values()), (
            f"eBGP spines do not share one ASN per pod: {spines_by_pod}"
        )
        print(
            f"asn report: routing_asns={len(report['asns'])} l3_devices={len(l3)} mlag_domains={len(report['domains'])}",
            flush=True,
        )

    # --- Component 10: cabling & IP allocation (US2) -----------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_cabling_and_ip_allocation(self, client: InfrahubClient) -> None:
        """Links exist and every L3 device has a unique loopback + a management IP (FR-009)."""
        report = await wait_until(
            fetch=lambda: _cabling_and_ip_report(client, PIPELINE_BRANCH),
            ready=lambda r: (
                r["network_link_count"] > 0
                and r["with_loopback"] >= r["l3_device_count"] > 0
                and not r["duplicate_loopbacks"]
                and r["with_mgmt"] > 0
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="NetworkLink cabling and device IP allocation",
        )
        print(
            f"cabling/ip report: links={report['network_link_count']} l3_devices={report['l3_device_count']} "
            f"total={report['total']} with_loopback={report['with_loopback']} "
            f"with_mgmt={report['with_mgmt']} duplicate_loopbacks={report['duplicate_loopbacks']}",
            flush=True,
        )
        assert report["network_link_count"] > 0, "no NetworkLink cabling created"
        assert report["uplink_count"] > 0, "no role=uplink inter-switch links created"
        assert not report["invalid_uplinks"], f"invalid generated uplink metadata: {report['invalid_uplinks']}"
        assert report["with_loopback"] >= report["l3_device_count"] > 0, (
            f"expected every L3 device to have a loopback; {report['with_loopback']} have one"
        )
        assert not report["duplicate_loopbacks"], (
            f"duplicate loopback addresses allocated: {report['duplicate_loopbacks']}"
        )
        assert report["with_mgmt"] > 0, "no devices have an allocated management IP"

    # --- Component 10b: server cabling trigger -----------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_server_cabling_trigger_creates_links_and_lags(self, client: InfrahubClient) -> None:
        """Creating a server in the servers group triggers server cabling and creates links/LAGs."""
        prerequisites = await wait_until(
            fetch=lambda: _server_cabling_prerequisites(client, PIPELINE_BRANCH),
            ready=lambda r: (
                r["rack_complete"]
                and r["leaf_count"] == 2
                and r["leaf1_server_ports"] > 0
                and r["leaf2_server_ports"] > 0
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"{SERVER_CABLING_RACK} leaves and server-role ports",
        )

        prior_tasks = await wait_until(
            fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH),
            ready=_branch_tasks_idle,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"{PIPELINE_BRANCH} tasks before server-cabling trigger",
        )
        prior_task_ids = {task["id"] for task in prior_tasks["tasks"]}

        rack = await client.get(kind="LocationRack", name__value=SERVER_CABLING_RACK, branch=PIPELINE_BRANCH)
        template = await client.get(
            kind="TemplateComputePhysicalServer",
            template_name__value=SERVER_CABLING_TEMPLATE,
            branch=PIPELINE_BRANCH,
        )
        server = await client.create(
            kind="ComputePhysicalServer",
            branch=PIPELINE_BRANCH,
            name=SERVER_CABLING_SERVER,
            rack={"id": rack.id},
            object_template={"id": template.id},
            status="provisioning",
            member_of_groups=["servers"],
        )
        await server.save(allow_upsert=True)

        await wait_until_stable(
            fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH, known_task_ids=prior_task_ids),
            ready=lambda snapshot: _branch_tasks_ready_since(snapshot, prior_task_ids),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"{PIPELINE_BRANCH} server-cabling and leaf cascade tasks",
        )

        report = await wait_until(
            fetch=lambda: _server_cabling_report(
                client,
                PIPELINE_BRANCH,
                SERVER_CABLING_SERVER,
                prerequisites["leaf_ids"],
            ),
            ready=lambda r: (
                r["server_link_count"] == 2
                and r["server_physical_count"] == 2
                and r["server_connected_count"] == 2
                and r["server_bond_count"] == 1
                and r["leaf_port_channel_count"] == 2
                and r["leaf_port_channel_member_count"] == 2
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"server cabling output for {SERVER_CABLING_SERVER}",
        )

        assert report["server_physical_lags"] == {"Ethernet1": "Bond1", "Ethernet2": "Bond1"}
        assert report["server_bond_members"] == ["Ethernet1", "Ethernet2"]
        assert report["server_link_count"] == 2
        assert all(link["medium"] is None for link in report["server_link_metadata"])
        assert all(link["role"] is None for link in report["server_link_metadata"])
        assert all(
            link["name"].startswith(f"{SERVER_CABLING_SERVER}-Ethernet")
            and "__leaf-pod-b2-1-" in link["name"]
            and not link["name"].startswith("Uplink ")
            for link in report["server_link_metadata"]
        )
        assert report["leaf_port_channel_count"] == 2
        assert report["leaf_port_channel_member_count"] == 2
        assert report["leaf_port_channel_ids"] == [1117, 1117]
        print(f"server cabling report: {report}", flush=True)

    # --- Component 11: AVD structured config (via trigger cascade) ---------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_structured_config_cascade(self, client: InfrahubClient) -> None:
        """Rack completion set avd_hostvars_ready, cascading into AVD structured config (FR-010)."""
        report = await wait_until(
            fetch=lambda: _devices_without_structured_config(client, PIPELINE_BRANCH),
            ready=lambda r: r is not None and r["total"] > 0 and not r["without"],
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="every device has an AVD structured config",
        )
        print(f"structured config present on all {report['total']} devices", flush=True)

    # --- Component 11a: hostvar generator idempotence ---------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_hostvar_generator_two_runs_are_idempotent(self, client: InfrahubClient) -> None:
        """Two explicit hostvar runs produce identical normalized files and checksums."""
        device_ids = await _device_ids_for_fabric(client, PIPELINE_BRANCH, FILTERED_ANTA_FABRIC)
        assert device_ids, "no devices found for hostvar idempotence validation"

        snapshots = []
        for pass_number in (1, 2):
            await _run_generator_for_nodes(client, PIPELINE_BRANCH, GENERATOR_AVD_HOSTVAR, device_ids)
            report = await wait_until(
                fetch=lambda: _hostvar_snapshot(client, PIPELINE_BRANCH, device_ids),
                ready=itemgetter("ready"),
                timeout=GENERATOR_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"hostvar generator pass {pass_number} settled",
            )
            snapshots.append(report["state"])

        assert snapshots[1] == snapshots[0], "hostvar generator rerun changed normalized hostvar files"

    # --- Component 11a: backfilled IP uniqueness --------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_backfilled_ip_assignments_are_unique(self, client: InfrahubClient) -> None:
        """Backfilled IPAM entries have unique address/namespace keys and interface assignments."""
        report = await wait_until(
            fetch=lambda: _ipam_duplicate_report(client, PIPELINE_BRANCH),
            ready=lambda r: r["total"] > 0 and not r["duplicate_ip_keys"] and not r["duplicate_interface_ips"],
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="unique backfilled IPAM assignments",
        )

        assert not report["duplicate_ip_keys"], f"duplicate IpamIPAddress keys: {report['duplicate_ip_keys']}"
        assert not report["duplicate_interface_ips"], (
            f"duplicate interface-side IP assignments: {report['duplicate_interface_ips']}"
        )
        print(f"ipam duplicate report: total={report['total']} assigned={report['assigned']}", flush=True)

    # --- Component 11b: rack generator rerun regression -------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_rack_generators_rerun_without_error_or_drift(self, client: InfrahubClient) -> None:
        """Explicit rack reruns, including DC1_BORDER, do not fail or mutate generated state."""
        racks = await client.all(kind="LocationRack", branch=PIPELINE_BRANCH)
        rack_targets = sorted((rack.name.value, rack.id) for rack in racks)
        assert any(name == "DC1_BORDER" for name, _ in rack_targets), "DC1_BORDER rack not found"
        rack_ids = [rack_id for _, rack_id in rack_targets]
        device_ids = await _rack_device_ids(client, PIPELINE_BRANCH, rack_ids)
        assert device_ids, "no generated devices belong to the rack rerun targets"
        branch_tasks = await wait_until_stable(
            fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH),
            ready=_branch_tasks_idle,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"all {PIPELINE_BRANCH} tasks before rack reruns",
        )
        prior_task_ids = {task["id"] for task in branch_tasks["tasks"]}
        baseline = await wait_until_stable(
            fetch=lambda: _rack_rerun_snapshot(client, PIPELINE_BRANCH, rack_ids, device_ids, prior_task_ids),
            ready=_rack_rerun_state_ready,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="stable rack, device, interface, hostvar, and structured-config baseline",
        )

        snapshots = []
        for pass_number in (1, 2):
            prior_tasks = await wait_until_stable(
                fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH),
                ready=_branch_tasks_idle,
                timeout=GENERATOR_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"all {PIPELINE_BRANCH} tasks before rack rerun pass {pass_number}",
            )
            prior_task_ids = {task["id"] for task in prior_tasks["tasks"]}
            await _run_generator_for_nodes(client, PIPELINE_BRANCH, GENERATOR_RACK, rack_ids)
            snapshot = await wait_until_stable(
                fetch=lambda task_ids=prior_task_ids: _rack_rerun_snapshot(
                    client,
                    PIPELINE_BRANCH,
                    rack_ids,
                    device_ids,
                    task_ids,
                ),
                ready=_rack_rerun_ready,
                timeout=GENERATOR_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"rack generator rerun pass {pass_number} tasks and generated state settled",
            )
            snapshots.append(snapshot["state"])
            print(
                f"rack rerun pass {pass_number}: racks={len(rack_targets)} "
                f"rack_instances={len(snapshot['state']['rack_generator_instances'])} "
                f"tasks={len(snapshot['new_tasks'])}",
                flush=True,
            )

        if baseline["state"] != snapshots[0] or snapshots[1] != snapshots[0]:
            pytest.fail(
                "rack reruns changed generated state from the stable baseline:\n"
                f"baseline -> pass 1:\n{_rack_snapshot_diff(baseline['state'], snapshots[0])}\n"
                f"pass 1 -> pass 2:\n{_rack_snapshot_diff(snapshots[0], snapshots[1])}"
            )

    # --- Component 11b: DCI link hostvars ----------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_dci_link_generates_l3_edge_hostvars(self, client: InfrahubClient) -> None:
        """The selected Multi-Domain Border Leafs render and retain a pooled DCI link."""
        setup = await _create_dci_link_scenario(client)
        await _run_generator_for_nodes_sequentially(
            client,
            PIPELINE_BRANCH,
            GENERATOR_AVD_HOSTVAR,
            setup["device_ids"],
            describe=f"DCI hostvar generator for {DCI_LINK_NAME}",
        )

        report = await wait_until_stable(
            fetch=lambda: _dci_hostvars_report(
                client,
                PIPELINE_BRANCH,
                setup["fabric_device_names"],
                setup["endpoints"],
            ),
            ready=lambda r: r["ready"] and len(r["unique_matching_links"]) == 1,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"stable DCI l3_edge hostvars for {DCI_LINK_NAME}",
        )

        link = report["unique_matching_links"][0]
        assert link["nodes"] == setup["device_names"]
        assert link["interfaces"] == setup["interface_names"]
        assert link["as"] == setup["expected_as"]
        mismatches = dci_allocation_mismatches(
            link["ip"],
            DCI_POOL_RESOURCE,
            setup["seed_allocations"],
        )
        assert not mismatches, "; ".join(mismatches)
        assert link["include_in_underlay_protocol"] is True
        assert "profile" not in link
        assert all("p2p_links_profiles" not in hostvars.get("l3_edge", {}) for hostvars in report["hostvars"].values())

        for device_name, baseline in setup["baseline_hostvars"].items():
            current = report["hostvars"][device_name]
            baseline_l3_edge = baseline.get("l3_edge", {})
            current_l3_edge = current.get("l3_edge", {})
            assert {key: value for key, value in current_l3_edge.items() if key != "p2p_links"} == {
                key: value for key, value in baseline_l3_edge.items() if key != "p2p_links"
            }, f"{device_name} l3_edge fields changed beyond p2p_links"
            current_link_ids = {_dci_link_identity(candidate) for candidate in current_l3_edge.get("p2p_links", [])}
            for seed_link in baseline_l3_edge.get("p2p_links", []):
                assert _dci_link_identity(seed_link) in current_link_ids, (
                    f"{device_name} lost a seeded l3_edge link: {seed_link}"
                )

        first_allocation = list(link["ip"])
        await _run_generator_for_nodes_sequentially(
            client,
            PIPELINE_BRANCH,
            GENERATOR_AVD_HOSTVAR,
            setup["device_ids"],
            describe=f"DCI hostvar rerun for {DCI_LINK_NAME}",
        )
        rerun = await wait_until_stable(
            fetch=lambda: _dci_hostvars_report(
                client,
                PIPELINE_BRANCH,
                setup["fabric_device_names"],
                setup["endpoints"],
            ),
            ready=lambda r: r["ready"] and len(r["unique_matching_links"]) == 1,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"stable DCI allocation for {DCI_LINK_NAME} after rerun",
        )
        assert rerun["unique_matching_links"][0]["ip"] == first_allocation

        final_dci_state = await _dci_fabric_state(client, PIPELINE_BRANCH)
        _assert_dci_fabric_state(final_dci_state, branch=PIPELINE_BRANCH)
        assert final_dci_state == setup["pipeline_fabric_state"], (
            f"DCI pool references or seeded links changed on {PIPELINE_BRANCH}: {final_dci_state}"
        )
        print(f"dci hostvars report: {report}", flush=True)

    # --- Component 12: artifacts generated ---------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_artifacts_generated(self, client: InfrahubClient) -> None:
        """Every AVD artifact definition yields at least one Ready artifact on the branch (FR-011/FR-013)."""
        definitions = await client.all(kind="CoreArtifactDefinition", branch=PIPELINE_BRANCH)
        assert definitions, "no artifact definitions registered (repository sync incomplete?)"
        await _trigger_all_artifacts(client, PIPELINE_BRANCH, definitions)

        for artifact_name in ALL_ARTIFACT_NAMES:
            await self._wait_for_ready_artifact(client, PIPELINE_BRANCH, artifact_name, definitions)

    # --- Component 13: artifact content ------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_artifact_content(self, client: InfrahubClient) -> None:
        """A rendered EOS config mentions its device hostname; the ANTA catalog is populated (FR-012)."""
        eos_contents = await wait_until(
            fetch=lambda: _fetch_ready_artifact_contents(client, PIPELINE_BRANCH, ARTIFACT_AVD_EOS_CONFIG),
            ready=lambda contents: any("hostname" in content for content in contents),
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="a rendered EOS configuration artifact on the branch",
        )
        eos_content = next(content for content in eos_contents if "hostname" in content)
        assert eos_content.strip(), "EOS configuration artifact is empty"

        anta_content, _ = await _fetch_ready_artifact_content(client, PIPELINE_BRANCH, ARTIFACT_AVD_ANTA_CATALOG)
        assert anta_content and anta_content.strip(), "ANTA catalog artifact is empty"
        assert ANTA_DISABLED_MARKER not in anta_content, (
            "ANTA catalog rendered the disabled marker despite anta_enabled"
        )

        # ContainerLab Topology is fabric-scoped and, unlike the device-scoped
        # EOS/ANTA artifacts, does not auto-cascade on the branch — so generate it
        # explicitly for each fabric target on the branch, then read a populated
        # render (this also proves the transform renders server-side on a branch).
        definitions = await client.all(kind="CoreArtifactDefinition", branch=PIPELINE_BRANCH)
        clab_def = next((d for d in definitions if d.artifact_name.value == ARTIFACT_CONTAINERLAB_TOPOLOGY), None)
        assert clab_def, "ContainerLab Topology artifact definition not registered"
        for fabric in await client.all(kind="NetworkFabric", branch=PIPELINE_BRANCH):
            resp = await client._post(
                f"{client.address}/api/artifact/generate/{clab_def.id}?branch={PIPELINE_BRANCH}",
                payload={"nodes": [fabric.id]},
            )
            resp.raise_for_status()

        def _has_populated_topology(contents: list[str]) -> bool:
            for content in contents:
                if not content or not content.strip():
                    continue
                topology = yaml.safe_load(content).get("topology") or {}
                if topology.get("nodes") and topology.get("links"):
                    return True
            return False

        clab_contents = await wait_until(
            fetch=lambda: _fetch_ready_artifact_contents(client, PIPELINE_BRANCH, ARTIFACT_CONTAINERLAB_TOPOLOGY),
            ready=_has_populated_topology,
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="a populated ContainerLab topology artifact on the branch",
        )
        topos = [yaml.safe_load(c) for c in clab_contents if c and c.strip()]
        populated = [
            t for t in topos if (t.get("topology") or {}).get("nodes") and (t.get("topology") or {}).get("links")
        ]
        topo = max(populated, key=lambda t: len(t["topology"]["nodes"]))
        assert topo.get("name"), "ContainerLab topology has no fabric name"
        nodes = topo["topology"]["nodes"]
        links = topo["topology"]["links"]
        assert links, "ContainerLab topology has no links"
        assert all(n["kind"] == "arista_ceos" for n in nodes.values()), "non-cEOS node in topology"
        # Interface names must be ContainerLab short form, never raw EOS names.
        untranslated = [ep for link in links for ep in link["endpoints"] if "Ethernet" in ep]
        assert not untranslated, f"untranslated EOS interface names in links: {untranslated[:5]}"

    # --- Component 14: proposed-change ANTA refresh -----------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_proposed_change_refreshes_anta_catalogs_for_fabric_flag(self, client: InfrahubClient) -> None:
        """Enabling ANTA on a generated fabric refreshes its device catalogs in the proposed change."""
        fabric = await client.get(
            kind="NetworkFabric",
            name__value=PROPOSED_CHANGE_ANTA_FABRIC,
            branch=PIPELINE_BRANCH,
        )
        device_ids_by_name = await _fabric_device_ids_by_name(
            client,
            PIPELINE_BRANCH,
            PROPOSED_CHANGE_ANTA_FABRIC,
        )
        assert device_ids_by_name, f"{PROPOSED_CHANGE_ANTA_FABRIC} has no generated devices"

        definition = await client.get(
            kind="CoreArtifactDefinition",
            name__value="avd_anta_catalog",
            branch=PIPELINE_BRANCH,
        )

        # Establish disabled-marker catalogs before the proposed change. This
        # makes the subsequent marker -> populated transition prove that the
        # proposed-change artifact validator actually re-rendered the devices.
        fabric.anta_enabled.value = False
        await fabric.save()
        response = await client._post(
            f"{client.address}/api/artifact/generate/{definition.id}?branch={PIPELINE_BRANCH}",
            payload={"nodes": list(device_ids_by_name.values())},
        )
        response.raise_for_status()
        disabled_catalogs = await wait_until(
            fetch=lambda: _fetch_ready_artifact_contents_by_target(
                client,
                PIPELINE_BRANCH,
                ARTIFACT_AVD_ANTA_CATALOG,
                device_ids_by_name,
            ),
            ready=lambda contents: all(
                ANTA_DISABLED_MARKER in contents.get(device_name, "") for device_name in device_ids_by_name
            ),
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"disabled ANTA marker catalogs for {PROPOSED_CHANGE_ANTA_FABRIC}",
        )

        fabric.anta_enabled.value = True
        await fabric.save()
        proposed_change = await client.create(
            kind="CoreProposedChange",
            data={
                "source_branch": PIPELINE_BRANCH,
                "destination_branch": "main",
                "name": "e2e-enable-anta",
            },
        )
        await proposed_change.save()

        refreshed = await wait_until(
            fetch=lambda: _fetch_ready_artifact_contents_by_target(
                client,
                PIPELINE_BRANCH,
                ARTIFACT_AVD_ANTA_CATALOG,
                device_ids_by_name,
            ),
            ready=lambda contents: all(
                "anta.tests." in contents.get(device_name, "")
                and ANTA_DISABLED_MARKER not in contents.get(device_name, "")
                for device_name in device_ids_by_name
            ),
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"proposed-change ANTA catalogs for {PROPOSED_CHANGE_ANTA_FABRIC}",
        )
        assert set(refreshed) == set(device_ids_by_name)
        for device_name in device_ids_by_name:
            before = disabled_catalogs[device_name]
            after = refreshed[device_name]
            assert ANTA_DISABLED_MARKER in before, f"{device_name} had no disabled-marker catalog before the change"
            assert "anta.tests." in after and ANTA_DISABLED_MARKER not in after, (
                f"{device_name} catalog was not populated after the proposed change"
            )
            assert before != after, f"{device_name} catalog content did not change"

    # --- Fabric-scoped deployment validation (US4 / `--fabric` selector) ----
    @pytest.mark.asyncio(loop_scope="class")
    async def test_selected_fabric_deployment(self, client: InfrahubClient, target_fabric: str | None) -> None:
        """Validate one fabric's deployment when selected via ``--fabric``.

        With no ``--fabric`` this skips, so the default suite is unchanged
        (FR-022). An unknown fabric fails fast (FR-023). For a known fabric it
        asserts the design's expected device roles were generated; for the
        standalone-L2LS design it also asserts MLAG on both tiers and that the
        rendered EOS is pure Layer-2 (no VXLAN/BGP/EVPN).
        """
        if not target_fabric:
            pytest.skip("no --fabric selector; pass --fabric <name> to validate a single fabric")

        report = await wait_until(
            fetch=lambda: fabric_deployment_report(client, PIPELINE_BRANCH, target_fabric),
            ready=lambda r: r is None or r["device_count"] > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"generated devices for fabric {target_fabric!r}",
        )
        assert report is not None, f"--fabric {target_fabric!r} not found on {PIPELINE_BRANCH}"
        assert report["device_count"] > 0, f"fabric {target_fabric!r} generated no devices"

        expected_roles = FABRIC_EXPECTED_ROLES.get(target_fabric)
        if expected_roles:
            got_roles = set(report["roles"])
            assert expected_roles <= got_roles, (
                f"{target_fabric}: expected roles {sorted(expected_roles)}, got {sorted(got_roles)}"
            )

        if target_fabric == "Fabric-L2LS":
            assert report["mlag_roles"] >= L2LS_MLAG_ROLES, (
                f"L2LS MLAG missing a tier: expected {sorted(L2LS_MLAG_ROLES)}, got {sorted(report['mlag_roles'])}"
            )
            await _assert_pure_layer2_configs(client, PIPELINE_BRANCH, report["roles"])

    async def _wait_for_ready_artifact(
        self, client: InfrahubClient, branch: str, artifact_name: str, definitions: list
    ) -> None:
        """Poll for >=1 Ready artifact of ``artifact_name``, re-triggering once midway."""
        retriggered = {"done": False}

        async def fetch() -> list:
            artifacts = await client.filters(kind="CoreArtifact", name__value=artifact_name, branch=branch)
            ready = [a for a in artifacts if a.status.value == "Ready"]
            # One re-trigger at the halfway mark to cover read-replica visibility gaps (FR-013).
            if not ready and not retriggered["done"]:
                retriggered["done"] = True
                await _trigger_all_artifacts(client, branch, definitions)
            return ready

        await wait_until(
            fetch=fetch,
            ready=lambda ready: len(ready) >= 1,
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"Ready artifact '{artifact_name}'",
        )


async def _assert_pure_layer2_configs(client: InfrahubClient, branch: str, roles: dict[str, list[str]]) -> None:
    """Assert rendered EOS for the L2LS devices carries no overlay constructs.

    A standalone-L2LS fabric is pure Layer-2: its device configs must contain no
    VXLAN interface, no ``router bgp`` and no EVPN address-family (SC-002).
    """
    l2ls_hostnames = {name for role in ("l2spine", "l2leaf") for name in roles.get(role, [])}
    if not l2ls_hostnames:
        return

    forbidden = ("interface Vxlan", "router bgp", "address-family evpn")
    contents = await wait_until(
        fetch=lambda: _fetch_ready_artifact_contents(client, branch, ARTIFACT_AVD_EOS_CONFIG),
        ready=lambda cs: any(any(f"hostname {h}" in (c or "") for h in l2ls_hostnames) for c in cs),
        timeout=ARTIFACT_TIMEOUT,
        interval=POLL_INTERVAL,
        describe="rendered EOS configs for the L2LS devices",
    )

    checked = 0
    for content in contents:
        if not content or not any(f"hostname {h}" in content for h in l2ls_hostnames):
            continue
        checked += 1
        for token in forbidden:
            assert token not in content, f"L2LS EOS config contains forbidden overlay construct {token!r}"
    assert checked, "no L2LS EOS configs found to assert pure Layer-2"


async def _template_interface_counts(client: InfrahubClient, branch: str, template_names: list[str]) -> dict[str, int]:
    """Return {template_name: interface_count} for the given object templates (issue #70)."""
    resp = await client.execute_graphql(
        query="""
        query($names: [String!]) {
          TemplateDcimDevice(template_name__values: $names) {
            edges { node { template_name { value } interfaces { count } } }
          }
        }
        """,
        variables={"names": template_names},
        branch_name=branch,
    )
    return {
        edge["node"]["template_name"]["value"]: edge["node"]["interfaces"]["count"]
        for edge in resp["TemplateDcimDevice"]["edges"]
    }


async def _asn_report(client: InfrahubClient, branch: str) -> dict:
    """Report Routing.Asn nodes and every device's / MLAG domain's linked ASN node.

    Returns:
        asns: [{id, value, fabric_id}] — the allocated ASN nodes.
        devices: [{id, name, role, pod_id, fabric_id, asn_node_id}] — each device's linked ASN node id (or None).
        fabrics: {id: {underlay_routing_protocol}} — fabrics keyed by id.
        domains: [{domain_id, asn_node_id, peer_ids}] — MLAG domains and their peers.
    """
    query = """
    query AsnReport {
      RoutingAsn { edges { node { id asn { value } fabric { node { id } } } } }
      NetworkFabric { edges { node { id underlay_routing_protocol { value } } } }
      NetworkPod { edges { node { id parent { node { __typename id } } } } }
      DcimDevice { edges { node { id name { value } role { value } pod { node { id } } asn { node { id } } } } }
      MlagDomain { edges { node { domain_id { value } asn { node { id } } peers { edges { node { id } } } } } }
    }
    """
    resp = await client.execute_graphql(query=query, branch_name=branch)

    asns = []
    for edge in resp["RoutingAsn"]["edges"]:
        node = edge["node"]
        fabric = (node.get("fabric") or {}).get("node")
        asns.append(
            {"id": node["id"], "value": (node.get("asn") or {}).get("value"), "fabric_id": (fabric or {}).get("id")}
        )

    fabrics = {
        edge["node"]["id"]: {
            "underlay_routing_protocol": ((edge["node"].get("underlay_routing_protocol") or {}).get("value"))
        }
        for edge in resp["NetworkFabric"]["edges"]
    }
    pod_fabric_ids = {}
    for edge in resp["NetworkPod"]["edges"]:
        node = edge["node"]
        parent = (node.get("parent") or {}).get("node") or {}
        pod_fabric_ids[node["id"]] = parent.get("id") if parent.get("__typename") == "NetworkFabric" else None

    devices = []
    for edge in resp["DcimDevice"]["edges"]:
        node = edge["node"]
        asn_node = (node.get("asn") or {}).get("node")
        pod_id = ((node.get("pod") or {}).get("node") or {}).get("id")
        devices.append(
            {
                "id": node["id"],
                "name": node["name"]["value"],
                "role": node["role"]["value"],
                "pod_id": pod_id,
                "fabric_id": pod_fabric_ids.get(pod_id),
                "asn_node_id": (asn_node or {}).get("id"),
            }
        )

    domains = []
    for edge in resp["MlagDomain"]["edges"]:
        node = edge["node"]
        asn_node = (node.get("asn") or {}).get("node")
        peer_ids = [p["node"]["id"] for p in node.get("peers", {}).get("edges", []) if p.get("node")]
        domains.append(
            {
                "domain_id": node["domain_id"]["value"],
                "asn_node_id": (asn_node or {}).get("id"),
                "peer_ids": peer_ids,
            }
        )

    return {"asns": asns, "devices": devices, "fabrics": fabrics, "domains": domains}


async def _device_ip_report(client: InfrahubClient, branch: str) -> dict:
    """Report loopback/mgmt IP allocation across all devices, plus duplicate loopbacks."""
    query = """
    query DeviceIPs {
      DcimDevice {
        edges {
          node {
            name { value }
            role { value }
            loopback_ip { node { address { value } } }
            mgmt_ip { node { address { value } } }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(query=query, branch_name=branch)
    edges = resp["DcimDevice"]["edges"]
    loopbacks: list[str] = []
    with_mgmt = 0
    for edge in edges:
        node = edge["node"]
        lo = node.get("loopback_ip", {}).get("node")
        if lo and lo.get("address", {}).get("value"):
            loopbacks.append(lo["address"]["value"])
        mgmt = node.get("mgmt_ip", {}).get("node")
        if mgmt and mgmt.get("address", {}).get("value"):
            with_mgmt += 1
    duplicate_loopbacks = sorted({ip for ip in loopbacks if loopbacks.count(ip) > 1})
    return {
        "total": len(edges),
        "with_loopback": len(loopbacks),
        "with_mgmt": with_mgmt,
        "duplicate_loopbacks": duplicate_loopbacks,
    }


async def _cabling_and_ip_report(client: InfrahubClient, branch: str) -> dict:
    ip_report = await _device_ip_report(client, branch)
    links = await client.all(kind="NetworkLink", branch=branch)
    uplinks = [link for link in links if getattr(getattr(link, "role", None), "value", None) == "uplink"]
    invalid_uplinks = sorted(
        (
            {
                "name": link.name.value,
                "medium": getattr(getattr(link, "medium", None), "value", None),
            }
            for link in uplinks
            if not link.name.value.startswith("Uplink ")
            or getattr(getattr(link, "medium", None), "value", None) is not None
        ),
        key=itemgetter("name"),
    )
    l3_device_count = 0
    for role in ("super_spine", "spine", "leaf", "border_leaf"):
        l3_device_count += len(await client.filters(kind="DcimDevice", role__value=role, branch=branch))
    return {
        **ip_report,
        "network_link_count": len(links),
        "uplink_count": len(uplinks),
        "invalid_uplinks": invalid_uplinks,
        "l3_device_count": l3_device_count,
    }


async def _ipam_duplicate_report(client: InfrahubClient, branch: str) -> dict:
    """Report duplicate IpamIPAddress keys and duplicate assigned interface-side IPs."""
    query = """
    query IpamDuplicateReport {
      IpamIPAddress {
        edges {
          node {
            id
            address { value }
            ip_namespace { node { name { value } } }
            interface { node { id display_label } }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(query=query, branch_name=branch)
    key_to_ip_ids: dict[tuple[str, str], list[str]] = {}
    key_to_interface_ids: dict[tuple[str, str], set[str]] = {}
    assigned = 0
    for edge in resp["IpamIPAddress"]["edges"]:
        node = edge["node"]
        address = (node.get("address") or {}).get("value")
        namespace = ((node.get("ip_namespace") or {}).get("node") or {}).get("name", {}).get("value") or "default"
        if not address:
            continue

        key = (address, namespace)
        key_to_ip_ids.setdefault(key, []).append(node["id"])
        interface = (node.get("interface") or {}).get("node")
        if interface and interface.get("id"):
            assigned += 1
            key_to_interface_ids.setdefault(key, set()).add(interface["id"])

    duplicate_ip_keys = {
        f"{address} [{namespace}]": sorted(ip_ids)
        for (address, namespace), ip_ids in key_to_ip_ids.items()
        if len(ip_ids) > 1
    }
    duplicate_interface_ips = {
        f"{address} [{namespace}]": sorted(interface_ids)
        for (address, namespace), interface_ids in key_to_interface_ids.items()
        if len(interface_ids) > 1
    }
    return {
        "total": sum(len(ip_ids) for ip_ids in key_to_ip_ids.values()),
        "assigned": assigned,
        "duplicate_ip_keys": duplicate_ip_keys,
        "duplicate_interface_ips": duplicate_interface_ips,
    }


async def _server_cabling_prerequisites(client: InfrahubClient, branch: str) -> dict:
    """Report whether Rack-B2-1 is ready for server-cabling validation."""
    query = """
    query ServerCablingPrerequisites($rack: String!) {
      LocationRack(name__value: $rack) {
        edges {
          node {
            generation_complete { value }
          }
        }
      }
      DcimDevice(role__value: "leaf") {
        edges {
          node {
            id
            name { value }
            rack { node { name { value } } }
            interfaces(role__value: "server") {
              edges {
                node {
                  __typename
                  ... on InterfacePhysical {
                    name { value }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(
        query=query,
        variables={"rack": SERVER_CABLING_RACK},
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    rack_edges = resp["LocationRack"]["edges"]
    rack_complete = bool(rack_edges and rack_edges[0]["node"]["generation_complete"]["value"])
    leaves = [
        edge["node"]
        for edge in resp["DcimDevice"]["edges"]
        if edge["node"].get("rack", {}).get("node", {}).get("name", {}).get("value") == SERVER_CABLING_RACK
    ]
    ports_by_leaf = {
        leaf["name"]["value"]: len(
            [
                iface_edge
                for iface_edge in leaf["interfaces"]["edges"]
                if iface_edge["node"].get("__typename") == "InterfacePhysical"
            ]
        )
        for leaf in leaves
    }
    selected_leaves = sorted(
        (leaf for leaf in leaves if leaf["name"]["value"] in SERVER_CABLING_LEAF_NAMES),
        key=lambda item: item["name"]["value"],
    )
    return {
        "rack_complete": rack_complete,
        "leaf_count": len(leaves),
        "leaf_ids": [leaf["id"] for leaf in selected_leaves],
        "leaf1_server_ports": ports_by_leaf.get(SERVER_CABLING_LEAF_NAMES[0], 0),
        "leaf2_server_ports": ports_by_leaf.get(SERVER_CABLING_LEAF_NAMES[1], 0),
    }


async def _server_cabling_report(client: InfrahubClient, branch: str, server_name: str, leaf_ids: list[str]) -> dict:
    """Summarize server-cabling links, server Bond1, and switch-side Port-Channels."""
    query = """
    query ServerCablingReport($server: String!, $leaf_ids: [ID]!) {
      ComputePhysicalServer(name__value: $server) {
        edges {
          node {
            interfaces {
              edges {
                node {
                  __typename
                  ... on InterfacePhysical {
                    name { value }
                    connector {
                      node {
                        __typename
                        ... on NetworkLink {
                          name { value }
                          medium { value }
                          role { value }
                        }
                      }
                    }
                    lag { node { name { value } } }
                  }
                  ... on InterfaceLag {
                    name { value }
                    lag_members {
                      edges {
                        node {
                          name { value }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      InterfaceLag(device__ids: $leaf_ids, name__value: "Port-Channel1117") {
        edges {
          node {
            name { value }
            channel_id { value }
            device { node { name { value } } }
            lag_members {
              edges {
                node {
                  name { value }
                  device { node { name { value } } }
                }
              }
            }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(
        query=query,
        variables={"server": server_name, "leaf_ids": leaf_ids},
        branch_name=branch,
    )

    server_edges = resp["ComputePhysicalServer"]["edges"]
    if not server_edges:
        return {
            "server_link_count": 0,
            "server_link_metadata": [],
            "server_physical_count": 0,
            "server_connected_count": 0,
            "server_physical_lags": {},
            "server_bond_count": 0,
            "server_bond_members": [],
            "leaf_port_channel_count": 0,
            "leaf_port_channel_member_count": 0,
            "leaf_port_channel_ids": [],
        }

    physical_interfaces = []
    server_bonds = []
    for edge in server_edges[0]["node"]["interfaces"]["edges"]:
        node = edge["node"]
        if node["__typename"] == "InterfacePhysical":
            physical_interfaces.append(node)
        elif node["__typename"] == "InterfaceLag" and node["name"]["value"] == "Bond1":
            server_bonds.append(node)

    server_links = []
    server_physical_lags = {}
    server_connected_count = 0
    for iface in physical_interfaces:
        lag_node = (iface.get("lag") or {}).get("node") or {}
        connector_node = (iface.get("connector") or {}).get("node") or {}
        server_physical_lags[iface["name"]["value"]] = (lag_node.get("name") or {}).get("value")
        if connector_node:
            server_connected_count += 1
            if connector_node.get("__typename") == "NetworkLink":
                server_links.append(connector_node)
    server_link_metadata = sorted(
        (
            {
                "name": link["name"]["value"],
                "medium": (link.get("medium") or {}).get("value"),
                "role": (link.get("role") or {}).get("value"),
            }
            for link in server_links
        ),
        key=itemgetter("name"),
    )
    server_bond_members = sorted(
        member_edge["node"]["name"]["value"] for bond in server_bonds for member_edge in bond["lag_members"]["edges"]
    )

    leaf_port_channels = []
    for edge in resp["InterfaceLag"]["edges"]:
        node = edge["node"]
        if node["name"]["value"] != "Port-Channel1117":
            continue
        device_name = node.get("device", {}).get("node", {}).get("name", {}).get("value")
        if device_name in {"leaf-pod-b2-1-1", "leaf-pod-b2-1-2"}:
            leaf_port_channels.append(node)

    return {
        "server_link_count": len(server_links),
        "server_link_metadata": server_link_metadata,
        "server_physical_count": len(physical_interfaces),
        "server_connected_count": server_connected_count,
        "server_physical_lags": server_physical_lags,
        "server_bond_count": len(server_bonds),
        "server_bond_members": server_bond_members,
        "leaf_port_channel_count": len(leaf_port_channels),
        "leaf_port_channel_member_count": sum(len(lag["lag_members"]["edges"]) for lag in leaf_port_channels),
        "leaf_port_channel_ids": sorted(lag["channel_id"]["value"] for lag in leaf_port_channels),
    }


async def _devices_without_structured_config(client: InfrahubClient, branch: str) -> dict:
    """Return AVD-target devices missing a structured config file."""
    query = """
    query DevicesStructuredConfig {
      DcimDevice(role__values: ["super_spine", "spine", "leaf", "l2leaf"]) {
        edges {
          node {
            name { value }
            role { value }
            avd_artifact {
              node {
                structured_config_file { node { id } }
              }
            }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(query=query, branch_name=branch)
    edges = resp["DcimDevice"]["edges"]
    without = []
    for edge in edges:
        node = edge["node"]
        artifact = node.get("avd_artifact", {}).get("node")
        if not artifact:
            continue
        scf = artifact.get("structured_config_file", {}).get("node") if artifact else None
        if not scf:
            without.append(f"{node['name']['value']} ({node.get('role', {}).get('value', 'unknown')})")
    return {"total": len(edges), "without": without}


async def _device_ids_for_fabric(client: InfrahubClient, branch: str, fabric_name: str) -> list[str]:
    """Return generated device IDs belonging to one named fabric."""
    query = """
    query DeviceFabricMembership {
      DcimDevice {
        edges {
          node {
            id
            pod { node { parent { node { ... on NetworkFabric { name { value } } } } } }
          }
        }
      }
    }
    """
    response = await client.execute_graphql(query=query, branch_name=branch)
    device_ids = []
    for edge in response["DcimDevice"]["edges"]:
        node = edge["node"]
        parent = (((node.get("pod") or {}).get("node") or {}).get("parent") or {}).get("node") or {}
        if (parent.get("name") or {}).get("value") == fabric_name:
            device_ids.append(node["id"])
    return sorted(device_ids)


async def _hostvar_snapshot(client: InfrahubClient, branch: str, device_ids: list[str]) -> dict:
    """Return normalized hostvar content once all explicit hostvar runs settle."""
    query = """
    query HostvarSnapshot($device_ids: [ID]) {
      DcimDevice(ids: $device_ids) {
        edges {
          node {
            name { value }
            avd_artifact {
              node {
                hostvar_file {
                  node {
                    file_name { value }
                    checksum { value }
                    storage_id { value }
                  }
                }
              }
            }
          }
        }
      }
      CoreGeneratorInstance {
        edges {
          node {
            status { value }
            object { node { id } }
            definition { node { ... on CoreGeneratorDefinition { name { value } } } }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(
        query=query,
        variables={"device_ids": device_ids},
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    hostvar_instances = []
    for edge in resp["CoreGeneratorInstance"]["edges"]:
        node = edge["node"]
        definition = (node.get("definition") or {}).get("node") or {}
        target_id = ((node.get("object") or {}).get("node") or {}).get("id")
        if (definition.get("name") or {}).get("value") == GENERATOR_AVD_HOSTVAR and target_id in device_ids:
            hostvar_instances.append((node.get("status") or {}).get("value") or "")

    files = []
    for edge in resp["DcimDevice"]["edges"]:
        node = edge["node"]
        hostvar_file = (((node.get("avd_artifact") or {}).get("node") or {}).get("hostvar_file") or {}).get(
            "node"
        ) or {}
        storage_id = (hostvar_file.get("storage_id") or {}).get("value")
        if not storage_id:
            continue
        content = json.loads(await client.object_store.get(identifier=storage_id))
        files.append(
            {
                "device": node["name"]["value"],
                "file_name": (hostvar_file.get("file_name") or {}).get("value") or "",
                "checksum": (hostvar_file.get("checksum") or {}).get("value") or "",
                "content": content,
            }
        )

    active_statuses = {"pending", "running", "scheduled", "in_progress", "queued"}
    return {
        "ready": (
            len(files) == len(device_ids) > 0
            and not any(status.lower() in active_statuses for status in hostvar_instances)
            and not any(status == "Error" for status in hostvar_instances)
        ),
        "state": sorted(files, key=itemgetter("device")),
    }


async def _rack_rerun_snapshot(
    client: InfrahubClient,
    branch: str,
    rack_ids: list[str],
    device_ids: list[str],
    prior_task_ids: set[str],
) -> dict:
    """Read targeted rack, task, interface, hostvar, and structured-config state."""
    target_ids = sorted(set(rack_ids + device_ids))
    query = """
    query RackRerunSnapshot($rack_ids: [ID], $device_ids: [ID], $target_ids: [ID]) {
      CoreGeneratorInstance(object__ids: $target_ids, limit: 1000) {
        edges {
          node {
            id
            name { value }
            status { value }
            object { node { id display_label } }
            definition {
              node {
                id
                display_label
                ... on CoreGeneratorDefinition {
                  name { value }
                }
              }
            }
          }
        }
      }
      LocationRack(ids: $rack_ids, limit: 1000) {
        edges {
          node {
            id
            name { value }
            index { value }
            rack_type { value }
            mlag { value }
            generation_complete { value }
          }
        }
      }
      DcimDevice(ids: $device_ids, limit: 1000) {
        edges {
          node {
            id
            name { value }
            role { value }
            index { value }
            node_id { value }
            description { value }
            os_version { value }
            status { value }
            position { value }
            rack_face { value }
            rack { node { id name { value } } }
            pod { node { id name { value } } }
            device_type { node { id name { value } } }
            platform { node { id name { value } } }
            primary_address {
              node { id address { value } ip_namespace { node { id name { value } } } }
            }
            loopback_ip {
              node { id address { value } ip_namespace { node { id name { value } } } }
            }
            vtep_loopback_ip {
              node { id address { value } ip_namespace { node { id name { value } } } }
            }
            mgmt_ip {
              node { id address { value } ip_namespace { node { id name { value } } } }
            }
            avd_artifact {
              node {
                hostvar_file {
                  node {
                    id
                    file_name { value }
                    file_type { value }
                    checksum { value }
                    file_size { value }
                    storage_id { value }
                  }
                }
                structured_config_file {
                  node {
                    id
                    file_name { value }
                    file_type { value }
                    checksum { value }
                    file_size { value }
                    storage_id { value }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(
        query=query,
        variables={"rack_ids": rack_ids, "device_ids": device_ids, "target_ids": target_ids},
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    interface_edges, interface_count = await _rack_interface_edges(client, branch, device_ids)
    branch_tasks = await _branch_task_snapshot(client, branch, known_task_ids=prior_task_ids)
    task_failures = [
        failure["diagnostic"] for failure in branch_tasks["task_failures"] if failure["id"] not in prior_task_ids
    ]
    active_task_states = {"PENDING", "RUNNING", "SCHEDULED", "PAUSED", "CANCELLING", "IN_PROGRESS", "QUEUED"}
    active_tasks = [task for task in branch_tasks["tasks"] if task["state"] in active_task_states]
    new_tasks = [task for task in branch_tasks["tasks"] if task["id"] not in prior_task_ids]

    generator_instances = _normalize_generator_instances(resp["CoreGeneratorInstance"]["edges"])
    target_id_set = set(target_ids)
    generator_instances = [instance for instance in generator_instances if instance["target_id"] in target_id_set]
    rack_generator_instances = [
        instance
        for instance in generator_instances
        if instance["definition_name"] == GENERATOR_RACK and instance["target_id"] in set(rack_ids)
    ]
    generator_errors = [
        instance for instance in generator_instances if instance["status"] in {"ERROR", "FAILED", "CRASHED"}
    ]
    active_generator_instances = [
        instance for instance in generator_instances if instance["status"] in active_task_states
    ]

    racks = {
        edge["node"]["id"]: {
            "name": edge["node"]["name"]["value"],
            "index": (edge["node"].get("index") or {}).get("value"),
            "rack_type": (edge["node"].get("rack_type") or {}).get("value"),
            "mlag": (edge["node"].get("mlag") or {}).get("value"),
            "generation_complete": bool(edge["node"]["generation_complete"]["value"]),
        }
        for edge in resp["LocationRack"]["edges"]
    }
    devices = {}
    hostvar_files = {}
    structured_config_files = {}
    for edge in resp["DcimDevice"]["edges"]:
        node = edge["node"]
        device_name = node["name"]["value"]
        rack = (node.get("rack") or {}).get("node") or {}
        pod = (node.get("pod") or {}).get("node") or {}
        device_type = (node.get("device_type") or {}).get("node") or {}
        platform = (node.get("platform") or {}).get("node") or {}

        devices[device_name] = {
            "id": node["id"],
            "role": (node.get("role") or {}).get("value"),
            "index": (node.get("index") or {}).get("value"),
            "node_id": (node.get("node_id") or {}).get("value"),
            "description": (node.get("description") or {}).get("value"),
            "os_version": (node.get("os_version") or {}).get("value"),
            "status": (node.get("status") or {}).get("value"),
            "position": (node.get("position") or {}).get("value"),
            "rack_face": (node.get("rack_face") or {}).get("value"),
            "rack": {"id": rack.get("id"), "name": (rack.get("name") or {}).get("value")},
            "pod": {"id": pod.get("id"), "name": (pod.get("name") or {}).get("value")},
            "device_type": {"id": device_type.get("id"), "name": (device_type.get("name") or {}).get("value")},
            "platform": {"id": platform.get("id"), "name": (platform.get("name") or {}).get("value")},
            "primary_address": _rack_ip_reference(node, "primary_address"),
            "loopback_ip": _rack_ip_reference(node, "loopback_ip"),
            "vtep_loopback_ip": _rack_ip_reference(node, "vtep_loopback_ip"),
            "mgmt_ip": _rack_ip_reference(node, "mgmt_ip"),
        }
        artifact = (node.get("avd_artifact") or {}).get("node") or {}
        hostvar_file = (artifact.get("hostvar_file") or {}).get("node") or {}
        structured_config_file = (artifact.get("structured_config_file") or {}).get("node") or {}
        hostvar_files[device_name] = await _rack_file_state(client, hostvar_file)
        structured_config_files[device_name] = await _rack_file_state(client, structured_config_file)

    interfaces, ipam_addresses = _normalize_rack_interfaces(interface_edges)
    new_tasks.sort(key=itemgetter("id"))
    active_tasks.sort(key=itemgetter("id"))
    rack_generator_instances.sort(key=itemgetter("target_id", "name"))
    generator_instances.sort(key=itemgetter("definition_name", "target_id", "name"))
    devices = dict(sorted(devices.items()))
    racks = dict(sorted(racks.items()))
    hostvar_files = dict(sorted(hostvar_files.items()))
    structured_config_files = dict(sorted(structured_config_files.items()))
    # Task IDs are retained in the outer snapshot for readiness diagnostics,
    # but are run-specific and must not count as generated-state drift.
    state = {
        "racks": racks,
        "devices": devices,
        "generator_instances": generator_instances,
        "rack_generator_instances": rack_generator_instances,
        "interfaces": interfaces,
        "interface_count": interface_count,
        "ipam_addresses": ipam_addresses,
        "hostvar_files": hostvar_files,
        "structured_config_files": structured_config_files,
    }
    snapshot = {
        "rack_ids": rack_ids,
        "device_ids": device_ids,
        "racks": racks,
        "devices": devices,
        "rack_generator_instances": rack_generator_instances,
        "new_tasks": new_tasks,
        "active_tasks": active_tasks,
        "active_generator_instances": active_generator_instances,
        "task_failures": task_failures,
        "generator_errors": generator_errors,
        "hostvar_files": hostvar_files,
        "structured_config_files": structured_config_files,
        "interface_count": interface_count,
        "interfaces": interfaces,
        "state": state,
    }
    snapshot["ready"] = _rack_rerun_ready(snapshot)
    return snapshot


async def _rack_device_ids(client: InfrahubClient, branch: str, rack_ids: list[str]) -> list[str]:
    """Return generated device IDs for the selected racks in stable order."""
    devices = await client.filters(kind="DcimDevice", rack__ids=rack_ids, branch=branch)
    return sorted({device.id for device in devices})


async def _branch_task_snapshot(
    client: InfrahubClient,
    branch: str,
    *,
    known_task_ids: set[str] | None = None,
) -> dict:
    """Read branch tasks and load logs only for failures newer than the baseline."""
    tasks = await client.task.filter(
        filter=TaskFilter(branch=branch),
        timeout=READ_TIMEOUT,
        include_related_nodes=True,
    )
    detailed_tasks = []
    for task in tasks:
        detailed_task = task
        if (
            known_task_ids is not None
            and task.id not in known_task_ids
            and _task_state(task) in {"FAILED", "CRASHED", "CANCELLED"}
        ):
            detailed_task = await client.task.get(id=task.id, include_logs=True, include_related_nodes=True)
        detailed_tasks.append(detailed_task)
    task_failures = [
        {"id": task.id, "diagnostic": diagnostic}
        for task in detailed_tasks
        if (diagnostic := task_failure_diagnostic(task)) is not None
    ]
    task_states = sorted((_rack_task_state(task) for task in detailed_tasks), key=itemgetter("id"))
    return {"tasks": task_states, "task_failures": task_failures}


def _branch_tasks_idle(snapshot: dict) -> bool:
    """Wait until the branch has no active tasks."""
    active_states = {"PENDING", "RUNNING", "SCHEDULED", "PAUSED", "CANCELLING", "IN_PROGRESS", "QUEUED"}
    return not any(task["state"] in active_states for task in snapshot["tasks"])


def _branch_tasks_ready_since(
    snapshot: dict,
    prior_task_ids: set[str],
    target_ids: list[str] | None = None,
) -> bool:
    """Require new completed tasks, no active branch work, and optional device targets."""
    failures = [failure["diagnostic"] for failure in snapshot["task_failures"] if failure["id"] not in prior_task_ids]
    if failures:
        raise AssertionError("new branch tasks failed:\n" + "\n".join(failures))

    new_tasks = [task for task in snapshot["tasks"] if task["id"] not in prior_task_ids]
    if not new_tasks or any(task["state"] != "COMPLETED" for task in new_tasks) or not _branch_tasks_idle(snapshot):
        return False
    if target_ids is None:
        return True

    observed_targets = {target for task in new_tasks for target in task["targets"]}
    expected_targets = {f"DcimDevice:{target_id}" for target_id in target_ids}
    return expected_targets <= observed_targets


async def _rack_interface_edges(client: InfrahubClient, branch: str, device_ids: list[str]) -> tuple[list[dict], int]:
    """Read every physical interface for the target devices using bounded pages."""
    query = """
    query RackRerunInterfaces($device_ids: [ID], $offset: Int!, $limit: Int!) {
      InterfacePhysical(device__ids: $device_ids, offset: $offset, limit: $limit) {
        count
        edges {
          node {
            id
            name { value }
            status { value }
            role { value }
            mtu { value }
            description { value }
            index { value }
            dot1q_id { value }
            mac_address { value }
            spanning_tree_portfast { value }
            l2_mode { value }
            device { node { id name { value } } }
            lag { node { id name { value } channel_id { value } } }
            untagged_vlan { node { id name { value } vlan_id { value } } }
            tagged_vlan {
              edges {
                node {
                  id
                  name { value }
                  vlan_id { value }
                }
              }
            }
            connector {
              node {
                __typename
                id
                display_label
                name { value }
                medium { value }
                ... on NetworkLink {
                  role { value }
                  include_in_underlay_protocol { value }
                }
                connected_endpoints {
                  edges {
                    node {
                      __typename
                      id
                      display_label
                    }
                  }
                }
              }
            }
            ip_address {
              node {
                id
                address { value }
                ip_namespace { node { id name { value } } }
              }
            }
            ip_addresses {
              edges {
                node {
                  id
                  address { value }
                  ip_namespace { node { id name { value } } }
                }
              }
            }
            profiles {
              edges {
                node {
                  id
                  profile_name { value }
                  profile_priority { value }
                }
              }
            }
            member_of_groups {
              edges {
                node {
                  id
                  name { value }
                  group_type { value }
                }
              }
            }
            subscriber_of_groups {
              edges {
                node {
                  id
                  name { value }
                  group_type { value }
                }
              }
            }
            tags {
              edges {
                node {
                  id
                  name { value }
                }
              }
            }
          }
        }
      }
    }
    """
    offset = 0
    limit = 500
    count = 0
    edges = []
    while offset == 0 or offset < count:
        response = await client.execute_graphql(
            query=query,
            variables={"device_ids": device_ids, "offset": offset, "limit": limit},
            branch_name=branch,
            timeout=READ_TIMEOUT,
        )
        connection = response["InterfacePhysical"]
        count = connection["count"]
        page = connection["edges"]
        if not page:
            break
        edges.extend(page)
        offset += len(page)
    return edges, count


def _normalize_generator_instances(edges: list[dict]) -> list[dict]:
    instances = []
    for edge in edges:
        node = edge["node"]
        definition = (node.get("definition") or {}).get("node") or {}
        definition_name = ((definition.get("name") or {}).get("value")) or definition.get("display_label")
        target = (node.get("object") or {}).get("node") or {}
        instances.append(
            {
                "id": node.get("id") or "",
                "name": (node.get("name") or {}).get("value") or "",
                "status": ((node.get("status") or {}).get("value") or "").upper(),
                "definition_name": definition_name or "",
                "target_id": target.get("id") or "",
                "target": target.get("display_label") or target.get("id") or "",
            }
        )
    return sorted(instances, key=itemgetter("definition_name", "target_id", "name"))


def _task_state(task: Any) -> str:
    state = getattr(task, "state", "")
    return str(getattr(state, "value", state)).upper()


def _rack_task_state(task: Any) -> dict[str, Any]:
    targets = sorted(f"{node.kind}:{node.id}" for node in getattr(task, "related_nodes", []))
    return {
        "id": task.id,
        "title": task.title,
        "state": _task_state(task),
        "branch": task.branch,
        "targets": targets,
    }


def _rack_rerun_state_ready(snapshot: dict) -> bool:
    """Require complete rack state with no active tasks or generator failures."""
    if snapshot.get("task_failures"):
        raise AssertionError("rack rerun tasks failed:\n" + "\n".join(snapshot["task_failures"]))
    if snapshot.get("generator_errors"):
        failures = [
            f"{instance['status']} generator {instance['definition_name']} "
            f"target={instance['target']} ({instance['target_id']})"
            for instance in snapshot["generator_errors"]
        ]
        raise AssertionError("rack generator instances failed:\n" + "\n".join(failures))
    if snapshot.get("active_tasks") or snapshot.get("active_generator_instances"):
        return False

    rack_ids = set(snapshot.get("rack_ids", []))
    device_ids = set(snapshot.get("device_ids", []))
    rack_instance_targets = {instance["target_id"] for instance in snapshot.get("rack_generator_instances", [])}
    if rack_instance_targets != rack_ids:
        return False
    racks = snapshot.get("racks", {})
    if set(racks) != rack_ids or not all(rack["generation_complete"] for rack in racks.values()):
        return False
    devices = snapshot.get("devices", {})
    if {device["id"] for device in devices.values()} != device_ids:
        return False
    if set(snapshot.get("hostvar_files", {})) != set(devices):
        return False
    if set(snapshot.get("structured_config_files", {})) != set(devices):
        return False
    if snapshot.get("interface_count") != len(snapshot.get("interfaces", [])):
        return False
    files = list(snapshot["hostvar_files"].values()) + list(snapshot["structured_config_files"].values())
    return all(file and file["checksum"] and file["storage_id"] and file["content"] is not None for file in files)


def _rack_rerun_ready(snapshot: dict) -> bool:
    """Require completed new tasks and complete generated state, failing on task errors."""
    if not _rack_rerun_state_ready(snapshot):
        return False
    new_tasks = snapshot.get("new_tasks", [])
    return bool(new_tasks) and all(task["state"] == "COMPLETED" for task in new_tasks)


async def _rack_file_state(client: InfrahubClient, file_node: dict) -> dict | None:
    """Capture complete file metadata and parsed JSON for a targeted device."""
    if not file_node:
        return None
    storage_id = (file_node.get("storage_id") or {}).get("value")
    content = None
    if storage_id:
        content = json.loads(await client.object_store.get(identifier=storage_id))
    return {
        "id": file_node.get("id") or "",
        "file_name": (file_node.get("file_name") or {}).get("value") or "",
        "file_type": (file_node.get("file_type") or {}).get("value") or "",
        "checksum": (file_node.get("checksum") or {}).get("value") or "",
        "file_size": (file_node.get("file_size") or {}).get("value") or 0,
        "storage_id": storage_id or "",
        "content": content,
    }


def _rack_ip_reference(device_node: dict, relation_name: str) -> dict | None:
    address_node = (device_node.get(relation_name) or {}).get("node") or {}
    if not address_node:
        return None
    namespace = (address_node.get("ip_namespace") or {}).get("node") or {}
    return {
        "id": address_node.get("id") or "",
        "address": (address_node.get("address") or {}).get("value") or "",
        "namespace": (namespace.get("name") or {}).get("value") or "default",
    }


def _normalize_rack_interfaces(edges: list[dict]) -> tuple[list[dict], list[dict]]:
    interfaces = []
    ipam_addresses = []
    for edge in edges:
        node = edge["node"]
        device = (node.get("device") or {}).get("node") or {}
        device_name = (device.get("name") or {}).get("value") or ""
        interface_name = (node.get("name") or {}).get("value") or ""
        connector = (node.get("connector") or {}).get("node") or {}
        lag = (node.get("lag") or {}).get("node") or {}
        untagged_vlan = (node.get("untagged_vlan") or {}).get("node") or {}
        tagged_vlans = [edge["node"] for edge in (node.get("tagged_vlan") or {}).get("edges", []) if edge.get("node")]
        ip_nodes = [
            ip_edge.get("node") or {}
            for ip_edge in (node.get("ip_addresses") or {}).get("edges", [])
            if ip_edge.get("node")
        ]
        singular_ip = (node.get("ip_address") or {}).get("node") or {}
        if singular_ip:
            ip_nodes.append(singular_ip)
        ip_states_by_id = {}
        for ip_node in ip_nodes:
            namespace = (ip_node.get("ip_namespace") or {}).get("node") or {}
            ip_state = {
                "id": ip_node.get("id") or "",
                "address": (ip_node.get("address") or {}).get("value") or "",
                "namespace": (namespace.get("name") or {}).get("value") or "default",
            }
            ip_states_by_id[ip_state["id"] or f"{ip_state['namespace']}:{ip_state['address']}"] = ip_state
        ip_addresses = list(ip_states_by_id.values())
        ipam_addresses.extend(
            {
                "id": ip_state["id"],
                "address": ip_state["address"],
                "namespace": ip_state["namespace"],
                "device": device_name,
                "interface": interface_name,
            }
            for ip_state in ip_addresses
        )
        connected_endpoints = [
            {
                "kind": endpoint["node"].get("__typename") or "",
                "id": endpoint["node"].get("id") or "",
                "display_label": endpoint["node"].get("display_label") or "",
            }
            for endpoint in (connector.get("connected_endpoints") or {}).get("edges", [])
            if endpoint.get("node")
        ]
        profiles = [edge["node"] for edge in (node.get("profiles") or {}).get("edges", []) if edge.get("node")]
        member_of_groups = [
            edge["node"] for edge in (node.get("member_of_groups") or {}).get("edges", []) if edge.get("node")
        ]
        subscriber_of_groups = [
            edge["node"] for edge in (node.get("subscriber_of_groups") or {}).get("edges", []) if edge.get("node")
        ]
        tags = [edge["node"] for edge in (node.get("tags") or {}).get("edges", []) if edge.get("node")]
        interfaces.append(
            {
                "id": node.get("id") or "",
                "device": device_name,
                "name": interface_name,
                "status": (node.get("status") or {}).get("value"),
                "role": (node.get("role") or {}).get("value"),
                "mtu": (node.get("mtu") or {}).get("value"),
                "description": (node.get("description") or {}).get("value"),
                "index": (node.get("index") or {}).get("value"),
                "dot1q_id": (node.get("dot1q_id") or {}).get("value"),
                "mac_address": (node.get("mac_address") or {}).get("value"),
                "spanning_tree_portfast": (node.get("spanning_tree_portfast") or {}).get("value"),
                "l2_mode": (node.get("l2_mode") or {}).get("value"),
                "lag": {
                    "id": lag.get("id"),
                    "name": (lag.get("name") or {}).get("value"),
                    "channel_id": (lag.get("channel_id") or {}).get("value"),
                },
                "untagged_vlan": {
                    "id": untagged_vlan.get("id"),
                    "name": (untagged_vlan.get("name") or {}).get("value"),
                    "vlan_id": (untagged_vlan.get("vlan_id") or {}).get("value"),
                },
                "tagged_vlans": sorted(
                    (
                        {
                            "id": vlan.get("id") or "",
                            "name": (vlan.get("name") or {}).get("value"),
                            "vlan_id": (vlan.get("vlan_id") or {}).get("value"),
                        }
                        for vlan in tagged_vlans
                    ),
                    key=lambda vlan: (vlan["name"] or "", vlan["id"]),
                ),
                "connector": {
                    "kind": connector.get("__typename") or "",
                    "id": connector.get("id") or "",
                    "name": (connector.get("name") or {}).get("value"),
                    "medium": (connector.get("medium") or {}).get("value"),
                    "role": (connector.get("role") or {}).get("value"),
                    "include_in_underlay_protocol": (connector.get("include_in_underlay_protocol") or {}).get("value"),
                    "endpoints": sorted(connected_endpoints, key=itemgetter("kind", "display_label", "id")),
                },
                "profiles": sorted(
                    (
                        {
                            "id": profile.get("id") or "",
                            "name": (profile.get("profile_name") or {}).get("value"),
                            "priority": (profile.get("profile_priority") or {}).get("value"),
                        }
                        for profile in profiles
                    ),
                    key=lambda profile: (
                        profile["priority"] is None,
                        profile["priority"] or 0,
                        profile["name"] or "",
                        profile["id"],
                    ),
                ),
                "member_of_groups": sorted(
                    (
                        {
                            "id": group.get("id") or "",
                            "name": (group.get("name") or {}).get("value"),
                            "group_type": (group.get("group_type") or {}).get("value"),
                        }
                        for group in member_of_groups
                    ),
                    key=lambda group: (group["group_type"] or "", group["name"] or "", group["id"]),
                ),
                "subscriber_of_groups": sorted(
                    (
                        {
                            "id": group.get("id") or "",
                            "name": (group.get("name") or {}).get("value"),
                            "group_type": (group.get("group_type") or {}).get("value"),
                        }
                        for group in subscriber_of_groups
                    ),
                    key=lambda group: (group["group_type"] or "", group["name"] or "", group["id"]),
                ),
                "tags": sorted(
                    (
                        {
                            "id": tag.get("id") or "",
                            "name": (tag.get("name") or {}).get("value"),
                        }
                        for tag in tags
                    ),
                    key=lambda tag: (tag["name"] or "", tag["id"]),
                ),
                "ip_addresses": sorted(ip_addresses, key=itemgetter("namespace", "address", "id")),
            }
        )
    interfaces.sort(key=itemgetter("device", "name", "id"))
    ipam_addresses.sort(key=itemgetter("device", "interface", "namespace", "address", "id"))
    return interfaces, ipam_addresses


def _rack_snapshot_diff(previous: dict, current: dict) -> str:
    """Show every changed snapshot field, with paths scoped to device/interface."""
    differences: list[str] = []

    def visit(path: str, expected: Any, observed: Any) -> None:
        if expected == observed:
            return
        if isinstance(expected, dict) and isinstance(observed, dict):
            for key in sorted(set(expected) | set(observed)):
                child_path = f"{path}.{key}" if path else str(key)
                if key not in expected:
                    differences.append(f"{child_path}: added {_snapshot_value(child_path, observed[key])}")
                elif key not in observed:
                    differences.append(f"{child_path}: removed {_snapshot_value(child_path, expected[key])}")
                else:
                    visit(child_path, expected[key], observed[key])
            return
        if isinstance(expected, list) and isinstance(observed, list):
            expected_by_identity = _snapshot_list_items(expected)
            observed_by_identity = _snapshot_list_items(observed)
            if expected_by_identity is not None and observed_by_identity is not None:
                for identity in sorted(set(expected_by_identity) | set(observed_by_identity)):
                    if identity not in expected_by_identity:
                        value = observed_by_identity[identity]
                        label = _snapshot_list_label(value, 0)
                        differences.append(f"{path}{label}: added {_snapshot_value(path, value)}")
                    elif identity not in observed_by_identity:
                        value = expected_by_identity[identity]
                        label = _snapshot_list_label(value, 0)
                        differences.append(f"{path}{label}: removed {_snapshot_value(path, value)}")
                    else:
                        old_item = expected_by_identity[identity]
                        new_item = observed_by_identity[identity]
                        label = _snapshot_list_label(old_item, 0)
                        visit(f"{path}{label}", old_item, new_item)
                return
            for index in range(max(len(expected), len(observed))):
                if index >= len(expected):
                    value = observed[index]
                    label = _snapshot_list_label(value, index)
                    differences.append(f"{path}{label}: added {_snapshot_value(path, value)}")
                elif index >= len(observed):
                    value = expected[index]
                    label = _snapshot_list_label(value, index)
                    differences.append(f"{path}{label}: removed {_snapshot_value(path, value)}")
                else:
                    old_item = expected[index]
                    new_item = observed[index]
                    label = _snapshot_list_label(old_item, index)
                    new_label = _snapshot_list_label(new_item, index)
                    if label != new_label and isinstance(old_item, dict) and isinstance(new_item, dict):
                        visit(f"{path}[{label}->{new_label}]", old_item, new_item)
                    else:
                        visit(f"{path}{label}", old_item, new_item)
            return
        differences.append(f"{path}: {_snapshot_value(path, expected)} -> {_snapshot_value(path, observed)}")

    visit("state", previous, current)
    return "\n".join(differences) if differences else "no differing snapshot fields found"


def _snapshot_list_label(value: Any, index: int) -> str:
    if isinstance(value, dict):
        if isinstance(value.get("device"), str) and isinstance(value.get("name"), str):
            return f"[{value['device']}/{value['name']}]"
        if isinstance(value.get("device"), str) and isinstance(value.get("interface"), str):
            return f"[{value['device']}/{value['interface']}]"
        for key in ("interface", "name", "id"):
            label = value.get(key)
            if isinstance(label, str) and label:
                return f"[{label}]"
    return f"[{index}]"


def _snapshot_value(path: str, value: Any) -> str:
    if any(secret in path.lower() for secret in ("password", "secret", "token", "credential", "authorization")):
        return "<redacted>"
    rendered = repr(_redact_snapshot_value(value))
    return rendered if len(rendered) <= 240 else f"{rendered[:237]}..."


def _redact_snapshot_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "<redacted>"
            if any(
                secret in str(key).lower() for secret in ("password", "secret", "token", "credential", "authorization")
            )
            else _redact_snapshot_value(nested)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_redact_snapshot_value(item) for item in value]
    return value


def _snapshot_list_items(values: list[Any]) -> dict[tuple[str, ...], Any] | None:
    items = {}
    for value in values:
        if not isinstance(value, dict):
            return None
        if isinstance(value.get("device"), str) and isinstance(value.get("name"), str):
            identity = ("device", value["device"], "interface", value["name"])
        elif isinstance(value.get("id"), str) and value["id"]:
            identity = ("id", value["id"])
        else:
            return None
        if identity in items:
            return None
        items[identity] = value
    return items


async def _create_dci_link_scenario(client: InfrahubClient) -> dict:
    """Create the test link on e2e-pipeline, already converged in the disposable stack.

    The pinned BranchCreate mutation cannot select a non-default source branch.
    Reuse this isolated test branch instead of rebuilding from default.
    """
    pipeline_fabric_state = await _dci_fabric_state(client, PIPELINE_BRANCH)
    _assert_dci_fabric_state(pipeline_fabric_state, branch=PIPELINE_BRANCH)
    prior_tasks = await wait_until_stable(
        fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH),
        ready=_branch_tasks_idle,
        timeout=GENERATOR_TIMEOUT,
        interval=POLL_INTERVAL,
        describe=f"{PIPELINE_BRANCH} tasks before DCI scenario",
    )

    def seeded_hostvars_ready(report: dict) -> bool:
        return report["ready"] and all(
            sum(dci_link_matches_endpoints(link, list(endpoints)) for link in report["links"]) == 1
            for endpoints in DCI_SEED_LINK_ENDPOINTS.values()
        )

    seed_hostvars = await wait_until_stable(
        fetch=lambda: _dci_hostvars_report(client, PIPELINE_BRANCH, list(DCI_FABRIC_DEVICE_NAMES)),
        ready=seeded_hostvars_ready,
        timeout=GENERATOR_TIMEOUT,
        interval=POLL_INTERVAL,
        describe=f"stable seed DCI hostvars on {PIPELINE_BRANCH}",
    )

    query = """
    query DciBorderLeafs($device_names: [String!]!) {
      DcimDevice(name__values: $device_names) {
        edges {
          node {
            id
            name { value }
            role { value }
            asn { node { id asn { value } } }
            pod {
              node {
                parent {
                  node {
                    __typename
                    ... on NetworkFabric { id name { value } }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    response = await client.execute_graphql(
        query=query,
        variables={"device_names": list(DCI_DEVICE_NAMES)},
        branch_name=PIPELINE_BRANCH,
        timeout=READ_TIMEOUT,
    )
    devices = []
    for edge in response["DcimDevice"]["edges"]:
        node = edge["node"]
        fabric = (((node.get("pod") or {}).get("node") or {}).get("parent") or {}).get("node") or {}
        asn_node = (node.get("asn") or {}).get("node") or {}
        asn = (asn_node.get("asn") or {}).get("value")
        assert node["role"]["value"] == "border_leaf", f"{node['name']['value']} lost its border_leaf role"
        assert fabric.get("id") == pipeline_fabric_state["fabric_id"], (
            f"{node['name']['value']} does not belong to selected fabric {DCI_FABRIC_NAME}"
        )
        assert fabric.get("name", {}).get("value") == DCI_FABRIC_NAME, (
            f"{node['name']['value']} does not belong to {DCI_FABRIC_NAME}"
        )
        assert asn is not None, f"{node['name']['value']} has no ASN"
        devices.append(
            {
                "device_id": node["id"],
                "device_name": node["name"]["value"],
                "role": node["role"]["value"],
                "asn_id": asn_node.get("id"),
                "asn": int(asn),
                "fabric_id": fabric["id"],
            }
        )
    devices.sort(key=itemgetter("device_name", "device_id"))
    assert [device["device_name"] for device in devices] == list(DCI_DEVICE_NAMES), (
        f"expected exactly the selected DCI border leafs {DCI_DEVICE_NAMES}, "
        f"observed {[device['device_name'] for device in devices]}"
    )
    assert len({device["fabric_id"] for device in devices}) == 1
    assert len({device["asn_id"] for device in devices}) == 1, f"DCI endpoints do not share an ASN: {devices}"

    seed_links = []
    for seed_link_name, seed_endpoints in DCI_SEED_LINK_ENDPOINTS.items():
        matches = [link for link in seed_hostvars["links"] if dci_link_matches_endpoints(link, list(seed_endpoints))]
        assert len(matches) == 1, f"expected one hostvar link for seeded DCI link {seed_link_name}, observed {matches}"
        seed_links.extend(matches)
    seed_allocations = sorted(
        {str(IPv4Interface(address).network) for link in seed_links for address in link.get("ip", [])}
    )
    assert len(seed_allocations) == len(DCI_SEED_LINK_ENDPOINTS), (
        f"expected four existing seed DCI allocations, observed {seed_allocations}"
    )
    assert all(IPv4Network(allocation).subnet_of(IPv4Network(DCI_POOL_RESOURCE)) for allocation in seed_allocations)

    baseline_report = seed_hostvars
    assert baseline_report["ready"], f"selected DCI branch hostvars are incomplete: {baseline_report}"

    existing_links = await client.filters(kind="NetworkLink", name__value=DCI_LINK_NAME, branch=PIPELINE_BRANCH)
    assert not existing_links, f"test DCI link {DCI_LINK_NAME} already exists on {PIPELINE_BRANCH}"

    dci_link = await client.create(
        kind="NetworkLink",
        branch=PIPELINE_BRANCH,
        name=DCI_LINK_NAME,
        medium="smf",
        role="dci",
        include_in_underlay_protocol=True,
    )
    await dci_link.save(allow_upsert=True)

    endpoints = [(device["device_name"], DCI_INTERFACE_NAME) for device in devices]
    for device in devices:
        existing = await client.filters(
            kind="InterfacePhysical",
            name__value=DCI_INTERFACE_NAME,
            device__ids=[device["device_id"]],
            branch=PIPELINE_BRANCH,
        )
        assert not existing, f"{device['device_name']} already has {DCI_INTERFACE_NAME}"
        dci_interface = await client.create(
            kind="InterfacePhysical",
            branch=PIPELINE_BRANCH,
            name=DCI_INTERFACE_NAME,
            device={"id": device["device_id"]},
            role="peering",
        )
        await dci_interface.save(allow_upsert=True)
        interface = await client.get(
            kind="InterfacePhysical",
            id=dci_interface.id,
            branch=PIPELINE_BRANCH,
            include=["connector"],
        )
        interface.connector = dci_link
        interface.status.value = "active"
        await interface.save(allow_upsert=True)

    prior_task_ids = {task["id"] for task in prior_tasks["tasks"]}
    setup_tasks = await wait_until_stable(
        fetch=lambda: _branch_task_snapshot(client, PIPELINE_BRANCH, known_task_ids=prior_task_ids),
        ready=_branch_tasks_idle,
        timeout=GENERATOR_TIMEOUT,
        interval=POLL_INTERVAL,
        describe=f"{PIPELINE_BRANCH} tasks after creating {DCI_LINK_NAME}",
    )
    setup_failures = [
        failure["diagnostic"] for failure in setup_tasks["task_failures"] if failure["id"] not in prior_task_ids
    ]
    if setup_failures:
        raise AssertionError("DCI link setup tasks failed:\n" + "\n".join(setup_failures))

    return {
        "device_ids": [device["device_id"] for device in devices],
        "device_names": [device["device_name"] for device in devices],
        "interface_names": [interface for _, interface in endpoints],
        "endpoints": endpoints,
        "fabric_device_names": list(DCI_FABRIC_DEVICE_NAMES),
        "expected_as": [device["asn"] for device in devices],
        "seed_allocations": seed_allocations,
        "baseline_hostvars": baseline_report["hostvars"],
        "pipeline_fabric_state": pipeline_fabric_state,
    }


async def _dci_fabric_state(client: InfrahubClient, branch: str) -> dict:
    """Read the seeded DCI pool references and links for the selected fabric."""
    query = """
    query DciFabricState($fabric_name: String!, $pool_name: String!, $seed_link_names: [String!]!) {
      NetworkFabric(name__value: $fabric_name) {
        edges {
          node {
            id
            name { value }
            dci_pool { node { id name { value } } }
            fabric_ip_pools { edges { node { id name { value } } } }
          }
        }
      }
      CoreIPPrefixPool(name__value: $pool_name) {
        edges {
          node {
            id
            name { value }
            default_prefix_length { value }
            resources { edges { node { prefix { value } } } }
          }
        }
      }
      NetworkLink(name__values: $seed_link_names) {
        edges {
          node {
            name { value }
            connected_endpoints {
              edges {
                node {
                  __typename
                  ... on InterfacePhysical {
                    name { value }
                    device { node { name { value } } }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    response = await client.execute_graphql(
        query=query,
        variables={
            "fabric_name": DCI_FABRIC_NAME,
            "pool_name": DCI_POOL_NAME,
            "seed_link_names": list(DCI_SEED_LINK_ENDPOINTS),
        },
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    fabric_edges = response["NetworkFabric"]["edges"]
    pool_edges = response["CoreIPPrefixPool"]["edges"]
    assert len(fabric_edges) == 1, f"{DCI_FABRIC_NAME} is not uniquely readable on {branch}"
    assert len(pool_edges) == 1, f"{DCI_POOL_NAME} is not unique on {branch}"

    fabric = fabric_edges[0]["node"]
    dci_pool_node = (fabric.get("dci_pool") or {}).get("node") or {}
    fabric_ip_pools = sorted(
        edge["node"]["name"]["value"] for edge in fabric.get("fabric_ip_pools", {}).get("edges", [])
    )
    pool = pool_edges[0]["node"]
    pool_resources = sorted(edge["node"]["prefix"]["value"] for edge in pool.get("resources", {}).get("edges", []))
    seed_links = {}
    for edge in response["NetworkLink"]["edges"]:
        node = edge["node"]
        link_name = node["name"]["value"]
        endpoints = set()
        for endpoint_edge in node.get("connected_endpoints", {}).get("edges", []):
            endpoint = endpoint_edge.get("node") or {}
            device = (endpoint.get("device") or {}).get("node") or {}
            if endpoint.get("__typename") == "InterfacePhysical" and device:
                endpoints.add((device["name"]["value"], endpoint["name"]["value"]))
        seed_links[link_name] = endpoints

    return {
        "fabric_id": fabric["id"],
        "dci_pool": {
            "id": dci_pool_node.get("id"),
            "name": (dci_pool_node.get("name") or {}).get("value"),
        },
        "fabric_ip_pools": fabric_ip_pools,
        "prefix_pool": {
            "id": pool["id"],
            "name": pool["name"]["value"],
            "default_prefix_length": (pool.get("default_prefix_length") or {}).get("value"),
            "resources": pool_resources,
        },
        "seed_links": seed_links,
    }


def _assert_dci_fabric_state(state: dict, *, branch: str) -> None:
    assert state["dci_pool"] == {
        "id": state["prefix_pool"]["id"],
        "name": DCI_POOL_NAME,
    }, f"{branch} fabric does not point to exactly one seeded DCI pool: {state['dci_pool']}"
    assert state["prefix_pool"]["default_prefix_length"] == 31, f"{DCI_POOL_NAME} does not allocate /31s on {branch}"
    assert state["prefix_pool"]["resources"] == [DCI_POOL_RESOURCE], (
        f"{DCI_POOL_NAME} resources differ on {branch}: {state['prefix_pool']['resources']}"
    )
    assert state["fabric_ip_pools"].count(DCI_POOL_NAME) == 1, (
        f"{DCI_POOL_NAME} is not referenced exactly once by fabric_ip_pools on {branch}: {state['fabric_ip_pools']}"
    )
    assert state["seed_links"] == DCI_SEED_LINK_ENDPOINTS, (
        f"seed DCI links or endpoints differ on {branch}: {state['seed_links']}"
    )


async def _run_generator_for_nodes(
    client: InfrahubClient, branch: str, generator_name: str, node_ids: list[str]
) -> None:
    gen_def = await client.get(kind="CoreGeneratorDefinition", name__value=generator_name, branch=branch)
    for node_id in node_ids:
        try:
            response = await client.execute_graphql(
                query="""
                mutation RunGenerator($id: String!, $nodes: [String!]!) {
                    CoreGeneratorDefinitionRun(data: { id: $id, nodes: $nodes }) {
                        ok
                    }
                }
                """,
                variables={"id": gen_def.id, "nodes": [node_id]},
                branch_name=branch,
                timeout=READ_TIMEOUT,
            )
        except TimeoutError, httpx.TimeoutException:
            print(
                f"launch request timed out for {generator_name} target {node_id} on {branch}; "
                "observing tasks and outputs before any retry",
                flush=True,
            )
            return
        result = response["CoreGeneratorDefinitionRun"]
        assert result["ok"], f"failed to launch {generator_name} on {branch} for target {node_id}: {result}"


async def _run_generator_for_nodes_sequentially(
    client: InfrahubClient,
    branch: str,
    generator_name: str,
    node_ids: list[str],
    *,
    describe: str,
) -> None:
    """Run each device generator only after the prior branch cascade settles."""
    settled = await wait_until_stable(
        fetch=lambda: _branch_task_snapshot(client, branch),
        ready=_branch_tasks_idle,
        timeout=GENERATOR_TIMEOUT,
        interval=POLL_INTERVAL,
        describe=f"{describe}: branch tasks before run",
    )
    for node_id in node_ids:
        prior_task_ids = {task["id"] for task in settled["tasks"]}
        await _run_generator_for_nodes(client, branch, generator_name, [node_id])
        settled = await wait_until_stable(
            fetch=lambda known_ids=prior_task_ids: _branch_task_snapshot(client, branch, known_task_ids=known_ids),
            ready=lambda snapshot, known_ids=prior_task_ids, target=node_id: _branch_tasks_ready_since(
                snapshot,
                known_ids,
                [target],
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"{describe}: completed task cascade for {node_id}",
        )


async def _dci_hostvars_report(
    client: InfrahubClient,
    branch: str,
    device_names: list[str],
    endpoints: list[tuple[str, str]] | None = None,
) -> dict:
    query = """
    query DciHostvars($names: [String!]) {
      DcimDevice(name__values: $names) {
        edges {
          node {
            name { value }
            avd_artifact {
              node {
                hostvar_file { node { storage_id { value } } }
              }
            }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(
        query=query,
        variables={"names": device_names},
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    hostvars = {}
    for edge in resp["DcimDevice"]["edges"]:
        node = edge["node"]
        storage_id = (
            node.get("avd_artifact", {})
            .get("node", {})
            .get("hostvar_file", {})
            .get("node", {})
            .get("storage_id", {})
            .get("value")
        )
        if not storage_id:
            continue
        content = await asyncio.wait_for(client.object_store.get(identifier=storage_id), timeout=READ_TIMEOUT)
        hostvars[node["name"]["value"]] = json.loads(content)

    hostvars = dict(sorted(hostvars.items()))
    all_links = []
    for device_hostvars in hostvars.values():
        all_links.extend(link for link in device_hostvars.get("l3_edge", {}).get("p2p_links", []))

    links_by_identity = {_dci_link_identity(link): link for link in all_links}
    unique_links = [links_by_identity[identity] for identity in sorted(links_by_identity, key=repr)]
    unique_matching_links = [
        link for link in unique_links if endpoints is None or dci_link_matches_endpoints(link, endpoints)
    ]

    return {
        "ready": set(hostvars) == set(device_names),
        "hostvars": hostvars,
        "links": unique_links,
        "unique_matching_links": unique_matching_links,
    }


def _dci_link_identity(link: dict) -> tuple:
    """Return an endpoint-order-independent identity for one rendered DCI p2p link."""
    nodes = link.get("nodes", [])
    interfaces = link.get("interfaces", [])
    as_values = link.get("as", [])
    ip_addresses = link.get("ip", [])
    if (
        isinstance(nodes, list)
        and isinstance(interfaces, list)
        and isinstance(as_values, list)
        and isinstance(ip_addresses, list)
        and len(nodes) == len(interfaces) == len(as_values) == len(ip_addresses)
    ):
        endpoint_data = tuple(
            sorted(zip(nodes, interfaces, as_values, ip_addresses, strict=True), key=itemgetter(0, 1))
        )
        return (
            endpoint_data,
            link.get("include_in_underlay_protocol"),
            link.get("profile"),
            link.get("speed"),
        )
    return (
        tuple(nodes) if isinstance(nodes, list) else nodes,
        tuple(interfaces) if isinstance(interfaces, list) else interfaces,
        tuple(as_values) if isinstance(as_values, list) else as_values,
        tuple(ip_addresses) if isinstance(ip_addresses, list) else ip_addresses,
        link.get("include_in_underlay_protocol"),
        link.get("profile"),
        link.get("speed"),
    )


async def _trigger_all_artifacts(client: InfrahubClient, branch: str, definitions: list) -> None:
    """Fire artifact generation for every definition on ``branch`` (fire-and-forget).

    The node ``.generate()`` helper posts without a branch (targets main, which has
    no devices in this flow), so hit the branch-aware endpoint directly.
    """
    for definition in definitions:
        resp = await client._post(
            f"{client.address}/api/artifact/generate/{definition.id}?branch={branch}",
            payload={"nodes": []},
        )
        resp.raise_for_status()


async def _fetch_ready_artifact_contents(client: InfrahubClient, branch: str, artifact_name: str) -> list[str]:
    """Return the content of every Ready artifact of ``artifact_name`` on ``branch``.

    Used for fabric-scoped artifacts where several targets (and possibly an early
    empty render) share one name, so the caller can pick a populated one.
    """
    query = (
        "query {\n"
        f'  CoreArtifact(name__value: "{artifact_name}") {{\n'
        "    edges { node { status { value } storage_id { value } } }\n"
        "  }\n"
        "}"
    )
    resp = await client.execute_graphql(query=query, branch_name=branch)
    contents: list[str] = []
    for edge in resp["CoreArtifact"]["edges"]:
        node = edge["node"]
        if node.get("status", {}).get("value") != "Ready":
            continue
        storage_id = node.get("storage_id", {}).get("value")
        if storage_id:
            contents.append(await client.object_store.get(identifier=storage_id))
    return contents


async def _fetch_ready_artifact_contents_by_target(
    client: InfrahubClient,
    branch: str,
    artifact_name: str,
    target_ids_by_name: dict[str, str],
) -> dict[str, str]:
    """Read Ready artifacts only for the requested device IDs."""
    query = """
    query ReadyArtifactsByTarget($artifact_name: String!, $target_ids: [ID]) {
      CoreArtifact(name__value: $artifact_name, object__ids: $target_ids) {
        edges {
          node {
            status { value }
            storage_id { value }
            object { node { id display_label } }
          }
        }
      }
    }
    """
    response = await client.execute_graphql(
        query=query,
        variables={"artifact_name": artifact_name, "target_ids": list(target_ids_by_name.values())},
        branch_name=branch,
        timeout=READ_TIMEOUT,
    )
    contents: dict[str, str] = {}
    target_names_by_id = {node_id: name for name, node_id in target_ids_by_name.items()}
    for edge in response["CoreArtifact"]["edges"]:
        node = edge["node"]
        if node.get("status", {}).get("value") != "Ready":
            continue
        storage_id = node.get("storage_id", {}).get("value")
        target = (node.get("object") or {}).get("node") or {}
        target_name = target_names_by_id.get(target.get("id"))
        if storage_id and target_name:
            contents[target_name] = await client.object_store.get(identifier=storage_id)
    return contents


async def _fabric_device_ids_by_name(
    client: InfrahubClient,
    branch: str,
    fabric_name: str,
) -> dict[str, str]:
    """Return device IDs keyed by name for one fabric."""
    query = """
    query {
      DcimDevice {
        edges {
          node {
            id
            name { value }
            pod {
              node {
                parent {
                  node {
                    __typename
                    id
                    ... on NetworkFabric { name { value } }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    response = await client.execute_graphql(query=query, branch_name=branch, timeout=READ_TIMEOUT)
    devices: dict[str, str] = {}
    for edge in response["DcimDevice"]["edges"]:
        node = edge["node"]
        parent = (((node.get("pod") or {}).get("node") or {}).get("parent") or {}).get("node") or {}
        if parent.get("__typename") != "NetworkFabric":
            continue
        if (parent.get("name") or {}).get("value") == fabric_name:
            devices[node["name"]["value"]] = node["id"]
    return devices


async def _fetch_ready_artifact_content(
    client: InfrahubClient, branch: str, artifact_name: str
) -> tuple[str | None, str | None]:
    """Return (content, target_display_label) for one Ready artifact of ``artifact_name``.

    Reads content straight from the object store so it does not depend on any
    particular device having the artifact. Returns (None, None) if none is Ready.
    """
    query = (
        "query {\n"
        f'  CoreArtifact(name__value: "{artifact_name}") {{\n'
        "    edges { node {\n"
        "      status { value }\n"
        "      storage_id { value }\n"
        "      object { node { display_label } }\n"
        "    } }\n"
        "  }\n"
        "}"
    )
    resp = await client.execute_graphql(query=query, branch_name=branch)
    for edge in resp["CoreArtifact"]["edges"]:
        node = edge["node"]
        if node.get("status", {}).get("value") != "Ready":
            continue
        storage_id = node.get("storage_id", {}).get("value")
        if not storage_id:
            continue
        target = (node.get("object") or {}).get("node") or {}
        content = await client.object_store.get(identifier=storage_id)
        return content, target.get("display_label")
    return None, None
