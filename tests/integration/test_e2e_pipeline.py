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

The container stack and lazy prerequisite fixtures are class-scoped. Each downstream test simply waits (bounded) for the stage's
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
import time
from ipaddress import ip_interface, ip_network
from operator import itemgetter
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
import yaml
from infrahub_sdk.branch import BranchStatus
from infrahub_sdk.protocols import CoreGenericRepository
from infrahub_sdk.task import TaskFilter
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
    GENERATOR_FABRIC,
    GENERATOR_RACK,
    GENERATOR_TIMEOUT,
    GROUP_TIMEOUT,
    L2LS_MLAG_ROLES,
    POLL_INTERVAL,
    REPO_SYNC_INTERVAL,
    REPO_SYNC_RETRIES,
    create_branch,
    device_design_mismatches,
    expected_super_spine_count,
    fabric_deployment_report,
    read_with_retry,
    retry_after_delay,
    storage_content,
    wait_until,
)
from .stack import IntegrationStack

if TYPE_CHECKING:
    from pathlib import Path

    from infrahub_sdk import InfrahubClient

REPO_NAME = "e2e-repository"
# The cascade runs on this branch; trigger rules only fire off `main`.
PIPELINE_BRANCH = "e2e-pipeline"
GENERATOR_AVD_HOSTVAR = "generate-avd-device-hostvar"
GENERATOR_AVD_STRUCTURED_CONFIG = "generate-avd-device-structured-config"
SERVER_CABLING_RACK = "Rack-B2-1"
SERVER_CABLING_SERVER = "e2e-server-b2-1"
SERVER_CABLING_TEMPLATE = "multipod-b-compute-server-dual"
DCI_FABRIC = "Fabric-L3LS-Multi-Domain"
DCI_POOL_PREFIX = "172.16.0.0/28"
DCI_POOL_NAME = f"{DCI_FABRIC}-DCI-Pool"
DCI_LINK_NAME = "e2e-dci-link"
DCI_INTERFACE_NAME = "Ethernet999"
FILTERED_ANTA_FABRIC = "Fabric-L3LS-Multi-Domain"
FILTERED_ANTA_TESTS = ("VerifyInterfaceDiscards", "VerifyLoggingErrors", "VerifyRunningConfigDiffs")
PROPOSED_CHANGE_ANTA_FABRIC = "Fabric-L3LS-MultiPod-A"


@pytest.mark.e2e
class TestE2EPipeline(IntegrationStack):
    """Trigger-driven design-to-artifact pipeline against a real Infrahub instance.

    Each node ID selects its prerequisites through lazy fixtures. The shared
    baseline cascades through triggers; graph-changing scenarios fork that stable
    state so selection and execution order cannot contaminate other scenarios.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _client_timeout(self) -> None:
        # Raise the infrahubctl client timeout: the fabric generator issues large
        # GraphQL reads/writes that can exceed the 60s default under load.
        os.environ.setdefault("INFRAHUB_TIMEOUT", "300")

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def schema_ready(self, default_branch: str, client: InfrahubClient, schemas: list[dict]):
        await self._prepare_schema(default_branch, client, schemas)
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def seeds_ready(self, schema_ready: bool, default_branch: str, client: InfrahubClient, infrahub_port: int):
        await self._prepare_objects(default_branch, client, infrahub_port)
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def repository_ready(
        self,
        seeds_ready: bool,
        default_branch: str,
        client: InfrahubClient,
        repository_source: Path,
        remote_repos_dir: Path,
    ):
        await self._prepare_repository(default_branch, client, repository_source, remote_repos_dir)
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def triggers_ready(self, repository_ready: bool, infrahub_port: int):
        await self._prepare_triggers(infrahub_port)
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def branch_ready(self, triggers_ready: bool, client: InfrahubClient):
        await self._prepare_branch(client)
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def cascade_started(self, branch_ready: bool, client: InfrahubClient):
        before = {task.id for task in await _pipeline_tasks(client, PIPELINE_BRANCH)}
        await self._prepare_fabric(client)
        return {"previous_tasks": before}

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def pipeline_ready(self, cascade_started: dict, client: InfrahubClient):
        await _wait_for_tasks(client, PIPELINE_BRANCH, cascade_started["previous_tasks"])
        await self.test_structured_config_cascade(client)
        await wait_until(
            fetch=lambda: _rack_rerun_snapshot(client, PIPELINE_BRANCH, contents=False),
            ready=itemgetter("ready"),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="baseline rack and backfill cascade settled",
        )
        return True

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def artifacts_ready(self, pipeline_ready: bool, client: InfrahubClient, default_branch: str):
        baseline = await client.branch.get(PIPELINE_BRANCH)
        branch = default_branch if baseline.status == BranchStatus.MERGED else PIPELINE_BRANCH
        await self._prepare_artifacts(client, branch)
        return branch

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def pipeline_published(self, pipeline_ready: bool, client: InfrahubClient, default_branch: str):
        # Infrahub 1.11 only supports branches originating from the default
        # branch. Publish once inside this temporary stack, after its cascade
        # settles, so every scenario inherits the same generated graph.
        original = await _scenario_isolation_state(client, PIPELINE_BRANCH)
        assert await client.branch.merge(branch_name=PIPELINE_BRANCH), "baseline merge failed"
        assert await _scenario_isolation_state(client, default_branch) == original
        return default_branch

    @pytest_asyncio.fixture(loop_scope="class")
    async def scenario_branch(self, pipeline_published: str, client: InfrahubClient, request: pytest.FixtureRequest):
        name = request.node.name.removeprefix("test_").replace("_", "-")
        original = await _scenario_isolation_state(client, pipeline_published)
        branch = await create_branch(client, name, pipeline_published)
        try:
            yield branch
        finally:
            await _restore_shared_fabric_fields(client, pipeline_published, original)
            assert await _scenario_isolation_state(client, pipeline_published) == original, (
                f"Scenario {name} leaked into baseline"
            )

    @pytest_asyncio.fixture(loop_scope="class")
    async def anta_scenario_branch(self, pipeline_published: str, client: InfrahubClient):
        # A proposed change compares with the destination's current value.
        # Prepare a disabled destination before forking, then restore the exact
        # published baseline after the scenario. Other tests retain enabled ANTA.
        original = await _scenario_isolation_state(client, pipeline_published)
        fabric = await client.get(
            kind="NetworkFabric", name__value=PROPOSED_CHANGE_ANTA_FABRIC, branch=pipeline_published
        )
        fabric.anta_enabled.value = False
        try:
            await fabric.save()
            yield await create_branch(client, "proposed-change-anta-refresh", pipeline_published)
        finally:
            await _restore_shared_fabric_fields(client, pipeline_published, original)
            assert await _scenario_isolation_state(client, pipeline_published) == original, (
                "ANTA scenario leaked into baseline"
            )

    @staticmethod
    def _address(infrahub_port: int) -> str:
        return f"http://localhost:{infrahub_port}"

    # --- Component 1: schema (main) ----------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_schema(self, schema_ready: bool) -> None:
        assert schema_ready

    async def _prepare_schema(self, default_branch: str, client: InfrahubClient, schemas: list[dict]) -> None:
        """Load this repository's schemas and wait for convergence (FR-002)."""
        await client.schema.wait_until_converged(branch=default_branch)
        resp = await client.schema.load(schemas=schemas, branch=default_branch, wait_until_converged=True)
        assert resp.errors == {}, f"schema load errors: {resp.errors}"
        await client.schema.wait_until_converged(branch=default_branch)

    # --- Component 2: objects (main) ---------------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_objects(self, seeds_ready: bool) -> None:
        assert seeds_ready

    async def _prepare_objects(self, default_branch: str, client: InfrahubClient, infrahub_port: int) -> None:
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
    @pytest.mark.usefixtures("seeds_ready")
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
    async def test_register_repository(self, repository_ready: bool) -> None:
        assert repository_ready

    async def _prepare_repository(
        self, default_branch: str, client: InfrahubClient, repository_source: Path, remote_repos_dir: Path
    ) -> None:
        """Register this repository and wait for in-sync, failing fast on error (FR-005)."""
        repo = GitRepo(name=REPO_NAME, src_directory=repository_source, dst_directory=remote_repos_dir)
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
    async def test_load_triggers(self, triggers_ready: bool) -> None:
        assert triggers_ready

    async def _prepare_triggers(self, infrahub_port: int) -> None:
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
    async def test_create_branch_and_enable_anta(self, branch_ready: bool) -> None:
        assert branch_ready

    async def _prepare_branch(self, client: InfrahubClient) -> None:
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
    async def test_fabric_generator_creates_super_spines(self, cascade_started: bool) -> None:
        assert cascade_started

    async def _prepare_fabric(self, client: InfrahubClient) -> None:
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
        fabric_ids = [fabric.id for fabric in sorted(fabrics, key=lambda fabric: fabric.name.value)]
        assert fabric_ids, "no fabrics found on the pipeline branch"
        # Bound fanout to two independent fabrics. Waiting for all seven in
        # series dominates bootstrap time; kicking off all seven at once floods
        # the CI-sized API. Every target and downstream trigger remains exercised.
        for offset in range(0, len(fabric_ids), 2):
            batch = fabric_ids[offset : offset + 2]
            before = {task.id for task in await _pipeline_tasks(client, PIPELINE_BRANCH)}
            await client.execute_graphql(
                query="""
                mutation RunGenerator($id: String!, $nodes: [String!]!) {
                    CoreGeneratorDefinitionRun(data: { id: $id, nodes: $nodes }) {
                        ok
                    }
                }
                """,
                variables={"id": gen_def.id, "nodes": batch},
                branch_name=PIPELINE_BRANCH,
            )
            await _wait_for_tasks(
                client,
                PIPELINE_BRANCH,
                before,
                expected_runs={GENERATOR_FABRIC: len(batch), GENERATOR_AVD_STRUCTURED_CONFIG: len(batch)},
            )
            print(f"Fabric cascades settled: {batch}", flush=True)

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
    @pytest.mark.usefixtures("cascade_started")
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
    @pytest.mark.usefixtures("cascade_started")
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
    @pytest.mark.usefixtures("pipeline_ready")
    @pytest.mark.asyncio(loop_scope="class")
    async def test_devices_match_device_designs(self, client: InfrahubClient) -> None:
        """Every pod/rack produced exactly the devices its device designs declare.

        Device designs are the only source of device sizing, so this is the parity
        guard for the whole chain: a design quantity that stops being honoured, or
        devices created for a role with no design, fails here.
        """
        mismatches = await read_with_retry(lambda: device_design_mismatches(client, PIPELINE_BRANCH))
        assert mismatches == [], "device/design mismatches: " + "; ".join(mismatches)

    # --- Component 9b: BGP ASN nodes (regression 002) ----------------------
    @pytest.mark.usefixtures("pipeline_ready")
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
                and all(d["asn_node_id"] for d in r["devices"] if d["role"] in ("super_spine", "spine", "leaf"))
            ),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="RoutingAsn nodes created and linked to every L3 device",
        )

        # SC-001 / C1: nodes exist and every one is owned by a fabric.
        assert report["asns"], "no RoutingAsn nodes created by fabric generation"
        assert all(a["fabric_id"] for a in report["asns"]), "a RoutingAsn node has no owning fabric"

        # SC-002: every L3 device (super-spine/spine/leaf) resolves an ASN node.
        l3 = [d for d in report["devices"] if d["role"] in ("super_spine", "spine", "leaf")]
        assert l3, "no L3 devices found"
        unlinked = [d["name"] for d in l3 if not d["asn_node_id"]]
        assert not unlinked, f"L3 devices without an ASN node: {unlinked}"

        # C2 / SC-004: each MLAG domain shares one ASN node with both of its peers.
        asn_by_device = {d["id"]: d["asn_node_id"] for d in report["devices"]}
        for dom in report["domains"]:
            dom_asn = dom["asn_node_id"]
            peer_roles = {next(d["role"] for d in report["devices"] if d["id"] == pid) for pid in dom["peer_ids"]}
            if peer_roles == {"l2spine"}:
                assert dom["peer_ids"], f"L2 MLAG domain {dom['domain_id']} has no peers"
                assert dom_asn is None, f"Pure L2 MLAG domain {dom['domain_id']} unexpectedly has an ASN"
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
    @pytest.mark.usefixtures("pipeline_ready")
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
    async def test_server_cabling_trigger_creates_links_and_lags(
        self, client: InfrahubClient, scenario_branch: str
    ) -> None:
        """Creating a server in the servers group triggers server cabling and creates links/LAGs."""
        await wait_until(
            fetch=lambda: _server_cabling_prerequisites(client, scenario_branch),
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

        rack = await client.get(kind="LocationRack", name__value=SERVER_CABLING_RACK, branch=scenario_branch)
        template = await client.get(
            kind="TemplateComputePhysicalServer",
            template_name__value=SERVER_CABLING_TEMPLATE,
            branch=scenario_branch,
        )
        server = await client.create(
            kind="ComputePhysicalServer",
            branch=scenario_branch,
            name=SERVER_CABLING_SERVER,
            rack={"id": rack.id},
            object_template={"id": template.id},
            status="provisioning",
            member_of_groups=["servers"],
        )
        before = {task.id for task in await _pipeline_tasks(client, scenario_branch)}
        await server.save(allow_upsert=True)
        await _wait_for_tasks(client, scenario_branch, before)
        report = await read_with_retry(lambda: _server_cabling_report(client, scenario_branch, SERVER_CABLING_SERVER))

        # The completed trigger cascade makes these exact quantities final;
        # a model mismatch should fail here instead of consuming a polling timeout.
        assert report["server_physical_count"] == 2, report
        assert report["server_connected_count"] == 2, report
        assert report["server_bond_count"] == 1, report

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
        # MultiPod-B now uses the Arista template: Ethernet1 determines ID 1.
        assert report["leaf_port_channel_ids"] == [1, 1]
        print(f"server cabling report: {report}", flush=True)

    # --- Component 11: AVD structured config (via trigger cascade) ---------
    @pytest.mark.usefixtures("cascade_started")
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
    async def test_hostvar_generator_two_runs_are_idempotent(
        self, client: InfrahubClient, scenario_branch: str
    ) -> None:
        """Two explicit hostvar runs produce identical normalized files and checksums."""
        device_ids = await _device_ids_for_fabric(client, scenario_branch, FILTERED_ANTA_FABRIC)
        assert device_ids, "no devices found for hostvar idempotence validation"

        snapshots = []
        for pass_number in (1, 2):
            await _run_generator_for_nodes(client, scenario_branch, GENERATOR_AVD_HOSTVAR, device_ids)
            report = await wait_until(
                fetch=lambda: _hostvar_snapshot(client, scenario_branch, device_ids, contents=False),
                ready=itemgetter("ready"),
                timeout=GENERATOR_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"hostvar generator pass {pass_number} settled",
            )
            report = await _hostvar_snapshot(client, scenario_branch, device_ids)
            snapshots.append(report["state"])

        assert snapshots[1] == snapshots[0], "hostvar generator rerun changed normalized hostvar files"

    # --- Component 11a: backfilled IP uniqueness --------------------------
    @pytest.mark.usefixtures("pipeline_ready")
    @pytest.mark.asyncio(loop_scope="class")
    async def test_backfilled_ip_assignments_are_unique(self, client: InfrahubClient) -> None:
        """Backfilled IPAM entries have unique address/namespace keys and interface assignments."""
        report = await wait_until(
            fetch=lambda: _ipam_duplicate_report(client, PIPELINE_BRANCH),
            ready=lambda r: r["total"] > 0 and r["assigned"] > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="unique backfilled IPAM assignments",
        )

        assert report["primary_assigned"] > 0, "no primary interface IP assignments were read"
        assert not report["duplicate_ip_keys"], f"duplicate IpamIPAddress keys: {report['duplicate_ip_keys']}"
        assert not report["duplicate_interface_ips"], (
            f"duplicate interface-side IP assignments: {report['duplicate_interface_ips']}"
        )
        print(
            f"ipam duplicate report: total={report['total']} assigned={report['assigned']} "
            f"primary_assigned={report['primary_assigned']} shared_mlag_vteps={len(report['shared_mlag_vteps'])}",
            flush=True,
        )

    # --- Component 11b: rack generator rerun regression -------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_rack_generators_rerun_without_error_or_drift(
        self, client: InfrahubClient, scenario_branch: str, tmp_directory: Path
    ) -> None:
        """Explicit rack reruns, including DC1_BORDER, do not fail or mutate generated state."""
        racks = await client.all(kind="LocationRack", branch=scenario_branch)
        rack_targets = sorted((rack.name.value, rack.id) for rack in racks)
        assert any(name == "DC1_BORDER" for name, _ in rack_targets), "DC1_BORDER rack not found"
        fabrics = await client.all(kind="NetworkFabric", branch=scenario_branch)

        snapshots = []
        for pass_number in (1, 2):
            await _run_generator_for_nodes(
                client,
                scenario_branch,
                GENERATOR_RACK,
                [rack_id for _, rack_id in rack_targets],
                cascade_runs={GENERATOR_AVD_STRUCTURED_CONFIG: len(fabrics)},
            )
            snapshot = await wait_until(
                fetch=lambda: _rack_rerun_snapshot(client, scenario_branch, contents=False),
                ready=itemgetter("ready"),
                timeout=GENERATOR_TIMEOUT,
                interval=POLL_INTERVAL,
                describe=f"rack generator rerun pass {pass_number} settled without Error",
            )
            snapshot = await _rack_rerun_snapshot(client, scenario_branch)
            snapshots.append(snapshot["state"])
            (tmp_directory / f"rack-pass-{pass_number}.json").write_text(json.dumps(snapshot["state"], indent=2))
            print(
                f"rack rerun pass {pass_number}: racks={len(rack_targets)} "
                f"rack_instances={len(snapshot['state']['rack_generator_instances'])}",
                flush=True,
            )

        assert snapshots[1] == snapshots[0], _snapshot_difference(snapshots[0], snapshots[1])

    # --- Component 11b: DCI link hostvars ----------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_dci_link_generates_l3_edge_hostvars(self, client: InfrahubClient, scenario_branch: str) -> None:
        """Both DCs' border pairs retain seed DCI links and allocate a new /31."""
        setup = await _create_dci_link_scenario(client, scenario_branch)
        await _run_generator_for_nodes(client, scenario_branch, GENERATOR_AVD_HOSTVAR, setup["device_ids"])

        report = await wait_until(
            fetch=lambda: _dci_hostvars_report(client, scenario_branch, setup["device_names"]),
            ready=lambda r: r["ready"] and len(r["unique_matching_links"]) == len(setup["expected_links"]),
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"DCI l3_edge hostvars for {DCI_LINK_NAME}",
        )

        networks = set()
        expected_interface_ips = {}
        actual_links = {
            (tuple(link["nodes"]), tuple(link["interfaces"])): link for link in report["unique_matching_links"]
        }
        assert set(actual_links) == set(setup["expected_links"])
        for key, expected_as in setup["expected_links"].items():
            link = actual_links[key]
            assert link["as"] == expected_as
            addresses = [ip_interface(address) for address in link["ip"]]
            assert len(addresses) == 2
            network = addresses[0].network
            assert network.prefixlen == 31 and addresses[1].network == network
            assert network.subnet_of(ip_network(DCI_POOL_PREFIX))
            assert [address.ip for address in addresses] == list(network.hosts())
            assert network not in networks, f"DCI links share {network}"
            networks.add(network)
            assert link["include_in_underlay_protocol"] is True
            assert "profile" not in link
            # Each endpoint's actual stored hostvars must contain its complete link.
            assert all(link in report["hostvars"][name]["l3_edge"]["p2p_links"] for name in link["nodes"])
            expected_interface_ips.update(
                zip(zip(link["nodes"], link["interfaces"], strict=True), link["ip"], strict=True)
            )
        assert all("p2p_links_profiles" not in hostvars.get("l3_edge", {}) for hostvars in report["hostvars"].values())
        # Materialize every single-valued relation touched by the scenario; an
        # accidental second peer produces a GraphQL cardinality error here.
        fabrics = await client.all(kind="NetworkFabric", branch=scenario_branch, include=["dci_pool"])
        assert next(fabric for fabric in fabrics if fabric.id == setup["fabric_id"]).dci_pool.id == setup["pool_id"]
        endpoints = await client.filters(
            kind="DcimDevice",
            name__values=setup["device_names"],
            branch=scenario_branch,
            include=["asn", "pod", "avd_artifact"],
        )
        assert len(endpoints) == 4
        assert all(device.role.value == "border_leaf" for device in endpoints)
        assert len({device.pod.id for device in endpoints}) == 2
        interfaces = await client.filters(
            kind="InterfacePhysical",
            name__values=["Ethernet5", "Ethernet6", DCI_INTERFACE_NAME],
            device__ids=setup["device_ids"],
            branch=scenario_branch,
            include=["device", "connector", "ip_address"],
        )
        assert len(interfaces) == 10
        assert len({interface.connector.id for interface in interfaces}) == 5
        assert all(interface.ip_address.id for interface in interfaces)
        device_names = {device.id: device.name.value for device in endpoints}
        assert {
            (device_names[interface.device.id], interface.name.value): str(interface.ip_address.peer.address.value)
            for interface in interfaces
        } == expected_interface_ips
        print(f"dci hostvars report: {report}", flush=True)

    # --- Component 12: artifacts generated ---------------------------------
    @pytest.mark.asyncio(loop_scope="class")
    async def test_artifacts_generated(self, artifacts_ready: str) -> None:
        assert artifacts_ready

    async def _prepare_artifacts(self, client: InfrahubClient, branch: str) -> None:
        """Every AVD artifact definition yields at least one Ready artifact on the branch (FR-011/FR-013)."""
        definitions = await client.all(kind="CoreArtifactDefinition", branch=branch)
        assert definitions, "no artifact definitions registered (repository sync incomplete?)"
        tasks = await _pipeline_tasks(client, branch)
        # Settle automatic renders, then refresh every definition against the
        # completed graph. Ready outputs can predate device/link generation.
        previous_tasks = {task.id for task in tasks if task.state == "COMPLETED"}
        if any(task.state != "COMPLETED" for task in tasks):
            await _wait_for_tasks(client, branch, previous_tasks)
        existing = await client.all(kind="CoreArtifact", branch=branch, prefetch_relationships=False)
        failed = [a.name.value for a in existing if a.status.value == "Error"]
        assert not failed, f"Artifact generation failed: {failed}"
        await _trigger_all_artifacts(client, branch, definitions)

        started = time.monotonic()
        retriggered = False
        quiet = 0

        async def artifact_state() -> tuple[list[str], bool]:
            nonlocal quiet
            tasks = await _execution_tasks(client, branch, previous_tasks)
            quiet = quiet + 1 if tasks and all(task.state == "COMPLETED" for task in tasks) else 0
            artifacts = await client.all(kind="CoreArtifact", branch=branch, prefetch_relationships=False)
            failed = [a.name.value for a in artifacts if a.status.value == "Error"]
            assert not failed, f"Artifact generation failed: {failed}"
            ready = _ready_artifact_names(artifacts)
            return sorted(set(ALL_ARTIFACT_NAMES) - ready), quiet >= 2

        async def retry_missing(state: tuple[list[str], bool]) -> None:
            nonlocal retriggered
            missing, _ = state
            if not retriggered and time.monotonic() - started >= ARTIFACT_TIMEOUT / 2:
                retriggered = True
                await _trigger_all_artifacts(
                    client, branch, [d for d in definitions if d.artifact_name.value in missing]
                )

        await wait_until(
            fetch=artifact_state,
            ready=lambda state: not state[0] and state[1],
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="all artifact definitions produced Ready artifacts",
            on_pending=retry_missing,
        )

    # --- Component 13: artifact content ------------------------------------
    @pytest.mark.usefixtures("artifacts_ready")
    @pytest.mark.asyncio(loop_scope="class")
    async def test_artifact_content(self, client: InfrahubClient, artifacts_ready: str) -> None:
        """A rendered EOS config mentions its device hostname; the ANTA catalog is populated (FR-012)."""
        eos_contents = await _fetch_ready_artifact_contents(client, artifacts_ready, ARTIFACT_AVD_EOS_CONFIG)
        assert any("hostname" in content for content in eos_contents), "No rendered EOS configuration has a hostname"
        eos_content = next(content for content in eos_contents if "hostname" in content)
        assert eos_content.strip(), "EOS configuration artifact is empty"

        anta_content, _ = await _fetch_ready_artifact_content(client, artifacts_ready, ARTIFACT_AVD_ANTA_CATALOG)
        assert anta_content and anta_content.strip(), "ANTA catalog artifact is empty"
        assert ANTA_DISABLED_MARKER not in anta_content, (
            "ANTA catalog rendered the disabled marker despite anta_enabled"
        )

        def _has_populated_topology(contents: list[str]) -> bool:
            for content in contents:
                if not content or not content.strip():
                    continue
                topology = yaml.safe_load(content).get("topology") or {}
                if topology.get("nodes") and topology.get("links"):
                    return True
            return False

        clab_contents = await _fetch_ready_artifact_contents(client, artifacts_ready, ARTIFACT_CONTAINERLAB_TOPOLOGY)
        assert _has_populated_topology(clab_contents), "No populated ContainerLab topology artifact on the branch"
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
    async def test_proposed_change_refreshes_anta_catalogs_for_fabric_flag(
        self, client: InfrahubClient, anta_scenario_branch: str
    ) -> None:
        """Enabling ANTA on a generated fabric refreshes its device catalogs in the proposed change."""
        scenario_branch = anta_scenario_branch
        fabric = await client.get(
            kind="NetworkFabric",
            name__value=PROPOSED_CHANGE_ANTA_FABRIC,
            branch=scenario_branch,
        )
        device_ids_by_name = await _fabric_device_ids_by_name(
            client,
            scenario_branch,
            PROPOSED_CHANGE_ANTA_FABRIC,
        )
        assert device_ids_by_name, f"{PROPOSED_CHANGE_ANTA_FABRIC} has no generated devices"

        definition = await client.get(
            kind="CoreArtifactDefinition",
            name__value="avd_anta_catalog",
            branch=scenario_branch,
        )
        catalog_ids = await _artifact_ids_for_targets(
            client, scenario_branch, ARTIFACT_AVD_ANTA_CATALOG, set(device_ids_by_name.values())
        )

        # Establish disabled-marker catalogs before the proposed change. This
        # makes the subsequent marker -> populated transition prove that the
        # proposed-change artifact validator actually re-rendered the devices.
        previous_tasks = {task.id for task in await _pipeline_tasks(client, scenario_branch)}
        fabric.anta_enabled.value = False
        await fabric.save()
        response = await client._post(
            f"{client.address}/api/artifact/generate/{definition.id}?branch={scenario_branch}",
            payload={"nodes": catalog_ids},
        )
        response.raise_for_status()
        await _wait_for_tasks(client, scenario_branch, previous_tasks)
        # Unchanged disabled catalogs may retain their immutable storage IDs.
        # The completed new task plus the actual marker establishes this state.
        disabled_storage = await wait_until(
            fetch=lambda: _fetch_ready_artifact_storage_by_target(client, scenario_branch, ARTIFACT_AVD_ANTA_CATALOG),
            ready=lambda storage: set(device_ids_by_name) <= storage.keys(),
            timeout=ARTIFACT_TIMEOUT,
            interval=POLL_INTERVAL,
            describe="Ready disabled ANTA catalogs after explicit generation",
        )
        disabled = {name: await storage_content(client, identifier) for name, identifier in disabled_storage.items()}
        assert all(ANTA_DISABLED_MARKER in disabled.get(name, "") for name in device_ids_by_name)

        previous_tasks = {task.id for task in await _pipeline_tasks(client, scenario_branch)}
        fabric.anta_enabled.value = True
        await fabric.save()
        destination = await client.get(
            kind="NetworkFabric", name__value=PROPOSED_CHANGE_ANTA_FABRIC, branch="main", include=["anta_enabled"]
        )
        assert destination.anta_enabled.value is False, "ANTA enable leaked into proposed-change destination"
        proposed_change = await client.create(
            kind="CoreProposedChange",
            data={
                "source_branch": scenario_branch,
                "destination_branch": "main",
                "name": "e2e-enable-anta",
            },
        )
        await proposed_change.save()
        await _wait_for_tasks(client, scenario_branch, previous_tasks)
        refreshed_storage = await _wait_for_new_artifact_storage(
            client, scenario_branch, ARTIFACT_AVD_ANTA_CATALOG, set(device_ids_by_name), disabled_storage
        )
        refreshed = {name: await storage_content(client, identifier) for name, identifier in refreshed_storage.items()}
        assert all(
            "anta.tests." in refreshed.get(name, "") and ANTA_DISABLED_MARKER not in refreshed.get(name, "")
            for name in device_ids_by_name
        )
        assert set(device_ids_by_name) <= set(refreshed)

    # --- Standalone L2LS deployment validation ---------------------------
    @pytest.mark.usefixtures("artifacts_ready")
    @pytest.mark.asyncio(loop_scope="class")
    async def test_l2ls_deployment(self, client: InfrahubClient, artifacts_ready: str) -> None:
        """Validate L2LS roles, both MLAG tiers and pure Layer-2 EOS by default.

        Reuse the class's generated baseline and artifacts; no separate stack
        or fabric selector is needed.
        """
        fabric_name = "Fabric-L2LS"
        report = await wait_until(
            fetch=lambda: fabric_deployment_report(client, PIPELINE_BRANCH, fabric_name),
            ready=lambda r: r is None or r["device_count"] > 0,
            timeout=GENERATOR_TIMEOUT,
            interval=POLL_INTERVAL,
            describe=f"generated devices for fabric {fabric_name!r}",
        )
        assert report is not None, f"{fabric_name!r} not found on {PIPELINE_BRANCH}"
        assert report["device_count"] > 0, f"fabric {fabric_name!r} generated no devices"

        expected_roles = {"l2spine", "l2leaf"}
        got_roles = set(report["roles"])
        assert expected_roles <= got_roles, (
            f"{fabric_name}: expected roles {sorted(expected_roles)}, got {sorted(got_roles)}"
        )

        assert report["mlag_roles"] >= L2LS_MLAG_ROLES, (
            f"L2LS MLAG missing a tier: expected {sorted(L2LS_MLAG_ROLES)}, got {sorted(report['mlag_roles'])}"
        )
        await _assert_pure_layer2_configs(client, artifacts_ready, report["roles"])


async def _scenario_isolation_state(client: InfrahubClient, branch: str) -> dict:
    response = await read_with_retry(
        lambda: client.execute_graphql(
            query="""query {
      NetworkFabric { edges { node { id anta_enabled { value } dci_pool { node { id } } } } }
      CoreIPPrefixPool { edges { node { id name { value } } } }
      DcimDevice { edges { node { id role { value } } } }
      ComputePhysicalServer { edges { node { id name { value } } } }
      InterfacePhysical(name__value: "Ethernet999") { edges { node { id connector { node { id } } } } }
    }""",
            branch_name=branch,
        )
    )
    # Relationship iteration order is not an isolation property.
    for collection in response.values():
        collection["edges"].sort(key=lambda edge: edge["node"]["id"])
    return response


async def _restore_shared_fabric_fields(client: InfrahubClient, branch: str, original: dict) -> None:
    """Restore prepared baseline flags and the shared DCI field after scenarios."""
    current = await _scenario_isolation_state(client, branch)
    actual = {edge["node"]["id"]: edge["node"] for edge in current["NetworkFabric"]["edges"]}
    for edge in original["NetworkFabric"]["edges"]:
        node = edge["node"]
        updates = {key: node[key] for key in ("anta_enabled", "dci_pool") if actual[node["id"]][key] != node[key]}
        if not updates:
            continue
        if "dci_pool" in updates:
            peer = updates["dci_pool"]["node"]
            updates["dci_pool"] = {"id": peer["id"]} if peer else None
        await client.execute_graphql(
            query="""mutation($data: NetworkFabricUpsertInput!) {
              NetworkFabricUpsert(data: $data) { ok }
            }""",
            variables={"data": {"id": node["id"], **updates}},
            branch_name=branch,
        )
    initial_pool_ids = {edge["node"]["id"] for edge in original["CoreIPPrefixPool"]["edges"]}
    for edge in current["CoreIPPrefixPool"]["edges"]:
        pool = edge["node"]
        if pool["id"] not in initial_pool_ids and pool["name"]["value"] == DCI_POOL_NAME:
            await client.delete(kind="CoreIPPrefixPool", id=pool["id"], branch=branch)


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
    query IpamDuplicateReport($offset: Int, $limit: Int, $details: Boolean!) {
      IpamIPAddress(offset: $offset, limit: $limit) {
        count
        edges {
          node {
            id
            address { value }
            ip_namespace { node { name { value } } }
            interface { node { id display_label } }
          }
        }
      }
      DcimInterface(offset: $offset, limit: $limit, ip_address__isnull: false) @include(if: $details) {
        count
        edges {
          node {
            id
            __typename
            name { value }
            role { value }
            device {
              node {
                ... on DcimDevice {
                  id
                  vtep_loopback_ip { node { id } }
                  mlag_domain { node { id peers { edges { node { id } } } } }
                }
              }
            }
            ip_address {
              node {
                id
                address { value }
                ip_namespace { node { name { value } } }
              }
            }
          }
        }
      }
    }
    """
    resp = await _snapshot_collections(client, branch, query, contents=True)
    key_to_ip_ids: dict[tuple[str, str], list[str]] = {}
    key_to_interface_ids: dict[tuple[str, str], set[str]] = {}
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
            key_to_interface_ids.setdefault(key, set()).add(interface["id"])

    primary_assigned = 0
    primary_interfaces = {}
    for edge in resp["DcimInterface"]["edges"]:
        interface = edge["node"]
        node = (interface.get("ip_address") or {}).get("node")
        if not node:
            continue
        address = node["address"]["value"]
        namespace = node["ip_namespace"]["node"]["name"]["value"]
        key = (address, namespace)
        assert node["id"] in key_to_ip_ids.get(key, []), f"primary IP {node['id']} missing from IPAM read"
        key_to_interface_ids.setdefault(key, set()).add(interface["id"])
        primary_interfaces[interface["id"]] = interface
        primary_assigned += 1

    duplicate_ip_keys = {
        f"{address} [{namespace}]": sorted(ip_ids)
        for (address, namespace), ip_ids in key_to_ip_ids.items()
        if len(ip_ids) > 1
    }
    duplicate_interface_ips = {}
    shared_mlag_vteps = {}
    for (address, namespace), interface_ids in key_to_interface_ids.items():
        if len(interface_ids) < 2:
            continue
        label = f"{address} [{namespace}]"
        if _is_shared_mlag_vtep([primary_interfaces.get(identifier) for identifier in sorted(interface_ids)]):
            shared_mlag_vteps[label] = sorted(interface_ids)
        else:
            duplicate_interface_ips[label] = sorted(interface_ids)
    return {
        "total": sum(len(ip_ids) for ip_ids in key_to_ip_ids.values()),
        "assigned": sum(len(ids) for ids in key_to_interface_ids.values()),
        "primary_assigned": primary_assigned,
        "duplicate_ip_keys": duplicate_ip_keys,
        "duplicate_interface_ips": duplicate_interface_ips,
        "shared_mlag_vteps": shared_mlag_vteps,
    }


def _is_shared_mlag_vtep(interfaces: list[dict | None]) -> bool:
    """Validate the modeled two-peer anycast VTEP; all other sharing is a duplicate."""
    if len(interfaces) != 2 or any(interface is None for interface in interfaces):
        return False
    devices = []
    domains = []
    for interface in interfaces:
        assert interface is not None
        if (
            interface.get("__typename") != "InterfaceVirtual"
            or (interface.get("name") or {}).get("value") != "Loopback1"
            or (interface.get("role") or {}).get("value") != "vtep_loopback"
        ):
            return False
        device = (interface.get("device") or {}).get("node") or {}
        domain = (device.get("mlag_domain") or {}).get("node") or {}
        ip_id = ((interface.get("ip_address") or {}).get("node") or {}).get("id")
        vtep_ip_id = ((device.get("vtep_loopback_ip") or {}).get("node") or {}).get("id")
        if not device.get("id") or not domain.get("id") or not ip_id or ip_id != vtep_ip_id:
            return False
        devices.append(device["id"])
        domains.append(domain)
    device_ids = set(devices)
    return (
        len(device_ids) == 2
        and domains[0]["id"] == domains[1]["id"]
        and all({edge["node"]["id"] for edge in domain["peers"]["edges"]} == device_ids for domain in domains)
        and interfaces[0]["ip_address"]["node"]["id"] == interfaces[1]["ip_address"]["node"]["id"]
    )


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
    resp = await client.execute_graphql(query=query, variables={"rack": SERVER_CABLING_RACK}, branch_name=branch)
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
    return {
        "rack_complete": rack_complete,
        "leaf_count": len(leaves),
        "leaf1_server_ports": ports_by_leaf.get("leaf-pod-b2-1-1", 0),
        "leaf2_server_ports": ports_by_leaf.get("leaf-pod-b2-1-2", 0),
    }


async def _server_cabling_report(client: InfrahubClient, branch: str, server_name: str) -> dict:
    """Summarize server-cabling links, server Bond1, and switch-side Port-Channels."""
    query = """
    query ServerCablingReport($server: String!) {
      ComputePhysicalServer(name__value: $server) {
        edges {
          node {
            interfaces {
              edges {
                node {
                  __typename
                  ... on InterfacePhysical {
                    name { value }
                    connector { node { name { value } } }
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
      NetworkLink {
        edges {
          node {
            name { value }
            medium { value }
            role { value }
          }
        }
      }
      InterfaceLag {
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
    resp = await client.execute_graphql(query=query, variables={"server": server_name}, branch_name=branch)

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

    server_links = [
        edge["node"] for edge in resp["NetworkLink"]["edges"] if server_name in edge["node"]["name"]["value"]
    ]
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
    server_physical_lags = {}
    server_connected_count = 0
    for iface in physical_interfaces:
        lag_node = (iface.get("lag") or {}).get("node") or {}
        connector_node = (iface.get("connector") or {}).get("node") or {}
        server_physical_lags[iface["name"]["value"]] = (lag_node.get("name") or {}).get("value")
        if connector_node:
            server_connected_count += 1
    server_bond_members = sorted(
        member_edge["node"]["name"]["value"] for bond in server_bonds for member_edge in bond["lag_members"]["edges"]
    )

    leaf_port_channels = []
    for edge in resp["InterfaceLag"]["edges"]:
        node = edge["node"]
        if node["name"]["value"] != "Port-Channel1":
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


async def _hostvar_snapshot(
    client: InfrahubClient, branch: str, device_ids: list[str], *, contents: bool = True
) -> dict:
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
        content = json.loads(await storage_content(client, storage_id, file_branch=branch)) if contents else None
        files.append(
            {
                "device": node["name"]["value"],
                "file_name": (hostvar_file.get("file_name") or {}).get("value") or "",
                "checksum": (hostvar_file.get("checksum") or {}).get("value") or "",
                "content": content,
            }
        )

    assert "Error" not in hostvar_instances, f"Hostvar generator failed on {branch}: {hostvar_instances}"
    active_statuses = {"pending", "running", "scheduled", "in_progress", "queued"}
    return {
        "ready": (
            len(files) == len(device_ids) > 0
            and not any(status.lower() in active_statuses for status in hostvar_instances)
            and not any(status == "Error" for status in hostvar_instances)
        ),
        "state": sorted(files, key=itemgetter("device")),
    }


async def _rack_rerun_snapshot(client: InfrahubClient, branch: str, *, contents: bool = True) -> dict:
    """Return a normalized snapshot of rack-generated state after a rerun."""
    query = """
    query RackRerunSnapshot($offset: Int, $limit: Int, $details: Boolean!) {
      CoreGeneratorInstance(offset: $offset, limit: $limit) {
        count
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
      LocationRack(offset: $offset, limit: $limit) {
        count
        edges { node { id name { value } generation_complete { value } } }
      }
      NetworkFabric(offset: $offset, limit: $limit) {
        count
        edges { node { id name { value } avd_hostvars_ready { value } } }
      }
      IpamIPAddress(offset: $offset, limit: $limit) @include(if: $details) {
        count
        edges {
          node {
            id
            address { value }
            ip_namespace { node { name { value } } }
            interface { node { id display_label } }
          }
        }
      }
      InterfacePhysical(offset: $offset, limit: $limit) @include(if: $details) {
        count
        edges {
          node {
            id
            name { value }
            device { node { name { value } } }
            ip_address { node { id address { value } } }
            ip_addresses { edges { node { id address { value } } } }
          }
        }
      }
      AvdStructuredConfigFile(offset: $offset, limit: $limit) {
        count
        edges {
          node {
            id
            file_name { value }
            checksum { value }
            file_size { value }
            storage_id { value }
            artifact { node { name { value } } }
          }
        }
      }
    }
    """
    resp = await _snapshot_collections(client, branch, query, contents=contents)
    generator_instances = _normalize_generator_instances(resp["CoreGeneratorInstance"]["edges"])
    rack_generator_instances = [
        instance for instance in generator_instances if instance["definition_name"] == GENERATOR_RACK
    ]
    # Generator instances are branch-local. Inherited files alone cannot prove
    # that every fabric's new cascade has run on this scenario branch.
    fabrics = resp["NetworkFabric"]["edges"]
    structured_targets = {
        edge["node"]["object"]["node"]["id"]
        for edge in resp["CoreGeneratorInstance"]["edges"]
        if edge["node"]["definition"]["node"]["name"]["value"] == GENERATOR_AVD_STRUCTURED_CONFIG
        and edge["node"]["status"]["value"] == "Ready"
    }
    missing_structured_targets = sorted(
        edge["node"]["name"]["value"] for edge in fabrics if edge["node"]["id"] not in structured_targets
    )
    rack_errors = [instance for instance in rack_generator_instances if instance["status"] == "Error"]
    active = [
        instance
        for instance in generator_instances
        if instance["status"].lower() in {"pending", "running", "scheduled", "in_progress", "queued"}
    ]

    racks = sorted(
        (
            edge["node"]["name"]["value"],
            bool(edge["node"]["generation_complete"]["value"]),
        )
        for edge in resp["LocationRack"]["edges"]
    )
    structured_config_files = _normalize_structured_config_files(resp["AvdStructuredConfigFile"]["edges"])
    structured_config_ethernet_ip_refs = (
        await _structured_config_ethernet_ip_refs(
            client,
            branch,
            resp["AvdStructuredConfigFile"]["edges"],
        )
        if contents
        else []
    )
    errors = [instance for instance in generator_instances if instance["status"] == "Error"]
    assert not errors, f"Generator failures on {branch}: {errors}"
    state = {
        "generator_instances": generator_instances,
        "rack_generator_instances": rack_generator_instances,
        "ipam_addresses": _normalize_ipam_addresses(resp.get("IpamIPAddress", {"edges": []})["edges"]),
        "physical_interface_ip_assignments": _normalize_physical_interface_ip_assignments(
            resp.get("InterfacePhysical", {"edges": []})["edges"]
        ),
        # Include ports without an IP as well: tracking cleanup must not delete
        # or recreate a topology-owned interface on the unchanged second pass.
        "physical_interfaces": sorted(
            (
                edge["node"]["device"]["node"]["name"]["value"],
                edge["node"]["name"]["value"],
                edge["node"]["id"],
            )
            for edge in resp.get("InterfacePhysical", {"edges": []})["edges"]
        ),
        "structured_config_files": structured_config_files,
        "structured_config_ethernet_ip_refs": structured_config_ethernet_ip_refs,
    }
    if contents:
        physical_ports = {(device, interface) for device, interface, _ in state["physical_interfaces"]}
        expected = {
            (ref["artifact"], ref["interface"], ref["ip_address"])
            for ref in structured_config_ethernet_ip_refs
            if (ref["artifact"], ref["interface"]) in physical_ports
        }
        assert expected, "no modeled physical interface IPs found in structured configs"
        actual = {
            (assignment["device"], assignment["interface"], assignment["ip_address"])
            for assignment in state["physical_interface_ip_assignments"]
            if assignment["ip_address"]
        }
        assert not expected - actual, f"physical primary IP backfill missing: {sorted(expected - actual)}"
    return {
        "ready": (
            len(rack_generator_instances) == len(racks) > 0
            and not rack_errors
            and not active
            and all(complete for _, complete in racks)
            and bool(fabrics)
            and all(edge["node"]["avd_hostvars_ready"]["value"] for edge in fabrics)
            and not missing_structured_targets
            and bool(structured_config_files)
        ),
        "rack_errors": rack_errors,
        "active_generator_instances": active,
        "missing_structured_targets": missing_structured_targets,
        "state": state,
    }


async def _snapshot_collections(client: InfrahubClient, branch: str, query: str, *, contents: bool) -> dict:
    """Read every metadata/object page, including all unaddressed physical ports."""
    collections: dict[str, dict] = {}
    offset = 0
    while True:
        response = await read_with_retry(
            lambda offset=offset: client.execute_graphql(
                query=query,
                variables={"offset": offset, "limit": 500, "details": contents},
                branch_name=branch,
            )
        )
        pending = []
        for kind, page in response.items():
            collection = collections.setdefault(kind, {"count": page["count"], "edges": []})
            assert collection["count"] == page["count"], f"{kind} changed while snapshot was read"
            if len(collection["edges"]) >= collection["count"]:
                continue
            assert page["edges"], f"{kind} snapshot ended before its reported count"
            collection["edges"].extend(page["edges"])
            assert len({edge["node"]["id"] for edge in collection["edges"]}) == len(collection["edges"]), (
                f"{kind} snapshot returned duplicate node IDs"
            )
            if len(collection["edges"]) < collection["count"]:
                pending.append(len(page["edges"]))
        if not pending:
            return collections
        offset += min(pending)


def _snapshot_difference(before: dict, after: dict) -> str:
    differences = []
    for key in before.keys() | after.keys():
        if before.get(key) == after.get(key):
            continue
        previous = {json.dumps(item, sort_keys=True) for item in before.get(key, [])}
        current = {json.dumps(item, sort_keys=True) for item in after.get(key, [])}
        differences.append(f"{key}: removed={sorted(previous - current)[:6]}, added={sorted(current - previous)[:6]}")
    return "rack rerun changed generated-state snapshot: " + "; ".join(sorted(differences))


def _normalize_generator_instances(edges: list[dict]) -> list[dict]:
    instances = []
    for edge in edges:
        node = edge["node"]
        definition = (node.get("definition") or {}).get("node") or {}
        definition_name = ((definition.get("name") or {}).get("value")) or definition.get("display_label")
        target = (node.get("object") or {}).get("node") or {}
        instances.append(
            {
                "name": (node.get("name") or {}).get("value") or "",
                "status": (node.get("status") or {}).get("value") or "",
                "definition_name": definition_name or "",
                "target": target.get("display_label") or target.get("id") or "",
            }
        )
    return sorted(instances, key=itemgetter("definition_name", "target", "name"))


def _normalize_ipam_addresses(edges: list[dict]) -> list[dict]:
    addresses = []
    for edge in edges:
        node = edge["node"]
        interface = (node.get("interface") or {}).get("node") or {}
        namespace = ((node.get("ip_namespace") or {}).get("node") or {}).get("name", {}).get("value") or "default"
        addresses.append(
            {
                "address": (node.get("address") or {}).get("value"),
                "namespace": namespace,
                "interface": interface.get("display_label") or interface.get("id") or "",
            }
        )
    return sorted(addresses, key=itemgetter("namespace", "address", "interface"))


def _normalize_physical_interface_ip_assignments(edges: list[dict]) -> list[dict]:
    assignments = []
    for edge in edges:
        node = edge["node"]
        device_name = ((node.get("device") or {}).get("node") or {}).get("name", {}).get("value") or ""
        interface_name = (node.get("name") or {}).get("value") or ""
        primary = ((node.get("ip_address") or {}).get("node") or {}).get("address", {}).get("value")
        ip_addresses = sorted(
            (ip_edge["node"].get("address") or {}).get("value")
            for ip_edge in (node.get("ip_addresses") or {}).get("edges", [])
            if ip_edge.get("node")
        )
        if primary or ip_addresses:
            assignments.append(
                {
                    "device": device_name,
                    "interface": interface_name,
                    "ip_address": primary,
                    "ip_addresses": ip_addresses,
                }
            )
    return sorted(assignments, key=itemgetter("device", "interface"))


def _normalize_structured_config_files(edges: list[dict]) -> list[dict]:
    files = []
    for edge in edges:
        node = edge["node"]
        artifact = (node.get("artifact") or {}).get("node") or {}
        files.append(
            {
                "artifact": (artifact.get("name") or {}).get("value") or "",
                "file_name": (node.get("file_name") or {}).get("value") or "",
                "checksum": (node.get("checksum") or {}).get("value") or "",
                "file_size": (node.get("file_size") or {}).get("value") or 0,
            }
        )
    return sorted(files, key=itemgetter("artifact", "file_name"))


async def _structured_config_ethernet_ip_refs(client: InfrahubClient, branch: str, edges: list[dict]) -> list[dict]:
    refs = []
    for edge in edges:
        node = edge["node"]
        storage_id = (node.get("storage_id") or {}).get("value")
        if not storage_id:
            continue
        content = await storage_content(client, storage_id, file_branch=branch)
        structured_config = json.loads(content)
        artifact = (node.get("artifact") or {}).get("node") or {}
        for interface in structured_config.get("ethernet_interfaces", []):
            if not interface.get("ip_address"):
                continue
            refs.append(
                {
                    "artifact": (artifact.get("name") or {}).get("value") or "",
                    "interface": interface.get("name") or "",
                    "ip_address": interface.get("ip_address") or "",
                }
            )
    return sorted(refs, key=itemgetter("artifact", "interface", "ip_address"))


async def _create_dci_link_scenario(client: InfrahubClient, branch: str) -> dict:
    """Extend the seeded two-DC design without replacing its pool or border roles."""
    candidates = await _dci_border_candidates(client, branch)
    assert [device["device_name"] for device in candidates] == [
        f"leaf-infrahub-dc{dc}-1-{index}" for dc in (1, 2) for index in (1, 2)
    ], f"expected both seeded border pairs, got {candidates}"
    assert [device["pod_name"] for device in candidates] == ["infrahub-dc1"] * 2 + ["infrahub-dc2"] * 2
    left, right = candidates[0], candidates[2]

    fabric = await client.get(kind="NetworkFabric", id=left["fabric_id"], branch=branch, include=["dci_pool"])
    pool_id = fabric.dci_pool.id
    assert pool_id, f"{DCI_FABRIC} must retain its seeded DCI pool"
    pool = await client.get(kind="CoreIPPrefixPool", id=pool_id, branch=branch, include=["resources"])
    assert pool.name.value == DCI_POOL_NAME
    assert pool.default_prefix_length.value == 31
    assert {str(resource.peer.prefix.value) for resource in pool.resources.peers} == {DCI_POOL_PREFIX}

    dci_link = await client.create(
        kind="NetworkLink",
        branch=branch,
        name=DCI_LINK_NAME,
        medium="smf",
        role="dci",
        include_in_underlay_protocol=True,
    )
    await dci_link.save(allow_upsert=True)

    for endpoint in (left, right):
        dci_interface = await client.create(
            kind="InterfacePhysical",
            branch=branch,
            name=DCI_INTERFACE_NAME,
            device={"id": endpoint["device_id"]},
            role="peering",
        )
        await dci_interface.save(allow_upsert=True)
        iface = await client.get(kind="InterfacePhysical", id=dci_interface.id, branch=branch, include=["connector"])
        iface.connector = dci_link
        iface.status.value = "active"
        await iface.save(allow_upsert=True)
        endpoint["interface_id"] = dci_interface.id
        endpoint["interface_name"] = DCI_INTERFACE_NAME

    # Retain every original inter-DC link as well as the newly created one.
    expected_links = {}
    for left_index, right_index, interface_name in (
        (0, 2, "Ethernet5"),
        (0, 3, "Ethernet6"),
        (1, 3, "Ethernet5"),
        (1, 2, "Ethernet6"),
        (0, 2, DCI_INTERFACE_NAME),
    ):
        endpoints = [candidates[left_index], candidates[right_index]]
        key = (tuple(device["device_name"] for device in endpoints), (interface_name, interface_name))
        expected_links[key] = [device["device_asn"] for device in endpoints]
    return {
        "device_ids": [device["device_id"] for device in candidates],
        "device_names": [device["device_name"] for device in candidates],
        "fabric_id": fabric.id,
        "pool_id": pool_id,
        "expected_links": expected_links,
    }


async def _dci_border_candidates(client: InfrahubClient, branch: str) -> list[dict]:
    query = """
    query DciBorderCandidates {
      DcimDevice(role__value: "border_leaf") {
        edges {
          node {
            id
            name { value }
            asn { node { asn { value } } }
            pod { node { name { value } parent { node { id name { value } } } } }
          }
        }
      }
    }
    """
    resp = await client.execute_graphql(query=query, branch_name=branch)
    candidates = []
    for edge in resp["DcimDevice"]["edges"]:
        device = edge["node"]
        fabric = device.get("pod", {}).get("node", {}).get("parent", {}).get("node")
        if not fabric:
            continue
        asn_node = (device.get("asn") or {}).get("node")
        device_asn = asn_node["asn"]["value"] if asn_node and asn_node.get("asn") else None
        if device_asn is None:
            continue
        if fabric["name"]["value"] != DCI_FABRIC:
            continue
        candidates.append(
            {
                "device_id": device["id"],
                "device_name": device["name"]["value"],
                "device_asn": int(device_asn),
                "fabric_id": fabric["id"],
                "fabric_name": fabric["name"]["value"],
                "pod_name": device["pod"]["node"]["name"]["value"],
                "interface_id": None,
                "interface_name": DCI_INTERFACE_NAME,
            }
        )
    return sorted(candidates, key=itemgetter("pod_name", "device_name", "device_id"))


async def _run_generator_for_nodes(
    client: InfrahubClient,
    branch: str,
    generator_name: str,
    node_ids: list[str],
    *,
    cascade_runs: dict[str, int] | None = None,
) -> None:
    before = {task.id for task in await _pipeline_tasks(client, branch)}
    gen_def = await client.get(kind="CoreGeneratorDefinition", name__value=generator_name, branch=branch)
    if node_ids:
        await client.execute_graphql(
            query="""
            mutation RunGenerator($id: String!, $nodes: [String!]!) {
                CoreGeneratorDefinitionRun(data: { id: $id, nodes: $nodes }) {
                    ok
                }
            }
            """,
            variables={"id": gen_def.id, "nodes": node_ids},
            branch_name=branch,
        )
    await _wait_for_tasks(client, branch, before, expected_runs={generator_name: len(node_ids), **(cascade_runs or {})})


async def _wait_for_tasks(
    client: InfrahubClient, branch: str, before: set[str], *, expected_runs: dict[str, int] | None = None
) -> None:
    """Observe the new execution and its cascades, never accept stale Ready nodes."""
    quiet = 0
    previous_ids: set[str] = set()

    async def fetch() -> tuple[list, bool]:
        nonlocal quiet, previous_ids
        tasks = await _execution_tasks(client, branch, before)
        active = [task for task in tasks if task.state != "COMPLETED"]
        expected_seen = all(
            sum(task.workflow == "generator-run" and task.title == f"Run generator {name}" for task in tasks) >= count
            for name, count in (expected_runs or {}).items()
        )
        task_ids = {task.id for task in tasks}
        if task_ids != previous_ids:
            quiet = 0
        quiet = quiet + 1 if tasks and not active and expected_seen else 0
        previous_ids = task_ids
        return tasks, quiet >= 2

    tasks, _ = await wait_until(
        fetch=fetch,
        ready=itemgetter(1),
        timeout=GENERATOR_TIMEOUT,
        interval=2,
        describe=f"new execution and cascades settled on {branch}",
    )
    print(f"Completed tasks on {branch}: {[task.id for task in tasks]}", flush=True)


async def _pipeline_tasks(client: InfrahubClient, branch: str) -> list:
    """Include generator/artifact runs whose Prefect metadata omits the branch.

    This client belongs to one isolated stack. Checkpoint both collections before
    each mutation so earlier generator runs cannot be mistaken for its cascade.
    """
    branch_tasks, background_tasks = await asyncio.gather(
        _task_pages(client, TaskFilter(branch=branch)),
        _task_pages(
            client,
            TaskFilter(workflow=["generator-run", "artifact-generate", "request_artifact_definitions_generate"]),
        ),
    )
    tasks = {task.id: task for task in branch_tasks}
    tasks.update({task.id: task for task in background_tasks if task.branch is None})
    return list(tasks.values())


async def _task_pages(client: InfrahubClient, filters: TaskFilter) -> list:
    """Read metadata pages without the SDK's repeated task-count queries."""
    tasks = []
    offset = 0
    while True:
        page = await read_with_retry(
            lambda offset=offset: client.task.filter(filters, offset=offset, limit=200, timeout=15)
        )
        tasks.extend(page)
        if len(page) < 200:
            return tasks
        offset += 200


async def _execution_tasks(client: InfrahubClient, branch: str, before: set[str]) -> list:
    tasks = [task for task in await _pipeline_tasks(client, branch) if task.id not in before]
    failed = [task for task in tasks if task.state in {"FAILED", "CRASHED", "CANCELLED"}]
    if failed:
        details = [await client.task.get(task.id, include_logs=True) for task in failed]
        raise AssertionError(f"Tasks failed on {branch}: {details}")
    return tasks


async def _dci_hostvars_report(client: InfrahubClient, branch: str, device_names: list[str]) -> dict:
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
    resp = await client.execute_graphql(query=query, variables={"names": device_names}, branch_name=branch)
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
        hostvars[node["name"]["value"]] = json.loads(await storage_content(client, storage_id, file_branch=branch))

    matching_links = []
    for device_hostvars in hostvars.values():
        matching_links.extend(
            link
            for link in device_hostvars.get("l3_edge", {}).get("p2p_links", [])
            if len(link.get("nodes", [])) == 2 and set(link["nodes"]) <= set(device_names)
        )

    unique_matching_links = list(
        {
            (
                tuple(link.get("nodes", [])),
                tuple(link.get("interfaces", [])),
                tuple(link.get("as", [])),
                tuple(link.get("ip", [])),
                link.get("include_in_underlay_protocol"),
                link.get("speed"),
            ): link
            for link in matching_links
        }.values()
    )

    return {
        "ready": set(hostvars) == set(device_names),
        "hostvars": hostvars,
        "matching_links": matching_links,
        "unique_matching_links": unique_matching_links,
    }


async def _trigger_all_artifacts(client: InfrahubClient, branch: str, definitions: list) -> None:
    """Generate each definition once, settling its tasks before the next one.

    The branch-aware endpoint exercises all targets. Serial definitions avoid
    simultaneous fabric-documentation downloads and per-device renders flooding
    the CI-sized API server. The node helper omits the branch.
    """
    for definition in definitions:
        before = {task.id for task in await _pipeline_tasks(client, branch)}
        async with asyncio.timeout(ARTIFACT_TIMEOUT):
            for attempt in range(4):
                resp = await client._post(
                    f"{client.address}/api/artifact/generate/{definition.id}?branch={branch}",
                    payload={"nodes": []},
                )
                # This marker guarantees admission rejected the request before
                # its handler ran. An unmarked 429 or lost response is not replayed.
                if resp.status_code != 429 or resp.headers.get("X-Infrahub-Admission") != "shed" or attempt == 3:
                    resp.raise_for_status()
                    break
                await asyncio.sleep(retry_after_delay(resp, 4 * 2**attempt))
        await _wait_for_tasks(client, branch, before)


def _ready_artifact_names(artifacts: list) -> set[str]:
    """A definition is complete only if every existing output is stored and Ready."""
    ready = {artifact.name.value for artifact in artifacts}
    ready.difference_update(
        artifact.name.value
        for artifact in artifacts
        if artifact.status.value != "Ready" or not artifact.storage_id.value
    )
    return ready


async def _artifact_ids_for_targets(client: InfrahubClient, branch: str, name: str, targets: set[str]) -> list[str]:
    """The endpoint's `nodes` filter selects CoreArtifact IDs, not device IDs."""
    response = await read_with_retry(
        lambda: client.execute_graphql(
            query="""query($name: String!) {
              CoreArtifact(name__value: $name) {
                edges { node { id object { node { id } } } }
              }
            }""",
            variables={"name": name},
            branch_name=branch,
        )
    )
    selected = {
        edge["node"]["object"]["node"]["id"]: edge["node"]["id"]
        for edge in response["CoreArtifact"]["edges"]
        if edge["node"]["object"]["node"]["id"] in targets
    }
    assert selected.keys() == targets, f"Missing {name} artifacts for targets {targets - selected.keys()}"
    return sorted(selected.values())


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
            contents.append(await storage_content(client, storage_id))
    return contents


async def _fetch_ready_artifact_storage_by_target(
    client: InfrahubClient,
    branch: str,
    artifact_name: str,
) -> dict[str, str]:
    """Poll metadata without downloading content until the execution settles."""
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
    response = await client.execute_graphql(query=query, branch_name=branch)
    storage: dict[str, str] = {}
    for edge in response["CoreArtifact"]["edges"]:
        node = edge["node"]
        assert node.get("status", {}).get("value") != "Error", f"Artifact failed on {branch}: {node}"
        if node.get("status", {}).get("value") != "Ready":
            continue
        storage_id = node.get("storage_id", {}).get("value")
        target = (node.get("object") or {}).get("node") or {}
        target_name = target.get("display_label")
        if storage_id and target_name:
            storage[target_name] = storage_id
    return storage


async def _wait_for_new_artifact_storage(
    client: InfrahubClient, branch: str, artifact_name: str, targets: set[str], previous: dict[str, str]
) -> dict[str, str]:
    return await wait_until(
        fetch=lambda: _fetch_ready_artifact_storage_by_target(client, branch, artifact_name),
        ready=lambda storage: all(name in storage and storage[name] != previous.get(name) for name in targets),
        timeout=ARTIFACT_TIMEOUT,
        interval=POLL_INTERVAL,
        describe=f"new Ready {artifact_name} storage for {sorted(targets)} on {branch}",
    )


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
    response = await client.execute_graphql(query=query, branch_name=branch)
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
        content = await storage_content(client, storage_id)
        return content, target.get("display_label")
    return None, None
