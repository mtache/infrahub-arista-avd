"""Opt-in seed matrix; one stack, schema-only origin and one empty branch per selection.

Run this file explicitly when integration validation is requested. No repository
or triggers are registered, and no existing shared instance is used.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import pytest
import pytest_asyncio
import yaml

from solution_arista_avd.seed_data import select_seed_data

from .helpers import create_branch
from .stack import IntegrationStack

if TYPE_CHECKING:
    from pathlib import Path

    from infrahub_sdk import InfrahubClient

FABRICS = {
    "campus": "Fabric-Campus",
    "isis-ldp": "Fabric-ISIS-LDP",
    "l2ls": "Fabric-L2LS",
    "l3ls-multi-domain": "Fabric-L3LS-Multi-Domain",
    "l3ls-multipod-a": "Fabric-L3LS-MultiPod-A",
    "l3ls-multipod-b": "Fabric-L3LS-MultiPod-B",
    "single-dc-l3ls": "Fabric-L3LS",
}
SELECTIONS = ("none", *FABRICS, "l3ls-multipod-a,l3ls-multipod-b", "l2ls,campus", "all")


@pytest.mark.e2e
class TestSeedSelection(IntegrationStack):
    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def seed_origin(self, client: InfrahubClient, default_branch: str, schemas: list[dict], root_directory: Path):
        await client.schema.wait_until_converged(branch=default_branch)
        response = await client.schema.load(schemas=schemas, branch=default_branch, wait_until_converged=True)
        assert not response.errors, response.errors
        # Introspect every kind loaded by the real seed command, including builtins.
        kinds = set()
        for path in (root_directory / "objects").rglob("*.yml"):
            kinds.update(document["spec"]["kind"] for document in yaml.safe_load_all(path.read_text()) if document)
        # SDK 1.22 drops `branch` from ProfileSchemaAPI/TemplateSchemaAPI.
        # Inspect the complete API document so these kinds remain covered too.
        response = await client._get(url=f"{client.address}/api/schema?{urlencode({'branch': default_branch})}")
        response.raise_for_status()
        global_kinds = _branch_agnostic_seed_kinds(response.json(), kinds)
        print(f"seed kinds without branch isolation: {global_kinds}", flush=True)
        return default_branch, global_kinds

    @pytest_asyncio.fixture(loop_scope="class")
    async def seed_branch(self, seed_origin: tuple[str, list[str]], seed_selection: str, client: InfrahubClient):
        origin, global_kinds = seed_origin
        initial_global_ids = {
            kind: {node.id for node in await client.all(kind=kind, branch=origin, prefetch_relationships=False)}
            for kind in global_kinds
        }
        branch = await create_branch(client, f"seed-{SELECTIONS.index(seed_selection)}", origin)
        try:
            yield branch
        finally:
            await client.branch.delete(branch_name=branch)
            for kind in reversed(global_kinds):
                for node in await client.all(kind=kind, branch=origin, prefetch_relationships=False):
                    if node.id not in initial_global_ids[kind]:
                        await node.delete()
                assert {
                    node.id for node in await client.all(kind=kind, branch=origin, prefetch_relationships=False)
                } == initial_global_ids[kind]

    @pytest.mark.parametrize("seed_selection", SELECTIONS)
    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_selected_seeds(
        self,
        seed_selection: str,
        root_directory: Path,
        seed_branch: str,
        client: InfrahubClient,
        seed_origin: tuple[str, list[str]],
        infrahub_port: int,
    ) -> None:
        origin, global_kinds = seed_origin
        branch = seed_branch
        for kind in ("NetworkFabric", "NetworkPod", "LocationRack", "ComputePhysicalServer"):
            assert not await client.all(kind=kind, branch=branch), f"matrix case must start empty: {kind}"
        selection = select_seed_data(root_directory / "objects", seed_selection)
        result = self.execute_command(
            command=f"invoke load-objects --fabrics={shlex.quote(seed_selection)} --branch={shlex.quote(branch)}",
            address=f"http://localhost:{infrahub_port}",
        )
        assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
        loaded = await client.all(kind="NetworkFabric", branch=branch)
        assert {fabric.name.value for fabric in loaded} == {FABRICS[slug] for slug in selection.fabrics}
        manufacturers = await client.all(kind="OrganizationManufacturer", branch=branch)
        assert {manufacturer.name.value for manufacturer in manufacturers} == {"Arista"}
        assert await client.all(kind="TemplateDcimDevice", branch=branch), "Common templates not loaded"
        assert await client.all(kind="CoreStandardGroup", branch=branch), "Common groups not loaded"

        # Agnostic topology nodes are visible on the source during this case;
        # the fixture removes them before the next selection. Aware nodes must
        # remain absent from the schema-only source throughout the case.
        for kind in ("NetworkFabric", "NetworkPod", "LocationRack", "ComputePhysicalServer"):
            source_ids = {node.id for node in await client.all(kind=kind, branch=origin)}
            if kind in global_kinds:
                assert source_ids == {node.id for node in await client.all(kind=kind, branch=branch)}
            else:
                assert not source_ids, f"Branch-aware seed objects leaked to source: {kind}"
        pods = await client.all(kind="NetworkPod", branch=branch)
        assert all(pod.parent.id in {fabric.id for fabric in loaded} for pod in pods)
        racks = await client.all(kind="LocationRack", branch=branch)
        assert all(rack.pod.id in {pod.id for pod in pods} for rack in racks)
        for kind, attribute in (
            ("ProfileDcimInterface", "profile_name"),
            ("TemplateComputePhysicalServer", "template_name"),
        ):
            expected = set()
            for directory in selection.directories:
                for path in directory.glob("*.yml"):
                    for document in yaml.safe_load_all(path.read_text()):
                        if document and document["spec"]["kind"] == kind:
                            expected.update(item[attribute] for item in document["spec"]["data"])
            actual = await client.all(kind=kind, branch=branch)
            assert {getattr(node, attribute).value for node in actual} == expected, f"Seed selection leaked {kind}"
        templates = await client.all(kind="TemplateComputePhysicalServer", branch=branch, include=["interfaces"])
        for template in templates:
            for interface in template.interfaces.peers:
                await interface.peer.profiles.fetch()
                assert interface.peer.profiles.peers, f"Server port has no profile: {interface.peer.display_label}"


def _branch_agnostic_seed_kinds(document: dict, kinds: set[str]) -> list[str]:
    schemas = {
        f"{schema['namespace']}{schema['name']}": schema
        for category in ("nodes", "profiles", "templates")
        for schema in document[category]
    }
    assert kinds <= schemas.keys(), f"Seed kinds missing from schema introspection: {kinds - schemas.keys()}"
    assert all(schemas[kind]["branch"] in {"aware", "agnostic", "local"} for kind in kinds)
    return sorted(kind for kind in kinds if schemas[kind]["branch"] == "agnostic")
