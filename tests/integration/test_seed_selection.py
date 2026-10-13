"""Opt-in seed matrix; each selection gets a fresh isolated container stack.

Run this file explicitly when integration validation is requested. No repository
or triggers are registered, and no existing shared instance is used.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, Any

import pytest
from infrahub_sdk.testing.docker import TestInfrahubDockerClient

from solution_arista_avd.seed_data import select_seed_data

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
class TestSeedSelection(TestInfrahubDockerClient):
    @pytest.fixture(scope="class", params=SELECTIONS)
    def seed_selection(self, request: pytest.FixtureRequest) -> str:
        return str(request.param)

    @pytest.fixture(scope="class")
    def tmp_directory(self, tmp_path_factory: pytest.TempPathFactory, seed_selection: str) -> Path:
        # All inherited stack fixtures depend on tmp_directory. Making it depend
        # on the parameter tears down the previous stack before the next case.
        return tmp_path_factory.mktemp(f"seed-{seed_selection}")

    @pytest.mark.asyncio(loop_scope="class")
    async def test_load_selected_seeds(
        self,
        seed_selection: str,
        root_directory: Path,
        default_branch: str,
        client: InfrahubClient,
        schemas: list[dict[str, Any]],
        infrahub_port: int,
    ) -> None:
        await client.schema.wait_until_converged(branch=default_branch)
        response = await client.schema.load(schemas=schemas, branch=default_branch, wait_until_converged=True)
        assert not response.errors, response.errors
        assert not await client.all(kind="NetworkFabric", branch=default_branch), "matrix case must start empty"
        selection = select_seed_data(root_directory / "objects", seed_selection)
        result = self.execute_command(
            command=f"invoke load-objects --fabrics={shlex.quote(seed_selection)} --branch={shlex.quote(default_branch)}",
            address=f"http://localhost:{infrahub_port}",
        )
        assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
        loaded = await client.all(kind="NetworkFabric", branch=default_branch)
        assert {fabric.name.value for fabric in loaded} == {FABRICS[slug] for slug in selection.fabrics}
        manufacturers = await client.all(kind="OrganizationManufacturer", branch=default_branch)
        assert {manufacturer.name.value for manufacturer in manufacturers} == {"Arista"}
        assert await client.all(kind="TemplateDcimDevice", branch=default_branch), "Common templates not loaded"
        assert await client.all(kind="CoreStandardGroup", branch=default_branch), "Common groups not loaded"
