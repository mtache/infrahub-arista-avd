"""Schema contracts for BGP ASNs and device designs on one isolated stack.

Boots a real Infrahub via ``infrahub-testcontainers``, loads this repo's schemas
once, and asserts both contracts without seeds, repositories or triggers:

- ``RoutingAsn.asn`` is unique, with matching device and MLAG relationships;
- the ``NetworkDeviceDesign`` generic carries ``role``
  (super_spine/spine/leaf/l2leaf), ``device_quantity``,
  and ``device_template`` -> ``CoreObjectTemplate`` (one, ``no-action`` so a
  shared template survives a design deletion) (SC-002);
- each tier's concrete design node inherits the generic, is keyed
  ``(container, role)`` (SC-004) with a readable ``human_friendly_id`` (SC-003),
  and pairs its ``Parent`` relationship to the container's cascade-owned
  ``Component`` many relationship (SC-005/6);
- the legacy paired sizing fields are gone, so ``device_designs`` is the only
  source of device sizing with no stale fallback (SC-006).

All nine cases share one class-scoped stack, client, event loop and schema load.
Each case can also be selected independently. The uniqueness case deletes its
own ASN before leaving the shared schema-only state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from infrahub_sdk.exceptions import GraphQLError

from .stack import IntegrationStack

if TYPE_CHECKING:
    from infrahub_sdk import InfrahubClient

ROLE_CHOICES = {"super_spine", "spine", "leaf", "l2leaf"}

# design kind, container kind, Parent relationship name, shared identifier
TIERS = [
    ("NetworkRackDeviceDesign", "LocationRack", "rack", "rack__device_designs"),
    ("NetworkPodDeviceDesign", "NetworkPod", "pod", "pod__device_designs"),
    ("NetworkFabricDeviceDesign", "NetworkFabric", "fabric", "fabric__device_designs"),
]

# container kind -> the legacy fields device_designs replaced, which MUST be gone
LEGACY_FIELDS = {
    "NetworkFabric": ["super_spine_switch_template", "amount_of_super_spines"],
    "NetworkPod": ["spine_switch_template", "amount_of_spines"],
    "LocationRack": ["leaf_switch_template", "amount_of_leafs", "l2leaf_switch_template", "amount_of_l2leafs"],
}


@pytest_asyncio.fixture(scope="class", loop_scope="class")
async def schema_contracts_ready(client: InfrahubClient, default_branch: str, schemas: list[dict]) -> None:
    """Load and converge the schema once, including for a single selected case."""
    await client.schema.wait_until_converged(branch=default_branch)
    resp = await client.schema.load(schemas=schemas, branch=default_branch, wait_until_converged=True)
    assert resp.errors == {}, f"schema load errors: {resp.errors}"  # SC-001
    await client.schema.wait_until_converged(branch=default_branch)


@pytest.mark.asyncio(loop_scope="class")
@pytest.mark.usefixtures("schema_contracts_ready")
class TestSchemaContracts(IntegrationStack):
    async def test_schema_loads_and_wires_routing_asn(self, default_branch: str, client: InfrahubClient) -> None:
        """Schema loads clean (SC-001); RoutingAsn exists and is wired to devices/MLAG (SC-002, SC-005)."""
        # SC-002: the node exists and its AS number is globally unique.
        asn_schema = await client.schema.get(kind="RoutingAsn", branch=default_branch)
        asn_attr = next((a for a in asn_schema.attributes if a.name == "asn"), None)
        assert asn_attr is not None, "RoutingAsn is missing the 'asn' attribute"
        assert asn_attr.unique is True, "RoutingAsn.asn must be globally unique"

        # SC-005: inverses on the ASN node, with the identifiers that pair them to the peers.
        rels = {r.name: r for r in asn_schema.relationships}
        assert rels["fabric"].cardinality == "one"
        assert rels["devices"].cardinality == "many"
        assert rels["devices"].identifier == "device__asn"
        assert rels["mlag_domains"].cardinality == "many"
        assert rels["mlag_domains"].identifier == "mlag_domain__asn"

        # forward "one" side on each peer, sharing the identifier (bidirectional edge).
        device_schema = await client.schema.get(kind="DcimDevice", branch=default_branch)
        dev_asn = next((r for r in device_schema.relationships if r.name == "asn"), None)
        assert dev_asn is not None, "DcimDevice is missing the 'asn' relationship"
        assert dev_asn.peer == "RoutingAsn"
        assert dev_asn.cardinality == "one"
        assert dev_asn.identifier == "device__asn"

        mlag_schema = await client.schema.get(kind="MlagDomain", branch=default_branch)
        mlag_asn = next((r for r in mlag_schema.relationships if r.name == "asn"), None)
        assert mlag_asn is not None, "MlagDomain is missing the 'asn' relationship"
        assert mlag_asn.peer == "RoutingAsn"
        assert mlag_asn.cardinality == "one"
        assert mlag_asn.identifier == "mlag_domain__asn"

    async def test_asn_is_globally_unique(self, default_branch: str, client: InfrahubClient) -> None:
        """A second RoutingAsn with the same AS number is rejected — no cross-fabric reuse (SC-004)."""
        first = await client.create(kind="RoutingAsn", asn=64599, branch=default_branch)
        await first.save()
        try:
            duplicate = await client.create(kind="RoutingAsn", asn=64599, branch=default_branch)
            with pytest.raises(GraphQLError):
                await duplicate.save()
        finally:
            await first.delete()

    async def test_generic_shape(self, default_branch: str, client: InfrahubClient) -> None:
        """The NetworkDeviceDesign generic carries role, device_quantity, and device_template (SC-002)."""
        generic = await client.schema.get(kind="NetworkDeviceDesign", branch=default_branch)

        attrs = {a.name: a for a in generic.attributes}
        assert attrs["role"].kind == "Dropdown"
        assert {c["name"] for c in attrs["role"].choices} == ROLE_CHOICES
        assert attrs["device_quantity"].kind == "Number"

        rels = {r.name: r for r in generic.relationships}
        assert rels["device_template"].peer == "CoreObjectTemplate"
        assert rels["device_template"].cardinality == "one"
        assert rels["device_template"].optional is False
        # Shared template must survive design deletion.
        assert rels["device_template"].on_delete == "no-action"

    @pytest.mark.parametrize(("design_kind", "container_kind", "parent_rel", "identifier"), TIERS)
    async def test_tier_design_is_keyed_and_wired(
        self,
        default_branch: str,
        client: InfrahubClient,
        design_kind: str,
        container_kind: str,
        parent_rel: str,
        identifier: str,
    ) -> None:
        """Every tier exposes designs through the same keyed, cascade-owned entity (SC-002/3/4/5/6)."""
        design = await client.schema.get(kind=design_kind, branch=default_branch)
        assert "NetworkDeviceDesign" in design.inherit_from  # SC-002 (uniform shape)
        assert [parent_rel, "role__value"] in design.uniqueness_constraints  # SC-004
        assert design.human_friendly_id == [f"{parent_rel}__name__value", "role__value"]  # SC-003

        design_rels = {r.name: r for r in design.relationships}
        assert design_rels[parent_rel].peer == container_kind
        assert design_rels[parent_rel].kind == "Parent"
        assert design_rels[parent_rel].cardinality == "one"
        assert design_rels[parent_rel].identifier == identifier

        # SC-005: container Component side — many, cascade, matching identifier.
        container = await client.schema.get(kind=container_kind, branch=default_branch)
        dd = next((r for r in container.relationships if r.name == "device_designs"), None)
        assert dd is not None, f"{container_kind} is missing the 'device_designs' relationship"
        assert dd.peer == design_kind
        assert dd.cardinality == "many"
        assert dd.kind == "Component"
        assert dd.on_delete == "cascade"
        assert dd.identifier == identifier

    @pytest.mark.parametrize("container_kind", list(LEGACY_FIELDS))
    async def test_legacy_sizing_fields_are_gone(
        self, default_branch: str, client: InfrahubClient, container_kind: str
    ) -> None:
        """Device sizing has exactly one source; the legacy paired fields no longer exist (SC-006)."""
        node = await client.schema.get(kind=container_kind, branch=default_branch)
        field_names = {a.name for a in node.attributes} | {r.name for r in node.relationships}

        for legacy in LEGACY_FIELDS[container_kind]:
            assert legacy not in field_names, (
                f"{container_kind}.{legacy} still exists; device sizing must come only "
                "from device_designs so there is no stale fallback."
            )
