"""Offline contracts for independently loadable examples and topology capacity."""

import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest
import yaml

from solution_arista_avd.seed_data import select_seed_data

ROOT = Path(__file__).resolve().parents[2]
OBJECTS = ROOT / "objects"
FABRICS = {
    "campus": "Fabric-Campus",
    "isis-ldp": "Fabric-ISIS-LDP",
    "l2ls": "Fabric-L2LS",
    "l3ls-multi-domain": "Fabric-L3LS-Multi-Domain",
    "l3ls-multipod-a": "Fabric-L3LS-MultiPod-A",
    "l3ls-multipod-b": "Fabric-L3LS-MultiPod-B",
    "single-dc-l3ls": "Fabric-L3LS",
}


def _schemas() -> dict[str, dict[str, Any]]:
    schemas: dict[str, dict[str, Any]] = {}
    for path in sorted((ROOT / "schemas").rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for category in ("nodes", "generics"):
            for node in document.get(category, []):
                schemas[node["namespace"] + node["name"]] = node
    for path in sorted((ROOT / "schemas").rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for node in document.get("extensions", {}).get("nodes", []):
            schemas[node["kind"]].setdefault("relationships", []).extend(node.get("relationships", []))
    return schemas


SCHEMAS = _schemas()


def _ancestors(kind: str) -> set[str]:
    if kind.startswith(("Template", "Profile")):
        return {kind, "CoreObjectTemplate" if kind.startswith("Template") else "CoreProfile"}
    return {kind}.union(*(_ancestors(parent) for parent in SCHEMAS.get(kind, {}).get("inherit_from", [])))


def _relationships(kind: str) -> dict[str, dict[str, str]]:
    # The SDK derives Template/Profile schemas from their concrete source kind.
    base_kind = kind.removeprefix("Template").removeprefix("Profile")
    relationships = {
        item["name"]: item
        for ancestor in _ancestors(base_kind)
        for item in SCHEMAS.get(ancestor, {}).get("relationships", [])
    }
    # These relationships come from server built-ins / hierarchical generics.
    relationships.update(
        {
            "object_template": {"peer": "CoreObjectTemplate", "cardinality": "one"},
            "profiles": {"peer": "CoreProfile", "cardinality": "many"},
            "member_of_groups": {"peer": "CoreGroup", "cardinality": "many"},
        }
    )
    if kind in {"CoreIPAddressPool", "CoreIPPrefixPool"}:
        relationships["resources"] = {"peer": "IpamPrefix", "cardinality": "many"}
        relationships["ip_namespace"] = {"peer": "BuiltinIPNamespace", "cardinality": "one"}
    if kind in {"LocationRack", "NetworkPod"}:
        relationships["parent"] = {
            "peer": "LocationHall" if kind == "LocationRack" else "NetworkFabric",
            "cardinality": "one",
        }
    return relationships


def _rows(kind: str, row: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    rows = [(kind, row)]
    for field, value in row.items():
        if isinstance(value, dict) and "data" in value:
            peer = value.get("kind") or _relationships(kind)[field]["peer"]
            for child in value["data"]:
                rows.extend(_rows(peer, child))
    return rows


def _batches(selection: str) -> list[tuple[Path, list[tuple[str, dict[str, Any]]]]]:
    batches = []
    for directory in select_seed_data(OBJECTS, selection).directories:
        for path in sorted(directory.glob("*.yml")):
            for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
                rows = [entry for row in document["spec"]["data"] for entry in _rows(document["spec"]["kind"], row)]
                batches.append((path, rows))
    return batches


def _identity(kind: str, row: dict[str, Any]) -> tuple[Any, ...] | None:
    if kind == "DcimDeviceType":
        return (row["manufacturer"], row["name"])
    if kind == "EvpnDomain":
        return (row["fabric"], row["domain_id"])
    if kind == "IpamPrefix":
        return (row["prefix"], row.get("ip_namespace", "default"))
    for field in ("template_name", "profile_name", "name"):
        if field in row:
            return (row[field],)
    return None  # Inline designs/priorities have parent-scoped keys, never external references.


@pytest.mark.parametrize("selection", ["none", *FABRICS, "l3ls-multipod-a,l3ls-multipod-b", "l2ls,campus", "all"])
def test_every_reference_resolves_in_load_order(selection: str) -> None:
    available: dict[str, set[tuple[Any, ...]]] = defaultdict(set)
    available["BuiltinIPNamespace"].add(("default",))
    for path, rows in _batches(selection):
        for kind, row in rows:
            identity = _identity(kind, row)
            if identity:
                aliases = _ancestors(kind)
                if kind in {"CoreStandardGroup", "CoreAccountGroup"}:
                    aliases.add("CoreGroup")
                if kind in {"CoreIPAddressPool", "CoreIPPrefixPool"}:
                    aliases.add("CoreResourcePool")
                for alias in aliases:
                    available[alias].add(identity)
        for kind, row in rows:
            for field, relationship in _relationships(kind).items():
                if field not in row or isinstance(row[field], dict):
                    continue
                value = row[field]
                references = value if relationship["cardinality"] == "many" else [value]
                for reference in references:
                    identity = tuple(reference) if isinstance(reference, list) else (reference,)
                    if relationship["peer"] == "IpamPrefix" and len(identity) == 1:
                        identity = (*identity, "default")
                    assert identity in available[relationship["peer"]], f"{path}:{kind}.{field}: unresolved {identity}"


def test_profile_names_fit_infrahub_limit() -> None:
    for path, rows in _batches("all"):
        for kind, row in rows:
            if kind.startswith("Profile"):
                profile_name = row.get("profile_name")
                assert profile_name is None or len(profile_name) <= 32, (
                    f"{path}: {kind} profile_name exceeds 32 characters: {profile_name!r}"
                )


def test_multipod_b_server_template_variants_are_seeded() -> None:
    rows = [entry for _, batch in _batches("l3ls-multipod-b") for entry in batch]
    template_names = {row["template_name"] for kind, row in rows if kind == "TemplateComputePhysicalServer"}
    assert {
        "multipod-b-compute-server-single",
        "multipod-b-compute-server-dual",
    } <= template_names


def test_combined_load_has_no_cross_directory_identity_collisions() -> None:
    owners: dict[tuple[str, tuple[Any, ...]], Path] = {}
    for path, rows in _batches("all"):
        for kind, row in rows:
            identity = _identity(kind, row)
            if identity:
                key = (kind, identity)
                assert owners.setdefault(key, path.parent) == path.parent, f"{key} belongs to multiple seed directories"


def test_expected_fabric_names_and_only_arista_switch_templates() -> None:
    rows = [entry for _, batch in _batches("all") for entry in batch]
    assert {row["name"] for kind, row in rows if kind == "NetworkFabric"} == set(FABRICS.values())
    assert {row["name"] for kind, row in rows if kind == "OrganizationManufacturer"} == {"Arista"}
    assert {row["name"] for kind, row in rows if kind == "DcimDeviceType"} == {
        "Arista 7050CX3-32C",
        "Arista 7050SX3-48YC8C",
    }
    templates = [row for kind, row in rows if kind == "TemplateDcimDevice"]
    assert templates and all(row["template_name"].startswith("arista-") for row in templates)
    assert all(row["device_type"][0] == "Arista" for row in templates)


def _physical_ports(template_name: str) -> dict[str, str | None]:
    ports = {}
    rows = [entry for _, batch in _batches("none") for entry in batch]
    profiles = {row["profile_name"]: row["role"] for kind, row in rows if kind == "ProfileDcimInterface"}
    for kind, row in rows:
        if kind != "TemplateDcimDevice" or row["template_name"] != template_name:
            continue
        if row["interfaces"]["kind"] != "TemplateInterfacePhysical":
            continue
        for interface in row["interfaces"]["data"]:
            role = profiles[interface["profiles"][0]] if interface.get("profiles") else None
            match = re.fullmatch(r"(.*)\[(\d+)-(\d+)\](.*)", interface["name"])
            names = (
                [interface["name"]]
                if not match
                else [f"{match[1]}{i}{match[4]}" for i in range(int(match[2]), int(match[3]) + 1)]
            )
            for name in names:
                assert name not in ports
                ports[name] = role
    return ports


def test_multipod_spine_and_secondary_leaf_port_layouts() -> None:
    spine = _physical_ports("arista-7050cx3-32c-multipod-spine-switch")
    assert spine == {
        **{f"Ethernet{i}/1": "leaf" for i in range(1, 27)},
        **{f"Ethernet{i}/1": "super_spine" for i in range(27, 33)},
    }
    leaf = _physical_ports("arista-7050sx3-48yc8c-l2leaf-switch")
    assert leaf == {
        **{f"Ethernet{i}": "server" for i in range(1, 49)},
        **{f"Ethernet{i}/1": "leaf" for i in range(49, 51)},
        **{f"Ethernet{i}/1": None for i in range(51, 57)},
    }


def test_template_capacity_for_preserved_topologies() -> None:
    rows = [entry for _, batch in _batches("all") for entry in batch]
    fabrics = {row["name"]: row for kind, row in rows if kind == "NetworkFabric"}
    pods = {row["name"]: fabric for fabric in fabrics.values() for row in fabric.get("children", {}).get("data", [])}
    for fabric in fabrics.values():
        super_spines = sum(design["device_quantity"] for design in fabric.get("device_designs", {}).get("data", []))
        for pod in fabric.get("children", {}).get("data", []):
            for design in pod.get("device_designs", {}).get("data", []):
                ports = _physical_ports(design["device_template"])
                assert sum(role == "super_spine" for role in ports.values()) >= super_spines
                leaf_count = sum(
                    leaf["device_quantity"]
                    for kind, rack in rows
                    if kind == "LocationRack" and rack["pod"] == pod["name"]
                    for leaf in rack["device_designs"]["data"]
                    if leaf["role"] == "leaf"
                )
                assert sum(role == "leaf" for role in ports.values()) >= leaf_count
    for kind, rack in rows:
        if kind != "LocationRack":
            continue
        fabric = pods[rack["pod"]]
        pod = next(row for row in fabric["children"]["data"] if row["name"] == rack["pod"])
        spine_count = sum(design["device_quantity"] for design in pod.get("device_designs", {}).get("data", []))
        for design in rack["device_designs"]["data"]:
            ports = _physical_ports(design["device_template"])
            if design["role"] == "leaf":
                assert sum(role == "spine" for role in ports.values()) >= spine_count
            else:
                l3_leaf_count = sum(
                    leaf["device_quantity"] for leaf in rack["device_designs"]["data"] if leaf["role"] == "leaf"
                )
                assert sum(role == "leaf" for role in ports.values()) >= l3_leaf_count


@pytest.mark.parametrize(
    ("selection", "pods", "racks", "servers"),
    [
        ("campus", 1, 1, 2),
        ("isis-ldp", 1, 1, 0),
        ("l2ls", 1, 2, 4),
        ("l3ls-multi-domain", 2, 4, 2),
        ("l3ls-multipod-a", 3, 8, 3),
        ("l3ls-multipod-b", 3, 8, 2),
        ("single-dc-l3ls", 1, 2, 3),
    ],
)
def test_preserved_example_sizes(selection: str, pods: int, racks: int, servers: int) -> None:
    rows = [entry for _, batch in _batches(selection) for entry in batch]
    assert len({row["name"] for kind, row in rows if kind == "NetworkPod"}) == pods
    assert len({row["name"] for kind, row in rows if kind == "LocationRack"}) == racks
    assert len({row["name"] for kind, row in rows if kind == "ComputePhysicalServer"}) == servers


@pytest.mark.parametrize(
    ("selection", "profile", "tagged", "untagged"),
    [
        ("l3ls-multipod-b", "profile-multipod-b-compute", {"MultiPod-B-Servers", "MultiPod-B-Storage"}, None),
        ("campus", "profile-campus-endpoint", set(), "Campus-Data"),
        ("single-dc-l3ls", "profile-sdc-l3ls-server-compute", {"SDC-DATA", "SDC-STORAGE"}, None),
        ("single-dc-l3ls", "profile-sdc-l3ls-server-gpu", {"SDC-DATA", "SDC-STORAGE", "SDC-APP"}, None),
    ],
)
def test_local_server_vlan_bindings(selection: str, profile: str, tagged: set[str], untagged: str | None) -> None:
    rows = [entry for _, batch in _batches(selection) for entry in batch]
    row = next(row for kind, row in rows if kind == "ProfileDcimInterface" and row["profile_name"] == profile)
    assert {
        reference[0] if isinstance(reference, list) else reference for reference in row.get("tagged_vlan", [])
    } == tagged
    value = row.get("untagged_vlan")
    assert (value[0] if isinstance(value, list) else value) == untagged
    if selection == "l3ls-multipod-b":
        assert not any(kind in {"EvpnTenant", "IpamVRF"} for kind, _ in rows)
        vlans = {row["name"]: row for kind, row in rows if kind == "IpamVLAN"}
        assert vlans["MultiPod-B-Servers"]["vlan_id"] == 300
        assert vlans["MultiPod-B-Storage"]["vlan_id"] == 400
        assert {row["l2domain"] for row in vlans.values()} == {"Fabric-L3LS-MultiPod-B-L2Domain"}
