"""Exercise the real PyAVD pipeline with offline inputs and mocked file storage."""

import json
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
import yaml
from pyavd import get_avd_facts, get_device_structured_config, validate_inputs

from tests.unit.test_avd_anta_catalog import _data, _transform
from tests.unit.test_generate_avd_device_hostvar import _base_hostvars, _underlay_hostvars
from transforms.avd_device_doc import AvdDeviceDocTransform
from transforms.avd_eos_config import AvdEosConfigTransform
from transforms.avd_fabric_doc import AvdFabricDocTransform


def fabric_inputs(scenario: str) -> dict[str, dict]:
    """Build representative inputs through the repository's hostvar builder."""
    if scenario == "l2ls":
        leaf = _base_hostvars(
            [{"name": "TENANT", "mac_vrf_vni_base": 10000, "l2vlans": [{"id": 10, "name": "ACCESS"}]}],
            role="l2leaf",
            underlay_routing_protocol="none",
            mlag_capable=True,
            mlag_info={
                "domain_id": "L2LS",
                "bgp_asn": None,
                "virtual_router_mac": None,
                "peer_names": ["leaf1", "leaf2"],
                "mlag_peer_interfaces": ["Ethernet47", "Ethernet48"],
            },
            rack_info={"name": "L2LS", "mlag": True, "leaf_names": [], "avd_tags": []},
            pools={"uplink_ipv4_pool": None, "mlag_peer_ipv4_pool": "10.3.0.0/24", "mlag_peer_l3_ipv4_pool": None},
        )
        peer = deepcopy(leaf)
        peer["hostname"] = "leaf2"
        peer["l2leaf"]["nodes"][0].update(name="leaf2", id=4, mgmt_ip="192.168.0.4")
        return {"leaf1": leaf, "leaf2": peer}

    inputs = {
        "spine1": _underlay_hostvars(hostname="spine1", role="spine", node_id=1),
        "leaf1": _underlay_hostvars(
            hostname="leaf1",
            role="border_leaf" if scenario.startswith("gateway") else "leaf",
            node_id=3,
            uplinks={
                "uplink_interfaces": ["Ethernet1"],
                "uplink_switches": ["spine1"],
                "uplink_switch_interfaces": ["Ethernet1"],
            },
        ),
    }
    inputs["leaf1"]["l3leaf"]["nodes"][0]["virtual_router_mac_address"] = "00:1c:73:00:00:99"
    inputs["leaf1"]["tenants"] = [
        {
            "name": "TENANT",
            "mac_vrf_vni_base": 10000,
            "vrfs": [
                {
                    "name": "default",
                    "vrf_id": 1,
                    "svis": [
                        {"id": 20, "name": "SECOND", "enabled": True, "ip_address_virtual": "10.20.0.1/24"},
                        {"id": 10, "name": "FIRST", "enabled": True, "ip_address_virtual": "10.10.0.1/24"},
                    ],
                }
            ],
        }
    ]
    if scenario.startswith("gateway"):
        peer = _underlay_hostvars(hostname="leaf2", role="border_leaf", node_id=4)
        peer["l3leaf"]["nodes"][0]["vtep_loopback_ipv4_address"] = "10.2.0.4"
        inputs["leaf2"] = peer
        for hostname, remote in (("leaf1", "leaf2"), ("leaf2", "leaf1")):
            inputs[hostname]["l3leaf"]["nodes"][0]["evpn_gateway"] = {
                "remote_peers": [{"hostname": remote}]
                + (
                    [
                        {
                            "hostname": "spine1",
                            "ip_address": "192.0.2.1" if scenario == "gateway-alternate" else "10.0.0.1",
                        }
                    ]
                    if hostname == "leaf1" and scenario != "gateway-remote"
                    else []
                ),
                "evpn_l2": {"enabled": True},
                "evpn_l3": {"enabled": True, "inter_domain": True},
            }
    return inputs


def structured_configs(hostvars: dict[str, dict]) -> dict[str, dict]:
    validated = {}
    for hostname, inputs in hostvars.items():
        result = validate_inputs(inputs)
        assert not result.validation_result.violations
        validated[hostname] = result.validated_data
    facts = get_avd_facts(validated)
    return {
        hostname: get_device_structured_config(hostname=hostname, inputs=inputs, avd_facts=facts)._as_dict()
        for hostname, inputs in validated.items()
    }


@pytest.mark.parametrize("scenario", ["l3ls", "l2ls", "gateway", "gateway-alternate", "gateway-remote"])
async def test_real_rendering_from_stored_files(scenario: str) -> None:
    hostvars = fabric_inputs(scenario)
    configs = structured_configs(hostvars)
    stored = {f"hv-{name}": json.dumps(data) for name, data in hostvars.items()}
    stored.update({f"sc-{name}": json.dumps(data) for name, data in configs.items()})

    async def get_file(*_args: object, **kwargs: str) -> AsyncMock:
        file = AsyncMock()
        file.download_file.return_value = stored[kwargs["id"]]
        return file

    client = AsyncMock()
    client.get.side_effect = get_file
    device_data = {
        "DcimDevice": {
            "edges": [
                {
                    "node": {
                        "id": "leaf1",
                        "name": {"value": "leaf1"},
                        "avd_artifact": {
                            "node": {
                                "id": "artifact-leaf1",
                                "structured_config_file": {
                                    "node": {"id": "sc-leaf1", "checksum": {"value": "fixture"}}
                                },
                            }
                        },
                    }
                }
            ]
        }
    }
    for transform_class in (AvdEosConfigTransform, AvdDeviceDocTransform):
        transform = transform_class.__new__(transform_class)
        transform._init_client = client
        rendered = await transform.transform(device_data)
        assert "leaf1" in rendered
        assert len(rendered) > 100

    fabric_data = {
        "NetworkFabric": {"edges": [{"node": {"id": "fabric-1", "name": {"value": "Fixture"}}}]},
        "DcimDevice": {
            "edges": [
                {
                    "node": {
                        "id": name,
                        "name": {"value": name},
                        "pod": {
                            "node": {
                                "id": "pod-1",
                                "parent": {"node": {"__typename": "NetworkFabric", "id": "fabric-1"}},
                            }
                        },
                        "avd_artifact": {
                            "node": {
                                "id": f"artifact-{name}",
                                "hostvar_file": {"node": {"id": f"hv-{name}"}},
                                "structured_config_file": {"node": {"id": f"sc-{name}"}},
                            }
                        },
                    }
                }
                for name in hostvars
            ]
        },
    }
    fabric_transform = AvdFabricDocTransform.__new__(AvdFabricDocTransform)
    fabric_transform._init_client = client
    assert "Fixture" in await fabric_transform.transform(fabric_data)

    unfiltered_catalog = await _transform(configs["leaf1"]).transform(_data(anta_enabled=True))
    assert "VerifyInterfaceDiscards" in unfiltered_catalog
    assert "VerifyLoggingErrors" in unfiltered_catalog
    catalog = await _transform(configs["leaf1"]).transform(
        _data(
            anta_enabled=True,
            avd_catalogs_filters=["VerifyInterfaceDiscards", "VerifyLoggingErrors"],
        )
    )
    parsed_catalog = yaml.safe_load(catalog)
    assert parsed_catalog
    assert "VerifyInterfaceDiscards" not in catalog
    assert "VerifyLoggingErrors" not in catalog


def test_default_vrf_prefix_list_order_is_consistent() -> None:
    configs = structured_configs(fabric_inputs("l3ls"))
    prefix_list = next(item for item in configs["leaf1"]["prefix_lists"] if item["name"] == "PL-SVI-VRF-DEFAULT")
    assert [entry["action"] for entry in prefix_list["sequence_numbers"]] == [
        "permit 10.10.0.0/24",
        "permit 10.20.0.0/24",
    ]


@pytest.mark.parametrize("scenario", ["gateway", "gateway-alternate"])
def test_gateway_route_server_peering(scenario: str) -> None:
    config = structured_configs(fabric_inputs(scenario))["leaf1"]
    neighbors = {neighbor["ip_address"]: neighbor for neighbor in config["router_bgp"]["neighbors"]}
    core_address = "192.0.2.1" if scenario == "gateway-alternate" else "10.0.0.1"
    assert neighbors[core_address]["peer_group"] == "EVPN-OVERLAY-CORE"
    if scenario == "gateway-alternate":
        assert neighbors["10.0.0.1"]["peer_group"] == "EVPN-OVERLAY-PEERS"
