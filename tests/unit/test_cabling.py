"""Unit tests for the index-based cabling-plan builders in cabling.py.

These validate the pure topology algorithms (no Infrahub client needed) using
lightweight mock interface/device objects, mirroring TestBuildServerCablingPlan
in test_server_cabling.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from solution_arista_avd.cabling import (
    build_pod_cabling_plan,
    build_rack_cabling_plan,
    build_server_cabling_plan,
    connect_interface_maps,
)


def _iface(iface_id: str, *, device_label: str = "", device_id: str = "") -> MagicMock:
    """A mock interface identified by .id (all the builders read off it)."""
    iface = MagicMock()
    iface.id = iface_id
    iface.device.display_label = device_label
    iface.device.id = device_id
    return iface


def _device(index: int | None = None, *, name: str = "", device_id: str = "") -> MagicMock:
    """A mock device; build_rack_cabling_plan reads device.index.value."""
    device = MagicMock()
    device.id = device_id
    device.index.value = index
    device.name.value = name
    device.display_label = name
    return device


def _device_without_hydrated_index(name: str) -> MagicMock:
    return _device(name=name, device_id=name)


def _ids(plan: list[tuple[MagicMock, MagicMock]]) -> list[tuple[str, str]]:
    return [(src.id, dst.id) for src, dst in plan]


class TestBuildPodCablingPlan:
    """spine -> super-spine cabling, offset per pod_index."""

    def test_two_spines_two_super_spines_first_pod(self) -> None:
        spine1, spine2 = _device(), _device()
        ss1, ss2 = _device(), _device()

        # Each spine has one interface per super-spine.
        src_map = {
            spine1: [_iface("sp1-i0"), _iface("sp1-i1")],
            spine2: [_iface("sp2-i0"), _iface("sp2-i1")],
        }
        # Each super-spine has a column of spine-facing interfaces.
        dst_map = {
            ss1: [_iface("ss1-0"), _iface("ss1-1"), _iface("ss1-2"), _iface("ss1-3")],
            ss2: [_iface("ss2-0"), _iface("ss2-1"), _iface("ss2-2"), _iface("ss2-3")],
        }

        plan = build_pod_cabling_plan(pod_index=2, src_interface_map=src_map, dst_interface_map=dst_map)

        # pod_index 2 -> base column 0. Spine N uses column N across every super-spine.
        assert _ids(plan) == [
            ("sp1-i0", "ss1-0"),
            ("sp1-i1", "ss2-0"),
            ("sp2-i0", "ss1-1"),
            ("sp2-i1", "ss2-1"),
        ]

    def test_pod_index_offsets_super_spine_column(self) -> None:
        spine1, spine2 = _device(), _device()
        ss1, ss2 = _device(), _device()
        src_map = {
            spine1: [_iface("sp1-i0"), _iface("sp1-i1")],
            spine2: [_iface("sp2-i0"), _iface("sp2-i1")],
        }
        dst_map = {
            ss1: [_iface("ss1-0"), _iface("ss1-1"), _iface("ss1-2"), _iface("ss1-3")],
            ss2: [_iface("ss2-0"), _iface("ss2-1"), _iface("ss2-2"), _iface("ss2-3")],
        }

        plan = build_pod_cabling_plan(pod_index=3, src_interface_map=src_map, dst_interface_map=dst_map)

        # pod_index 3 -> base = (3-2)*2 = column 2, so this pod consumes a
        # distinct block of super-spine ports from pod_index 2.
        assert _ids(plan) == [
            ("sp1-i0", "ss1-2"),
            ("sp1-i1", "ss2-2"),
            ("sp2-i0", "ss1-3"),
            ("sp2-i1", "ss2-3"),
        ]

    def test_device_map_insertion_order_does_not_change_plan(self) -> None:
        spine2 = _device(name="spine-2", device_id="spine-2")
        spine10 = _device(name="spine-10", device_id="spine-10")
        super_spine2 = _device(name="super-spine-2", device_id="super-spine-2")
        super_spine10 = _device(name="super-spine-10", device_id="super-spine-10")
        spine2_interfaces = [_iface("sp2-i0"), _iface("sp2-i1")]
        spine10_interfaces = [_iface("sp10-i0"), _iface("sp10-i1")]
        super_spine2_interfaces = [_iface(f"ss2-{index}") for index in range(4)]
        super_spine10_interfaces = [_iface(f"ss10-{index}") for index in range(4)]

        expected = _ids(
            build_pod_cabling_plan(
                pod_index=2,
                src_interface_map={spine2: spine2_interfaces, spine10: spine10_interfaces},
                dst_interface_map={
                    super_spine2: super_spine2_interfaces,
                    super_spine10: super_spine10_interfaces,
                },
            )
        )
        reordered = _ids(
            build_pod_cabling_plan(
                pod_index=2,
                src_interface_map={spine10: spine10_interfaces, spine2: spine2_interfaces},
                dst_interface_map={
                    super_spine10: super_spine10_interfaces,
                    super_spine2: super_spine2_interfaces,
                },
            )
        )

        assert reordered == expected

    def test_partial_device_index_hydration_does_not_change_plan(self) -> None:
        spine1_indexed = _device(index=1, name="spine-1", device_id="spine-1")
        spine2_unindexed = _device(name="spine-2", device_id="spine-2")
        spine1_unindexed = _device(name="spine-1", device_id="spine-1")
        spine2_indexed = _device(index=2, name="spine-2", device_id="spine-2")
        super_spine1 = _device(name="super-spine-1", device_id="super-spine-1")
        super_spine2 = _device(name="super-spine-2", device_id="super-spine-2")
        spine1_interfaces = [_iface("sp1-i0"), _iface("sp1-i1")]
        spine2_interfaces = [_iface("sp2-i0"), _iface("sp2-i1")]
        super_spine1_interfaces = [_iface(f"ss1-{index}") for index in range(4)]
        super_spine2_interfaces = [_iface(f"ss2-{index}") for index in range(4)]
        destination_map = {
            super_spine1: super_spine1_interfaces,
            super_spine2: super_spine2_interfaces,
        }

        first = _ids(
            build_pod_cabling_plan(
                pod_index=2,
                src_interface_map={spine2_unindexed: spine2_interfaces, spine1_indexed: spine1_interfaces},
                dst_interface_map=destination_map,
            )
        )
        second = _ids(
            build_pod_cabling_plan(
                pod_index=2,
                src_interface_map={spine1_unindexed: spine1_interfaces, spine2_indexed: spine2_interfaces},
                dst_interface_map=destination_map,
            )
        )

        assert second == first


class TestBuildRackCablingPlan:
    """leaf -> spine cabling, windowed per rack_index and indexed by leaf index."""

    def test_two_leaves_two_spines_first_rack(self) -> None:
        leaf1, leaf2 = _device(index=1), _device(index=2)
        spine1, spine2 = _device(), _device()
        src_map = {
            leaf1: [_iface("l1-i0"), _iface("l1-i1")],
            leaf2: [_iface("l2-i0"), _iface("l2-i1")],
        }
        dst_map = {
            spine1: [_iface("sp1-0"), _iface("sp1-1"), _iface("sp1-2"), _iface("sp1-3")],
            spine2: [_iface("sp2-0"), _iface("sp2-1"), _iface("sp2-2"), _iface("sp2-3")],
        }

        plan = build_rack_cabling_plan(rack_index=1, src_interface_map=src_map, dst_interface_map=dst_map)

        # rack_index 1 -> window [0:2] on each spine; leaf index selects within it.
        assert _ids(plan) == [
            ("l1-i0", "sp1-0"),
            ("l1-i1", "sp2-0"),
            ("l2-i0", "sp1-1"),
            ("l2-i1", "sp2-1"),
        ]

    def test_rack_index_shifts_spine_window(self) -> None:
        leaf1, leaf2 = _device(index=1), _device(index=2)
        spine1, spine2 = _device(), _device()
        src_map = {
            leaf1: [_iface("l1-i0"), _iface("l1-i1")],
            leaf2: [_iface("l2-i0"), _iface("l2-i1")],
        }
        dst_map = {
            spine1: [_iface("sp1-0"), _iface("sp1-1"), _iface("sp1-2"), _iface("sp1-3")],
            spine2: [_iface("sp2-0"), _iface("sp2-1"), _iface("sp2-2"), _iface("sp2-3")],
        }

        plan = build_rack_cabling_plan(rack_index=2, src_interface_map=src_map, dst_interface_map=dst_map)

        # rack_index 2 -> window [2:4]; a different rack lands on different spine ports.
        assert _ids(plan) == [
            ("l1-i0", "sp1-2"),
            ("l1-i1", "sp2-2"),
            ("l2-i0", "sp1-3"),
            ("l2-i1", "sp2-3"),
        ]

    def test_missing_hydrated_device_index_uses_generated_name_suffix(self) -> None:
        l2leaf = _device_without_hydrated_index("l2leaf-pod-a2-1-1")
        leaf1, leaf2 = _device(), _device()
        src_map = {
            l2leaf: [_iface("l2leaf-i0"), _iface("l2leaf-i1")],
        }
        dst_map = {
            leaf1: [_iface("leaf1-0"), _iface("leaf1-1"), _iface("leaf1-2"), _iface("leaf1-3")],
            leaf2: [_iface("leaf2-0"), _iface("leaf2-1"), _iface("leaf2-2"), _iface("leaf2-3")],
        }

        plan = build_rack_cabling_plan(rack_index=2, src_interface_map=src_map, dst_interface_map=dst_map)

        assert _ids(plan) == [
            ("l2leaf-i0", "leaf1-2"),
            ("l2leaf-i1", "leaf2-2"),
        ]

    def test_device_map_insertion_order_does_not_change_plan(self) -> None:
        leaf1 = _device(index=1, name="leaf-1", device_id="leaf-1")
        leaf2 = _device(index=2, name="leaf-2", device_id="leaf-2")
        spine1 = _device(index=1, name="spine-1", device_id="spine-1")
        spine2 = _device(index=2, name="spine-2", device_id="spine-2")
        leaf1_interfaces = [_iface("l1-i0"), _iface("l1-i1")]
        leaf2_interfaces = [_iface("l2-i0"), _iface("l2-i1")]
        spine1_interfaces = [_iface(f"sp1-{index}") for index in range(4)]
        spine2_interfaces = [_iface(f"sp2-{index}") for index in range(4)]

        expected = _ids(
            build_rack_cabling_plan(
                rack_index=1,
                src_interface_map={leaf1: leaf1_interfaces, leaf2: leaf2_interfaces},
                dst_interface_map={spine1: spine1_interfaces, spine2: spine2_interfaces},
            )
        )
        reordered = _ids(
            build_rack_cabling_plan(
                rack_index=1,
                src_interface_map={leaf2: leaf2_interfaces, leaf1: leaf1_interfaces},
                dst_interface_map={spine2: spine2_interfaces, spine1: spine1_interfaces},
            )
        )

        assert reordered == expected

    def test_partial_device_index_hydration_uses_whole_map_fallback(self) -> None:
        leaf1_indexed = _device(index=2, name="leaf-1", device_id="leaf-1")
        leaf2_unindexed = _device(name="leaf-2", device_id="leaf-2")
        leaf1_unindexed = _device(name="leaf-1", device_id="leaf-1")
        leaf2_indexed = _device(index=1, name="leaf-2", device_id="leaf-2")
        spine1 = _device(name="spine-1", device_id="spine-1")
        spine2 = _device(name="spine-2", device_id="spine-2")
        leaf1_interfaces = [_iface("l1-i0"), _iface("l1-i1")]
        leaf2_interfaces = [_iface("l2-i0"), _iface("l2-i1")]
        destination_map = {
            spine1: [_iface("sp1-0"), _iface("sp1-1")],
            spine2: [_iface("sp2-0"), _iface("sp2-1")],
        }

        first = _ids(
            build_rack_cabling_plan(
                rack_index=1,
                src_interface_map={leaf2_unindexed: leaf2_interfaces, leaf1_indexed: leaf1_interfaces},
                dst_interface_map=destination_map,
            )
        )
        second = _ids(
            build_rack_cabling_plan(
                rack_index=1,
                src_interface_map={leaf1_unindexed: leaf1_interfaces, leaf2_indexed: leaf2_interfaces},
                dst_interface_map=destination_map,
            )
        )

        assert (
            first
            == second
            == [
                ("l1-i0", "sp1-0"),
                ("l1-i1", "sp2-0"),
                ("l2-i0", "sp1-1"),
                ("l2-i1", "sp2-1"),
            ]
        )

    def test_interface_relationship_identity_stabilizes_unhydrated_device_keys(self) -> None:
        first_leaf = _device()
        second_leaf = _device()
        first_spine = _device()
        second_spine = _device()
        leaf1_interfaces = [
            _iface("l1-i0", device_label="leaf-1", device_id="leaf-1"),
            _iface("l1-i1", device_label="leaf-1", device_id="leaf-1"),
        ]
        leaf2_interfaces = [
            _iface("l2-i0", device_label="leaf-2", device_id="leaf-2"),
            _iface("l2-i1", device_label="leaf-2", device_id="leaf-2"),
        ]
        spine1_interfaces = [
            _iface("sp1-0", device_label="spine-1", device_id="spine-1"),
            _iface("sp1-1", device_label="spine-1", device_id="spine-1"),
        ]
        spine2_interfaces = [
            _iface("sp2-0", device_label="spine-2", device_id="spine-2"),
            _iface("sp2-1", device_label="spine-2", device_id="spine-2"),
        ]

        plan = build_rack_cabling_plan(
            rack_index=1,
            src_interface_map={second_leaf: leaf2_interfaces, first_leaf: leaf1_interfaces},
            dst_interface_map={second_spine: spine2_interfaces, first_spine: spine1_interfaces},
        )

        assert _ids(plan) == [
            ("l1-i0", "sp1-0"),
            ("l1-i1", "sp2-0"),
            ("l2-i0", "sp1-1"),
            ("l2-i1", "sp2-1"),
        ]


class TestBuildServerCablingPlan:
    def test_device_map_insertion_order_does_not_change_plan(self) -> None:
        server1 = _device(index=1, name="server-1", device_id="server-1")
        server2 = _device(index=2, name="server-2", device_id="server-2")
        leaf1 = _device(index=1, name="leaf-1", device_id="leaf-1")
        leaf2 = _device(index=2, name="leaf-2", device_id="leaf-2")
        server1_interfaces = [_iface("server1-i0"), _iface("server1-i1")]
        server2_interfaces = [_iface("server2-i0"), _iface("server2-i1")]
        leaf1_interfaces = [_iface("leaf1-i0")]
        leaf2_interfaces = [_iface("leaf2-i0")]

        expected = _ids(
            build_server_cabling_plan(
                server_index=0,
                src_interface_map={server1: server1_interfaces, server2: server2_interfaces},
                dst_interface_map={leaf1: leaf1_interfaces, leaf2: leaf2_interfaces},
            )
        )
        reordered = _ids(
            build_server_cabling_plan(
                server_index=0,
                src_interface_map={server2: server2_interfaces, server1: server1_interfaces},
                dst_interface_map={leaf2: leaf2_interfaces, leaf1: leaf1_interfaces},
            )
        )

        assert reordered == expected

    def test_partial_device_index_hydration_does_not_change_plan(self) -> None:
        server1_indexed = _device(index=2, name="server-1", device_id="server-1")
        server2_unindexed = _device(name="server-2", device_id="server-2")
        server1_unindexed = _device(name="server-1", device_id="server-1")
        server2_indexed = _device(index=1, name="server-2", device_id="server-2")
        leaf1 = _device(name="leaf-1", device_id="leaf-1")
        leaf2 = _device(name="leaf-2", device_id="leaf-2")
        server1_interfaces = [_iface("server1-i0"), _iface("server1-i1")]
        server2_interfaces = [_iface("server2-i0"), _iface("server2-i1")]
        destination_map = {leaf1: [_iface("leaf1-i0")], leaf2: [_iface("leaf2-i0")]}

        first = _ids(
            build_server_cabling_plan(
                server_index=0,
                src_interface_map={server2_unindexed: server2_interfaces, server1_indexed: server1_interfaces},
                dst_interface_map=destination_map,
            )
        )
        second = _ids(
            build_server_cabling_plan(
                server_index=0,
                src_interface_map={server1_unindexed: server1_interfaces, server2_indexed: server2_interfaces},
                dst_interface_map=destination_map,
            )
        )

        assert first == second


def _physical_interface(
    interface_id: str,
    device_label: str,
    interface_name: str,
    connector_id: str | None = None,
) -> MagicMock:
    iface = MagicMock()
    iface.id = interface_id
    iface.name.value = interface_name
    iface.device.display_label = device_label
    iface.display_label = f"{device_label}-{interface_name}"
    iface.connector = SimpleNamespace(id=connector_id)
    iface.status.value = "planned"
    iface.save = AsyncMock()
    return iface


class _EmptySdkRelationship:
    @property
    def peer(self) -> object:
        raise ValueError("Node must have at least one identifier (ID or HFID) to query it.")


class TestConnectInterfaceMaps:
    @pytest.mark.asyncio
    async def test_populates_missing_connectors_with_deterministic_link(self) -> None:
        src_query_iface = _physical_interface("src-query", "leaf-1", "Ethernet1")
        dst_query_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        src_fetched_iface = _physical_interface("src-query", "leaf-1", "Ethernet1")
        dst_fetched_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        network_link = SimpleNamespace(id="link-1", name="leaf-1-Ethernet1__spine-1-Ethernet1", save=AsyncMock())
        client = SimpleNamespace(
            create=AsyncMock(return_value=network_link),
            get=AsyncMock(side_effect=[src_fetched_iface, dst_fetched_iface]),
        )
        logger = MagicMock()

        await connect_interface_maps(client, logger, [(src_query_iface, dst_query_iface)])  # type: ignore[arg-type]

        client.create.assert_awaited_once_with(
            kind="NetworkLink",
            name="leaf-1-Ethernet1__spine-1-Ethernet1",
            medium="copper",
        )
        network_link.save.assert_awaited_once_with(allow_upsert=True)
        assert src_fetched_iface.connector is network_link
        assert dst_fetched_iface.connector is network_link
        assert src_fetched_iface.status.value == "active"
        assert dst_fetched_iface.status.value == "active"
        src_fetched_iface.save.assert_awaited_once_with(allow_upsert=True)
        dst_fetched_iface.save.assert_awaited_once_with(allow_upsert=True)

    @pytest.mark.asyncio
    async def test_preserves_existing_conflicting_connectors_and_logs_skip(self) -> None:
        src_query_iface = _physical_interface("src-query", "leaf-1", "Ethernet1")
        dst_query_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        src_fetched_iface = _physical_interface("src-query", "leaf-1", "Ethernet1", connector_id="manual-link")
        dst_fetched_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        network_link = SimpleNamespace(
            id="generated-link", name="leaf-1-Ethernet1__spine-1-Ethernet1", save=AsyncMock()
        )
        client = SimpleNamespace(
            create=AsyncMock(return_value=network_link),
            get=AsyncMock(side_effect=[src_fetched_iface, dst_fetched_iface]),
        )
        logger = MagicMock()

        await connect_interface_maps(client, logger, [(src_query_iface, dst_query_iface)])  # type: ignore[arg-type]

        assert src_fetched_iface.connector.id == "manual-link"
        src_fetched_iface.save.assert_not_awaited()
        assert dst_fetched_iface.connector is network_link
        dst_fetched_iface.save.assert_awaited_once_with(allow_upsert=True)
        assert any("Skipped connector reconciliation" in call.args[0] for call in logger.warning.call_args_list)

    @pytest.mark.asyncio
    async def test_populates_empty_sdk_connector_relationship(self) -> None:
        src_query_iface = _physical_interface("src-query", "leaf-1", "Ethernet1")
        dst_query_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        src_fetched_iface = _physical_interface("src-query", "leaf-1", "Ethernet1")
        dst_fetched_iface = _physical_interface("dst-query", "spine-1", "Ethernet1")
        src_fetched_iface.connector = _EmptySdkRelationship()
        dst_fetched_iface.connector = _EmptySdkRelationship()
        network_link = SimpleNamespace(id="link-1", name="leaf-1-Ethernet1__spine-1-Ethernet1", save=AsyncMock())
        client = SimpleNamespace(
            create=AsyncMock(return_value=network_link),
            get=AsyncMock(side_effect=[src_fetched_iface, dst_fetched_iface]),
        )
        logger = MagicMock()

        await connect_interface_maps(client, logger, [(src_query_iface, dst_query_iface)])  # type: ignore[arg-type]

        assert src_fetched_iface.connector is network_link
        assert dst_fetched_iface.connector is network_link
        src_fetched_iface.save.assert_awaited_once_with(allow_upsert=True)
        dst_fetched_iface.save.assert_awaited_once_with(allow_upsert=True)
