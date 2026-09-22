from __future__ import annotations

import re
from sys import maxsize
from typing import TYPE_CHECKING

from .protocols import DcimDevice, DcimInterface, InterfacePhysical

if TYPE_CHECKING:
    import logging

    from infrahub_sdk import InfrahubClient


def build_pod_cabling_plan(
    pod_index: int,
    src_interface_map: dict[DcimDevice, list[DcimInterface]],
    dst_interface_map: dict[DcimDevice, list[DcimInterface]],
) -> list[tuple[DcimInterface, DcimInterface]]:
    """Builds a cabling plan between source and destination interfaces based on Indexes.

    See tests/unit/test_cabling.py for the behavioural contract.
    """
    src_items = _sorted_device_items(src_interface_map)
    dst_items = _sorted_device_items(dst_interface_map)
    dst_devices = [device for device, _interfaces in dst_items]
    dst_device_count = len(dst_devices)
    dst_interface_base_index = (pod_index - 2) * len(dst_interface_map)

    cabling_plan: list[tuple[DcimInterface, DcimInterface]] = []

    for src_index, (_src_device, src_interfaces) in enumerate(src_items):
        dst_interface_index = dst_interface_base_index + src_index

        for dst_index, src_interface in enumerate(src_interfaces[:dst_device_count]):
            dst_interface = dst_interface_map[dst_devices[dst_index]][dst_interface_index]

            cabling_plan.append((src_interface, dst_interface))

    return cabling_plan


def build_rack_cabling_plan(
    rack_index: int,
    src_interface_map: dict[DcimDevice, list[DcimInterface]],
    dst_interface_map: dict[DcimDevice, list[DcimInterface]],
) -> list[tuple[DcimInterface, DcimInterface]]:
    cabling_plan: list[tuple[DcimInterface, DcimInterface]] = []
    src_items = _sorted_device_items(src_interface_map)
    dst_items = _sorted_device_items(dst_interface_map)
    dst_devices = [device for device, _interfaces in dst_items]
    dst_device_count = len(dst_devices)
    start = (rack_index * 2) - 2
    end = start + 2

    for fallback_index, (src_device, src_interfaces) in enumerate(src_items, start=1):
        src_device_index = _rack_source_device_index(src_device, fallback=fallback_index)

        for dst_index, src_interface in enumerate(src_interfaces[:dst_device_count]):
            dst_interface = dst_interface_map[dst_devices[dst_index]][start:end][src_device_index - 1]
            cabling_plan.append((src_interface, dst_interface))

    return cabling_plan


def _rack_source_device_index(device: DcimDevice, *, fallback: int) -> int:
    value = getattr(getattr(device, "index", None), "value", None)
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)

    for name_attr in ("name", "display_label"):
        candidate = getattr(device, name_attr, None)
        name = getattr(candidate, "value", candidate)
        if isinstance(name, str):
            suffix = name.rsplit("-", maxsplit=1)[-1]
            if suffix.isdecimal() and int(suffix) > 0:
                return int(suffix)

    return fallback


def build_server_cabling_plan(
    server_index: int,
    src_interface_map: dict[DcimDevice, list[DcimInterface]],
    dst_interface_map: dict[DcimDevice, list[DcimInterface]],
) -> list[tuple[DcimInterface, DcimInterface]]:
    """Builds a cabling plan connecting server interfaces to leaf switch interfaces.

    Each server interface is paired with a leaf at the given index position,
    round-robin across leaves. Follows the same index-based pattern as
    build_pod_cabling_plan and build_rack_cabling_plan.
    """
    src_items = _sorted_device_items(src_interface_map)
    dst_items = _sorted_device_items(dst_interface_map)
    dst_devices = [device for device, _interfaces in dst_items]
    dst_device_count = len(dst_devices)
    cabling_plan: list[tuple[DcimInterface, DcimInterface]] = []

    for _src_device, src_interfaces in src_items:
        for i, src_interface in enumerate(src_interfaces):
            dst_device = dst_devices[i % dst_device_count]
            dst_offset = server_index + (i // dst_device_count)
            dst_interface = dst_interface_map[dst_device][dst_offset]
            cabling_plan.append((src_interface, dst_interface))

    return cabling_plan


def _sorted_device_items(
    interface_map: dict[DcimDevice, list[DcimInterface]],
) -> list[tuple[DcimDevice, list[DcimInterface]]]:
    """Return interface-map entries in a stable topology order.

    Infrahub does not guarantee GraphQL edge order. Cabling plans therefore
    cannot use dictionary insertion order to decide which devices are paired.
    Prefer the explicit device index, then a naturally sorted hostname, with
    the immutable node ID as the final tie-breaker.
    """
    return sorted(interface_map.items(), key=lambda item: _device_order_key(item[0]))


def _device_order_key(device: DcimDevice) -> tuple[int, tuple[tuple[int, str | int], ...], str]:
    index = _positive_int(getattr(getattr(device, "index", None), "value", None))
    name = _device_text_value(device, "name") or _device_text_value(device, "display_label")
    device_id = getattr(device, "id", "")
    return index if index is not None else maxsize, _natural_sort_key(name), str(device_id)


def _positive_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return None


def _device_text_value(device: DcimDevice, attribute_name: str) -> str:
    attribute = getattr(device, attribute_name, None)
    value = getattr(attribute, "value", attribute)
    return value if isinstance(value, str) else ""


def _natural_sort_key(value: str) -> tuple[tuple[int, str | int], ...]:
    return tuple((1, int(part)) if part.isdecimal() else (0, part.casefold()) for part in re.split(r"(\d+)", value))


async def connect_interface_maps(
    client: InfrahubClient, logger: logging.Logger, cabling_plan: list[tuple[DcimInterface, DcimInterface]]
) -> None:
    for src_interface, dst_interface in cabling_plan:
        name = f"{src_interface.device.display_label}-{src_interface.name.value}__{dst_interface.device.display_label}-{dst_interface.name.value}"
        network_link = await client.create(kind="NetworkLink", name=name, medium="copper")
        await network_link.save(allow_upsert=True)

        src_populated = await _connect_interface_if_missing(client, logger, src_interface, network_link)
        dst_populated = await _connect_interface_if_missing(client, logger, dst_interface, network_link)

        if src_populated or dst_populated:
            logger.info("Populated missing generated connector(s) for %s", name)
        else:
            logger.info("Preserved existing connector state for %s", name)


async def _connect_interface_if_missing(
    client: InfrahubClient, logger: logging.Logger, interface: DcimInterface, network_link: object
) -> bool:
    # Set connector using InterfacePhysical, the concrete type that exposes the
    # connector relationship from DcimEndpoint.
    iface = await client.get(InterfacePhysical, id=interface.id, include=["connector"])  # type: ignore[type-abstract]
    connector_id = _relationship_node_id(getattr(iface, "connector", None))
    network_link_id = getattr(network_link, "id", None)

    if connector_id:
        if network_link_id and connector_id == network_link_id:
            logger.info("Preserved generated connector on %s", iface.display_label)
        else:
            logger.warning(
                "Skipped connector reconciliation for %s: existing connector %s conflicts with generated link %s",
                iface.display_label,
                connector_id,
                getattr(network_link, "name", network_link_id),
            )
        return False

    # SDK accepts protocol kinds at runtime; assigning a node to a relationship is the SDK pattern.
    iface.connector = network_link  # type: ignore[assignment]
    iface.status.value = "active"
    await iface.save(allow_upsert=True)
    logger.info("Populated missing connector on %s", iface.display_label)
    return True


def _relationship_node_id(relationship: object | None) -> str | None:
    if relationship is None:
        return None
    relationship_id = getattr(relationship, "id", None)
    if isinstance(relationship_id, str) and relationship_id:
        return relationship_id
    node = getattr(relationship, "node", None)
    if node is None:
        try:
            node = getattr(relationship, "peer", None)
        except ValueError as exc:
            if str(exc) == "Node must have at least one identifier (ID or HFID) to query it.":
                return None
            raise
    node_id = getattr(node, "id", None)
    if isinstance(node_id, str) and node_id:
        return node_id
    return None
