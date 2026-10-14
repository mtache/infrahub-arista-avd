from __future__ import annotations

import re
from collections import defaultdict
from typing import TYPE_CHECKING, cast

from netutils.interface import sort_interface_list

if TYPE_CHECKING:
    from .protocols import DcimDevice, DcimInterface


def create_sorted_device_interface_map(
    interfaces: list[DcimInterface],
) -> dict[DcimDevice, list[DcimInterface]]:
    """
    Creates a dictionary that maps a device hostname to a sorted list of interfaces from a list of interfaces
    """

    device_interface_map: dict[DcimDevice, list[DcimInterface]] = defaultdict(list)

    for interface in interfaces:
        device_interface_map[cast("DcimDevice", interface.device.peer)].append(interface)

    for device, intfs in device_interface_map.items():
        interface_map = {interface.name.value: interface for interface in intfs}
        sorted_interface_names = sort_interface_list(list(interface_map.keys()))
        device_interface_map[device] = [interface_map[interface] for interface in sorted_interface_names]

    return _sort_devices(device_interface_map)


def create_reverse_sorted_device_interface_map(
    interfaces: list[DcimInterface],
) -> dict[DcimDevice, list[DcimInterface]]:
    device_interface_map: dict[DcimDevice, list[DcimInterface]] = defaultdict(list)

    for interface in interfaces:
        device_interface_map[cast("DcimDevice", interface.device.peer)].append(interface)

    for device, intfs in device_interface_map.items():
        interface_map = {interface.name.value: interface for interface in intfs}
        sorted_interface_names = sort_interface_list(list(interface_map.keys()))
        sorted_interface_names.reverse()
        device_interface_map[device] = [interface_map[interface] for interface in sorted_interface_names]

    return _sort_devices(device_interface_map)


def _sort_devices(interface_map: dict[DcimDevice, list[DcimInterface]]) -> dict[DcimDevice, list[DcimInterface]]:
    """Keep cabling independent of GraphQL edge order and partial index hydration."""

    def index(device: DcimDevice) -> int | None:
        value = getattr(getattr(device, "index", None), "value", None)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
        if isinstance(value, str) and value.isdecimal() and int(value) > 0:
            return int(value)
        return None

    def name_key(device: DcimDevice) -> tuple:
        name = getattr(getattr(device, "name", None), "value", None) or device.display_label
        return tuple(
            (1, int(part)) if part.isdecimal() else (0, part.casefold()) for part in re.split(r"(\d+)", str(name))
        )

    indexed = all(index(device) is not None for device in interface_map)
    devices = sorted(
        interface_map, key=lambda device: ((index(device),) if indexed else ()) + (name_key(device), device.id)
    )
    return {device: interface_map[device] for device in devices}
