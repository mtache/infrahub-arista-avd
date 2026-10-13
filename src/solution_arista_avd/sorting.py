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

    for device, intfs in sorted(device_interface_map.items(), key=lambda item: _device_sort_key(item[0])):
        interface_map = {interface.name.value: interface for interface in intfs}
        sorted_interface_names = sort_interface_list(list(interface_map.keys()))
        device_interface_map[device] = [interface_map[interface] for interface in sorted_interface_names]

    return dict(sorted(device_interface_map.items(), key=lambda item: _device_sort_key(item[0])))


def create_reverse_sorted_device_interface_map(
    interfaces: list[DcimInterface],
) -> dict[DcimDevice, list[DcimInterface]]:
    device_interface_map: dict[DcimDevice, list[DcimInterface]] = defaultdict(list)

    for interface in interfaces:
        device_interface_map[cast("DcimDevice", interface.device.peer)].append(interface)

    for device, intfs in sorted(device_interface_map.items(), key=lambda item: _device_sort_key(item[0])):
        interface_map = {interface.name.value: interface for interface in intfs}
        sorted_interface_names = sort_interface_list(list(interface_map.keys()))
        sorted_interface_names.reverse()
        device_interface_map[device] = [interface_map[interface] for interface in sorted_interface_names]

    return dict(sorted(device_interface_map.items(), key=lambda item: _device_sort_key(item[0])))


def _device_sort_key(device: DcimDevice) -> tuple[tuple[tuple[int, int | str], ...], str]:
    """Sort map keys by natural hostname and stable node ID, not response order."""
    name_attribute = getattr(device, "name", None)
    name = getattr(name_attribute, "value", None)
    if not isinstance(name, str) or not name:
        display_label = getattr(device, "display_label", None)
        name = getattr(display_label, "value", display_label)
    hostname = name if isinstance(name, str) else ""
    device_id = getattr(device, "id", None)
    return _natural_sort_key(hostname), str(device_id or "")


def _natural_sort_key(value: str) -> tuple[tuple[int, int | str], ...]:
    return tuple((0, int(part)) if part.isdigit() else (1, part.casefold()) for part in re.split(r"(\d+)", value))
