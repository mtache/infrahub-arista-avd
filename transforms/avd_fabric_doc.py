"""AVD Fabric Documentation Transform.

Generates fabric documentation from stored structured configs.
"""

import asyncio
import copy
import json
from typing import Any

import pyavd
from infrahub_sdk.transforms import InfrahubTransform

from solution_arista_avd.protocols import AvdHostvarFile, AvdStructuredConfigFile

from .avd_fabric_devices_query import AvdFabricDevicesQuery


class AvdFabricDocTransform(InfrahubTransform):
    """Generates fabric documentation from stored structured configs."""

    query = "avd_fabric_devices"

    async def transform(self, data: dict[str, Any]) -> str:
        """Transform fabric data to documentation."""
        data: AvdFabricDevicesQuery = AvdFabricDevicesQuery(**data)

        # Get fabric info
        fabric_edges = data.network_fabric.edges
        if not fabric_edges:
            return "# No fabric found"

        fabric_node = fabric_edges[0].node
        fabric_name = fabric_node.name.value
        fabric_id = fabric_node.id

        # Get all devices and filter by fabric
        device_edges = data.dcim_device.edges
        all_hostvars: dict[str, dict[str, Any]] = {}
        structured_configs: dict[str, dict[str, Any]] = {}

        selected_files: list[tuple[str, str | None, str | None]] = []
        for edge in device_edges:
            device = edge.node
            # Check if device belongs to this fabric
            if not device.pod or not device.pod.node:
                continue
            pod_node = device.pod.node
            if not pod_node.parent or not pod_node.parent.node:
                continue
            if pod_node.parent.node.id != fabric_id:
                continue

            hostname = device.name.value

            # Fetch AVD data from CoreFileObject children
            if not device.avd_artifact or not device.avd_artifact.node:
                continue

            artifact_node = device.avd_artifact.node

            hostvar_file_node = artifact_node.hostvar_file.node if artifact_node.hostvar_file else None
            sc_file_node = artifact_node.structured_config_file.node if artifact_node.structured_config_file else None

            selected_files.append(
                (
                    hostname,
                    hostvar_file_node.id if hostvar_file_node else None,
                    sc_file_node.id if sc_file_node else None,
                )
            )

        hostvar_ids = sorted({hv_id for _, hv_id, _ in selected_files if hv_id})
        config_ids = sorted({sc_id for _, _, sc_id in selected_files if sc_id})
        hostvar_files = (
            await self.client.filters(AvdHostvarFile, ids=hostvar_ids, prefetch_relationships=False)
            if hostvar_ids
            else []
        )
        config_files = (
            await self.client.filters(AvdStructuredConfigFile, ids=config_ids, prefetch_relationships=False)
            if config_ids
            else []
        )
        files = {file.id: file for file in [*hostvar_files, *config_files]}
        missing = set(hostvar_ids + config_ids) - files.keys()
        if missing:
            raise ValueError(f"Fabric documentation files disappeared: {sorted(missing)}")

        # Read every content, with two downloads at a time across both kinds.
        # Keep this cache local: file node IDs can point at different content on
        # a later generation or branch.
        semaphore = asyncio.Semaphore(2)

        async def download(file_id: str) -> tuple[str, dict[str, Any]]:
            async with semaphore:
                content = await files[file_id].download_file()
            return file_id, json.loads(content)

        contents = dict(await asyncio.gather(*(download(file_id) for file_id in files)))
        for hostname, hostvar_id, config_id in selected_files:
            if hostvar_id:
                all_hostvars[hostname] = contents[hostvar_id]
            if config_id:
                structured_configs[hostname] = contents[config_id]

        if not all_hostvars or not structured_configs:
            return f"# {fabric_name}\n\nNo AVD data available for this fabric."

        # Generate AVD facts
        hostvars_copy = copy.deepcopy(all_hostvars)
        avd_facts = pyavd.get_avd_facts(hostvars_copy)

        # Generate fabric documentation
        fabric_doc = pyavd.get_fabric_documentation(
            avd_facts,
            structured_configs,
            fabric_name,
        )

        return fabric_doc.fabric_documentation
