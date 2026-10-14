"""Grouped, paginated reads for fabric hostvar readiness."""

from pathlib import Path
from typing import TYPE_CHECKING

from .rack_hostvar_readiness_query import RackHostvarReadinessQuery, RackHostvarReadinessQueryDcimDeviceEdgesNode

if TYPE_CHECKING:
    from infrahub_sdk import InfrahubClient


async def fabric_devices_with_hostvars(
    client: InfrahubClient, fabric_id: str
) -> list[RackHostvarReadinessQueryDcimDeviceEdgesNode]:
    """Read pod- and rack-owned devices without per-device relationship fetches."""
    query = Path(__file__).with_name("rack_hostvar_readiness.gql").read_text()
    devices = []
    offset = 0
    while True:
        response = await client.execute_graphql(query=query, variables={"offset": offset, "limit": 200})
        page = RackHostvarReadinessQuery.model_validate(response).dcim_device
        for edge in page.edges:
            device = edge.node
            if device is None:
                continue
            pods = [device.pod.node, device.rack.node.pod.node if device.rack.node else None]
            if any(
                pod is not None
                and pod.parent.node is not None
                and pod.parent.node.typename__ == "NetworkFabric"
                and pod.parent.node.id == fabric_id
                for pod in pods
            ):
                devices.append(device)
        offset += len(page.edges)
        if offset >= page.count:
            return devices
        if not page.edges:
            raise ValueError("Device readiness query returned an empty page before its reported count")
