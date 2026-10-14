from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class DeviceDesignParityQuery(BaseModel):
    network_fabric: "DeviceDesignParityQueryNetworkFabric" = Field(
        alias="NetworkFabric"
    )
    network_pod: "DeviceDesignParityQueryNetworkPod" = Field(alias="NetworkPod")
    location_rack: "DeviceDesignParityQueryLocationRack" = Field(alias="LocationRack")
    network_pod_device_design: "DeviceDesignParityQueryNetworkPodDeviceDesign" = Field(
        alias="NetworkPodDeviceDesign"
    )
    network_rack_device_design: "DeviceDesignParityQueryNetworkRackDeviceDesign" = (
        Field(alias="NetworkRackDeviceDesign")
    )
    dcim_device: "DeviceDesignParityQueryDcimDevice" = Field(alias="DcimDevice")


class DeviceDesignParityQueryNetworkFabric(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryNetworkFabricEdges"]


class DeviceDesignParityQueryNetworkFabricEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkFabricEdgesNode"]


class DeviceDesignParityQueryNetworkFabricEdgesNode(BaseModel):
    id: str
    underlay_routing_protocol: Optional[
        "DeviceDesignParityQueryNetworkFabricEdgesNodeUnderlayRoutingProtocol"
    ]


class DeviceDesignParityQueryNetworkFabricEdgesNodeUnderlayRoutingProtocol(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryNetworkPod(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryNetworkPodEdges"]


class DeviceDesignParityQueryNetworkPodEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkPodEdgesNode"]


class DeviceDesignParityQueryNetworkPodEdgesNode(BaseModel):
    id: str
    name: Optional["DeviceDesignParityQueryNetworkPodEdgesNodeName"]
    parent: "DeviceDesignParityQueryNetworkPodEdgesNodeParent"


class DeviceDesignParityQueryNetworkPodEdgesNodeName(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryNetworkPodEdgesNodeParent(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkPodEdgesNodeParentNode"]


class DeviceDesignParityQueryNetworkPodEdgesNodeParentNode(BaseModel):
    typename__: Literal["NetworkBuildingBlock", "NetworkFabric", "NetworkPod"] = Field(
        alias="__typename"
    )
    id: Optional[str]


class DeviceDesignParityQueryLocationRack(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryLocationRackEdges"]


class DeviceDesignParityQueryLocationRackEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryLocationRackEdgesNode"]


class DeviceDesignParityQueryLocationRackEdgesNode(BaseModel):
    id: str
    name: Optional["DeviceDesignParityQueryLocationRackEdgesNodeName"]
    pod: "DeviceDesignParityQueryLocationRackEdgesNodePod"


class DeviceDesignParityQueryLocationRackEdgesNodeName(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryLocationRackEdgesNodePod(BaseModel):
    node: Optional["DeviceDesignParityQueryLocationRackEdgesNodePodNode"]


class DeviceDesignParityQueryLocationRackEdgesNodePodNode(BaseModel):
    id: str


class DeviceDesignParityQueryNetworkPodDeviceDesign(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryNetworkPodDeviceDesignEdges"]


class DeviceDesignParityQueryNetworkPodDeviceDesignEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNode"]


class DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNode(BaseModel):
    role: Optional["DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodeRole"]
    device_quantity: Optional[
        "DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodeDeviceQuantity"
    ]
    pod: "DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodePod"


class DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodeRole(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodeDeviceQuantity(BaseModel):
    value: Optional[Any]


class DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodePod(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodePodNode"]


class DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodePodNode(BaseModel):
    id: str


class DeviceDesignParityQueryNetworkRackDeviceDesign(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryNetworkRackDeviceDesignEdges"]


class DeviceDesignParityQueryNetworkRackDeviceDesignEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNode"]


class DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNode(BaseModel):
    role: Optional["DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRole"]
    device_quantity: Optional[
        "DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeDeviceQuantity"
    ]
    rack: "DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRack"


class DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRole(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeDeviceQuantity(BaseModel):
    value: Optional[Any]


class DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRack(BaseModel):
    node: Optional["DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRackNode"]


class DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRackNode(BaseModel):
    id: str


class DeviceDesignParityQueryDcimDevice(BaseModel):
    count: int
    edges: list["DeviceDesignParityQueryDcimDeviceEdges"]


class DeviceDesignParityQueryDcimDeviceEdges(BaseModel):
    node: Optional["DeviceDesignParityQueryDcimDeviceEdgesNode"]


class DeviceDesignParityQueryDcimDeviceEdgesNode(BaseModel):
    id: str
    role: Optional["DeviceDesignParityQueryDcimDeviceEdgesNodeRole"]
    rack: "DeviceDesignParityQueryDcimDeviceEdgesNodeRack"
    pod: "DeviceDesignParityQueryDcimDeviceEdgesNodePod"


class DeviceDesignParityQueryDcimDeviceEdgesNodeRole(BaseModel):
    value: Optional[str]


class DeviceDesignParityQueryDcimDeviceEdgesNodeRack(BaseModel):
    node: Optional["DeviceDesignParityQueryDcimDeviceEdgesNodeRackNode"]


class DeviceDesignParityQueryDcimDeviceEdgesNodeRackNode(BaseModel):
    id: str


class DeviceDesignParityQueryDcimDeviceEdgesNodePod(BaseModel):
    node: Optional["DeviceDesignParityQueryDcimDeviceEdgesNodePodNode"]


class DeviceDesignParityQueryDcimDeviceEdgesNodePodNode(BaseModel):
    id: str


DeviceDesignParityQuery.model_rebuild()
DeviceDesignParityQueryNetworkFabric.model_rebuild()
DeviceDesignParityQueryNetworkFabricEdges.model_rebuild()
DeviceDesignParityQueryNetworkFabricEdgesNode.model_rebuild()
DeviceDesignParityQueryNetworkPod.model_rebuild()
DeviceDesignParityQueryNetworkPodEdges.model_rebuild()
DeviceDesignParityQueryNetworkPodEdgesNode.model_rebuild()
DeviceDesignParityQueryNetworkPodEdgesNodeParent.model_rebuild()
DeviceDesignParityQueryLocationRack.model_rebuild()
DeviceDesignParityQueryLocationRackEdges.model_rebuild()
DeviceDesignParityQueryLocationRackEdgesNode.model_rebuild()
DeviceDesignParityQueryLocationRackEdgesNodePod.model_rebuild()
DeviceDesignParityQueryNetworkPodDeviceDesign.model_rebuild()
DeviceDesignParityQueryNetworkPodDeviceDesignEdges.model_rebuild()
DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNode.model_rebuild()
DeviceDesignParityQueryNetworkPodDeviceDesignEdgesNodePod.model_rebuild()
DeviceDesignParityQueryNetworkRackDeviceDesign.model_rebuild()
DeviceDesignParityQueryNetworkRackDeviceDesignEdges.model_rebuild()
DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNode.model_rebuild()
DeviceDesignParityQueryNetworkRackDeviceDesignEdgesNodeRack.model_rebuild()
DeviceDesignParityQueryDcimDevice.model_rebuild()
DeviceDesignParityQueryDcimDeviceEdges.model_rebuild()
DeviceDesignParityQueryDcimDeviceEdgesNode.model_rebuild()
DeviceDesignParityQueryDcimDeviceEdgesNodeRack.model_rebuild()
DeviceDesignParityQueryDcimDeviceEdgesNodePod.model_rebuild()
