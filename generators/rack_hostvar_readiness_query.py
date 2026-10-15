from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class RackHostvarReadinessQuery(BaseModel):
    dcim_device: "RackHostvarReadinessQueryDcimDevice" = Field(alias="DcimDevice")


class RackHostvarReadinessQueryDcimDevice(BaseModel):
    count: int
    edges: list["RackHostvarReadinessQueryDcimDeviceEdges"]


class RackHostvarReadinessQueryDcimDeviceEdges(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNode(BaseModel):
    id: str
    name: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodeName"]
    pod: "RackHostvarReadinessQueryDcimDeviceEdgesNodePod"
    rack: "RackHostvarReadinessQueryDcimDeviceEdgesNodeRack"
    avd_artifact: "RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifact"


class RackHostvarReadinessQueryDcimDeviceEdgesNodeName(BaseModel):
    value: Optional[str]


class RackHostvarReadinessQueryDcimDeviceEdgesNodePod(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodePodNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNodePodNode(BaseModel):
    parent: "RackHostvarReadinessQueryDcimDeviceEdgesNodePodNodeParent"


class RackHostvarReadinessQueryDcimDeviceEdgesNodePodNodeParent(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodePodNodeParentNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNodePodNodeParentNode(BaseModel):
    typename__: Literal["NetworkBuildingBlock", "NetworkFabric", "NetworkPod"] = Field(
        alias="__typename"
    )
    id: Optional[str]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRack(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNode(BaseModel):
    pod: "RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePod"


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePod(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNode(BaseModel):
    parent: "RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNodeParent"


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNodeParent(BaseModel):
    node: Optional[
        "RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNodeParentNode"
    ]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNodeParentNode(BaseModel):
    typename__: Literal["NetworkBuildingBlock", "NetworkFabric", "NetworkPod"] = Field(
        alias="__typename"
    )
    id: Optional[str]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifact(BaseModel):
    node: Optional["RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNode"]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNode(BaseModel):
    hostvar_file: (
        "RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFile"
    )


class RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFile(BaseModel):
    node: Optional[
        "RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFileNode"
    ]


class RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFileNode(
    BaseModel
):
    id: str


RackHostvarReadinessQuery.model_rebuild()
RackHostvarReadinessQueryDcimDevice.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdges.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodePod.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodePodNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodePodNodeParent.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeRack.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePod.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeRackNodePodNodeParent.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifact.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFile.model_rebuild()
