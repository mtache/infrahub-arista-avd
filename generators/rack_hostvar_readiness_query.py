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
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifact.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNode.model_rebuild()
RackHostvarReadinessQueryDcimDeviceEdgesNodeAvdArtifactNodeHostvarFile.model_rebuild()
