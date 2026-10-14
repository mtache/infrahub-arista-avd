"""Readiness must cover every device before triggering structured configuration."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from infrahub_sdk.exceptions import GraphQLError

from generators import generate_avd_device_hostvar as hostvars
from generators.hostvar_readiness import fabric_devices_with_hostvars, missing_hostvar_generator_instances


def _device(name: str, *, ready: bool, rack_only: bool = False, fabric: str = "fabric") -> dict:
    pod = {"node": {"parent": {"node": {"__typename": "NetworkFabric", "id": fabric}}}}
    return {
        "node": {
            "id": name,
            "name": {"value": name},
            "pod": {"node": None} if rack_only else pod,
            "rack": {"node": {"pod": pod}} if rack_only else {"node": None},
            "avd_artifact": {"node": {"hostvar_file": {"node": {"id": f"{name}-file"} if ready else None}}},
        }
    }


@pytest.mark.parametrize("rack_ready", [False, True])
@pytest.mark.asyncio
async def test_readiness_checks_rack_devices_and_all_pages_before_setting_flag(
    monkeypatch: pytest.MonkeyPatch, rack_ready: bool
) -> None:
    client = Mock()
    client.execute_graphql = AsyncMock(
        side_effect=[
            {"DcimDevice": {"count": 3, "edges": [_device("spine", ready=True)]}},
            {
                "DcimDevice": {
                    "count": 3,
                    "edges": [
                        _device("leaf", ready=rack_ready, rack_only=True),
                        _device("foreign", ready=False, fabric="other-fabric"),
                    ],
                }
            },
        ]
    )
    flag = AsyncMock()
    monkeypatch.setattr(hostvars, "set_fabric_avd_hostvars_ready", flag)
    assert await hostvars.check_fabric_hostvars_ready(client, "fabric") is rack_ready
    if rack_ready:
        flag.assert_awaited_once_with(client, "fabric", True)
    else:
        flag.assert_not_awaited()
    assert client.execute_graphql.await_args_list[1].kwargs["variables"]["offset"] == 1
    client.filters.assert_not_called()
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_missing_artifact_blocks_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    device = _device("leaf", ready=True)
    device["node"]["avd_artifact"]["node"] = None
    client = Mock(execute_graphql=AsyncMock(return_value={"DcimDevice": {"count": 1, "edges": [device]}}))
    flag = AsyncMock()
    monkeypatch.setattr(hostvars, "set_fabric_avd_hostvars_ready", flag)
    assert await hostvars.check_fabric_hostvars_ready(client, "fabric") is False
    flag.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_page_before_count_does_not_mark_fabric_ready() -> None:
    client = Mock(execute_graphql=AsyncMock(return_value={"DcimDevice": {"count": 1, "edges": []}}))
    with pytest.raises(ValueError, match="empty page"):
        await fabric_devices_with_hostvars(client, "fabric")


@pytest.mark.asyncio
async def test_branch_local_hostvar_instances_are_paginated_and_missing_targets_remain_missing() -> None:
    def page(target: str) -> dict:
        return {
            "CoreGeneratorInstance": {
                "count": 2,
                "edges": [{"node": {"object": {"node": {"__typename": "DcimDevice", "id": target}}}}],
            }
        }

    client = Mock(execute_graphql=AsyncMock(side_effect=[page("spine"), page("leaf")]))
    assert await missing_hostvar_generator_instances(client, ["spine", "leaf", "missing"]) == {"missing"}
    assert client.execute_graphql.await_args_list[1].kwargs["variables"]["offset"] == 1


@pytest.mark.asyncio
async def test_missing_hostvar_instance_target_is_a_permanent_failure() -> None:
    client = Mock(
        execute_graphql=AsyncMock(
            return_value={"CoreGeneratorInstance": {"count": 1, "edges": [{"node": {"object": {"node": None}}}]}}
        )
    )
    with pytest.raises(ValueError, match="no target device"):
        await missing_hostvar_generator_instances(client, ["leaf"])


@pytest.mark.asyncio
async def test_permanent_read_error_is_not_hidden_as_pending() -> None:
    client = Mock(execute_graphql=AsyncMock(side_effect=GraphQLError(errors=[{"message": "cardinality"}])))
    with pytest.raises(GraphQLError, match="cardinality"):
        await hostvars.check_fabric_hostvars_ready(client, "fabric")


@pytest.mark.parametrize("stored", ["same", "changed", "missing"])
@pytest.mark.parametrize("metadata", [False, True])
@pytest.mark.asyncio
async def test_hostvar_file_marks_pending_only_when_it_will_change(
    monkeypatch: pytest.MonkeyPatch, stored: str, metadata: bool
) -> None:
    content = {"hostname": "leaf-1"}
    events = []

    async def pending(client: object, fabric: str, ready: bool) -> None:
        assert fabric == "fabric" and ready is False
        events.append("pending")

    async def saved(**kwargs: object) -> None:
        assert kwargs == {"allow_upsert": True, "update_group_context": False}
        events.append("saved")

    file = Mock(save=AsyncMock(side_effect=saved))
    file.download_file = AsyncMock(
        return_value=json.dumps(content if stored == "same" else {"hostname": "old"}, indent=2).encode()
    )
    if metadata:
        file.checksum = SimpleNamespace(value=hostvars.file_content_checksum(file.download_file.return_value))
    artifact = SimpleNamespace(
        save=AsyncMock(),
        hostvar_file=SimpleNamespace(id="file" if stored != "missing" else None, peer=file, fetch=AsyncMock()),
    )
    generator = hostvars.GenerateAVDDeviceHostvar.__new__(hostvars.GenerateAVDDeviceHostvar)
    generator.logger = Mock()
    generator.client = Mock(create=AsyncMock(side_effect=[artifact, file]), get=AsyncMock(return_value=artifact))
    flag = AsyncMock(side_effect=pending)
    ready = AsyncMock(return_value=True)
    monkeypatch.setattr(hostvars, "set_fabric_avd_hostvars_ready", flag)
    monkeypatch.setattr(hostvars, "check_fabric_hostvars_ready", ready)
    await generator._store_hostvars(fabric_id="fabric", device_id="device", hostname="leaf-1", hostvars=content)
    ready.assert_awaited_once_with(generator.client, "fabric")
    artifact.hostvar_file.fetch.assert_not_awaited()
    if metadata:
        file.download_file.assert_not_awaited()
    if stored == "same":
        assert events == []
        flag.assert_not_awaited()
        file.upload_from_bytes.assert_not_called()
        file.save.assert_not_awaited()
    else:
        assert events == ["pending", "saved"]
        file.upload_from_bytes.assert_called_once_with(
            content=json.dumps(content, indent=2).encode(), name="leaf-1-hostvars.json"
        )


@pytest.mark.asyncio
async def test_failed_hostvar_write_does_not_mark_the_fabric_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    file = Mock(save=AsyncMock(side_effect=RuntimeError("storage failed")))
    artifact = SimpleNamespace(save=AsyncMock(), hostvar_file=SimpleNamespace(id=None))
    generator = hostvars.GenerateAVDDeviceHostvar.__new__(hostvars.GenerateAVDDeviceHostvar)
    generator.logger = Mock()
    generator.client = Mock(create=AsyncMock(side_effect=[artifact, file]), get=AsyncMock(return_value=artifact))
    flag = AsyncMock()
    ready = AsyncMock()
    monkeypatch.setattr(hostvars, "set_fabric_avd_hostvars_ready", flag)
    monkeypatch.setattr(hostvars, "check_fabric_hostvars_ready", ready)
    with pytest.raises(RuntimeError, match="storage failed"):
        await generator._store_hostvars(fabric_id="fabric", device_id="device", hostname="leaf-1", hostvars={})
    flag.assert_awaited_once_with(generator.client, "fabric", False)
    ready.assert_not_awaited()
