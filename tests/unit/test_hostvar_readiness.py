"""Readiness must cover every device before triggering structured configuration."""

from unittest.mock import AsyncMock, Mock

import pytest
from infrahub_sdk.exceptions import GraphQLError

from generators import generate_avd_device_hostvar as hostvars
from generators.hostvar_readiness import fabric_devices_with_hostvars


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
async def test_permanent_read_error_is_not_hidden_as_pending() -> None:
    client = Mock(execute_graphql=AsyncMock(side_effect=GraphQLError(errors=[{"message": "cardinality"}])))
    with pytest.raises(GraphQLError, match="cardinality"):
        await hostvars.check_fabric_hostvars_ready(client, "fabric")
