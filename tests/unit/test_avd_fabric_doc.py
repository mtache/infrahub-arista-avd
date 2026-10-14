"""Fabric documentation reads complete contents without per-device metadata lookups."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from solution_arista_avd.protocols import AvdHostvarFile, AvdStructuredConfigFile
from transforms.avd_fabric_doc import AvdFabricDocTransform


def _data() -> dict:
    def device(name: str, fabric: str) -> dict:
        return {
            "node": {
                "id": name,
                "name": {"value": name},
                "pod": {
                    "node": {"id": f"pod-{fabric}", "parent": {"node": {"id": fabric, "__typename": "NetworkFabric"}}}
                },
                "avd_artifact": {
                    "node": {
                        "id": f"artifact-{name}",
                        "hostvar_file": {"node": {"id": f"hv-{name}"}},
                        "structured_config_file": {"node": {"id": f"sc-{name}"}},
                    }
                },
            }
        }

    return {
        "NetworkFabric": {"edges": [{"node": {"id": "fabric-1", "name": {"value": "Fabric"}}}]},
        "DcimDevice": {
            "edges": [device("leaf1", "fabric-1"), device("leaf2", "fabric-1"), device("unrelated", "fabric-2")]
        },
    }


async def test_documentation_reads_all_contents_with_bounded_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    transform = AvdFabricDocTransform.__new__(AvdFabricDocTransform)
    client = AsyncMock()
    transform._init_client = client
    active = maximum = 0
    downloads = []

    async def filter_files(_kind: object, *, ids: list[str], **_kwargs: object) -> list:
        def file(identifier: str) -> SimpleNamespace:
            async def download() -> str:
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                try:
                    await asyncio.sleep(0)
                    downloads.append(identifier)
                    return json.dumps({"file": identifier, "full_content": [1, 2, 3]})
                finally:
                    active -= 1

            return SimpleNamespace(id=identifier, download_file=download)

        return [file(identifier) for identifier in ids]

    client.filters.side_effect = filter_files
    facts = Mock(return_value={"facts": True})
    documentation = Mock(return_value=SimpleNamespace(fabric_documentation="complete documentation"))
    monkeypatch.setattr("transforms.avd_fabric_doc.pyavd.get_avd_facts", facts)
    monkeypatch.setattr("transforms.avd_fabric_doc.pyavd.get_fabric_documentation", documentation)

    assert await transform.transform(_data()) == "complete documentation"
    assert maximum == 2
    assert sorted(downloads) == ["hv-leaf1", "hv-leaf2", "sc-leaf1", "sc-leaf2"]
    assert facts.call_args.args == (
        {name: {"file": f"hv-{name}", "full_content": [1, 2, 3]} for name in ("leaf1", "leaf2")},
    )
    assert documentation.call_args.args == (
        {"facts": True},
        {name: {"file": f"sc-{name}", "full_content": [1, 2, 3]} for name in ("leaf1", "leaf2")},
        "Fabric",
    )
    client.get.assert_not_awaited()
    client.filters.assert_any_await(AvdHostvarFile, ids=["hv-leaf1", "hv-leaf2"], prefetch_relationships=False)
    client.filters.assert_any_await(AvdStructuredConfigFile, ids=["sc-leaf1", "sc-leaf2"], prefetch_relationships=False)


async def test_documentation_missing_file_fails_before_any_content_read(monkeypatch: pytest.MonkeyPatch) -> None:
    transform = AvdFabricDocTransform.__new__(AvdFabricDocTransform)
    client = AsyncMock()
    transform._init_client = client
    file = SimpleNamespace(id="hv-leaf1", download_file=AsyncMock())
    client.filters.side_effect = [[file], []]
    facts = Mock()
    monkeypatch.setattr("transforms.avd_fabric_doc.pyavd.get_avd_facts", facts)

    with pytest.raises(ValueError, match="Fabric documentation files disappeared"):
        await transform.transform(_data())

    file.download_file.assert_not_awaited()
    facts.assert_not_called()
