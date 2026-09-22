"""Unit tests for the AVD ANTA catalog transform."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

from transforms.avd_anta_catalog import AvdAntaCatalogTransform

FABRIC_ID = "fabric-1"


def _fabric_parent(
    anta_enabled: bool | None,
    name: str = "Fabric-L3LS-MultiPod-A",
    avd_catalogs_filters: object = None,
) -> dict:
    return {
        "node": {
            "__typename": "NetworkFabric",
            "id": FABRIC_ID,
            "name": {"value": name},
            "anta_enabled": {"value": anta_enabled},
            "avd_catalogs_filters": {"value": avd_catalogs_filters},
        }
    }


def _device(hostname: str, dev_id: str, *, with_sc: bool = True, fabric_id: str = FABRIC_ID) -> dict:
    node: dict = {
        "id": dev_id,
        "name": {"value": hostname},
        "pod": {
            "node": {
                "id": f"pod-{dev_id}",
                "parent": {"node": {"__typename": "NetworkFabric", "id": fabric_id}},
            }
        },
        "avd_artifact": {"node": {"id": f"art-{dev_id}", "structured_config_file": {"node": None}}},
    }
    if with_sc:
        node["avd_artifact"] = {
            "node": {"id": f"art-{dev_id}", "structured_config_file": {"node": {"id": f"scf-{dev_id}"}}},
        }
    return node


def _data(
    *,
    anta_enabled: bool | None,
    target_found: bool = True,
    target_has_sc: bool = True,
    avd_catalogs_filters: object = None,
) -> dict:
    target_edges = []
    if target_found:
        target_edges = [
            {
                "node": {
                    "id": "dev-target",
                    "name": {"value": "leaf1"},
                    "pod": {
                        "node": {
                            "id": "pod-t",
                            "parent": _fabric_parent(anta_enabled, avd_catalogs_filters=avd_catalogs_filters),
                        }
                    },
                }
            }
        ]
    return {
        "target": {"edges": target_edges},
        "DcimDevice": {"edges": [{"node": _device("leaf1", "dev-target", with_sc=target_has_sc)}]},
    }


def _transform(structured_config: dict | None = None) -> AvdAntaCatalogTransform:
    """Build a transform with a mocked client that returns the given structured config."""
    t = AvdAntaCatalogTransform.__new__(AvdAntaCatalogTransform)
    sc_file = AsyncMock()
    sc_file.download_file = AsyncMock(return_value=json.dumps(structured_config or {"hostname": "leaf1"}))
    client = AsyncMock()
    client.get = AsyncMock(return_value=sc_file)
    t._init_client = client  # `client` is a read-only property backed by _init_client
    return t


async def test_disabled_fabric_returns_marker() -> None:
    result = await _transform().transform(_data(anta_enabled=False))
    assert result.startswith("# ANTA disabled for fabric Fabric-L3LS-MultiPod-A")


async def test_flag_absent_treated_as_disabled() -> None:
    result = await _transform().transform(_data(anta_enabled=None))
    assert result.startswith("# ANTA disabled")


async def test_device_not_found_returns_marker() -> None:
    result = await _transform().transform(_data(anta_enabled=True, target_found=False))
    assert result.startswith("# ANTA catalog: device not found")


async def test_missing_structured_config_returns_marker() -> None:
    result = await _transform().transform(_data(anta_enabled=True, target_has_sc=False))
    assert result.startswith("# No structured config for leaf1")


async def test_enabled_produces_valid_yaml_catalog() -> None:
    result = await _transform().transform(_data(anta_enabled=True))
    assert not result.startswith("#")
    parsed = yaml.safe_load(result)
    assert isinstance(parsed, dict) and parsed  # non-empty ANTA catalog mapping


async def test_enabled_passes_typed_exclusions_to_catalog_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_settings = None

    def fake_catalog(_hostname: str, _target_sc: object, _fabric_data: object, settings: object) -> object:
        nonlocal captured_settings
        captured_settings = settings
        return SimpleNamespace(dump=lambda: SimpleNamespace(yaml=lambda: "anta.tests.fake: []\n"))

    monkeypatch.setattr("transforms.avd_anta_catalog.get_device_test_catalog", fake_catalog)
    result = await _transform().transform(
        _data(
            anta_enabled=True,
            avd_catalogs_filters=["VerifyInterfaceDiscards", "VerifyLoggingErrors"],
        )
    )

    assert result == "anta.tests.fake: []\n"
    assert captured_settings is not None
    assert captured_settings.skip_tests == ("VerifyInterfaceDiscards", "VerifyLoggingErrors")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
