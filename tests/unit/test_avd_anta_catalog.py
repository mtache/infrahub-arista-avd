"""Unit tests for the AVD ANTA catalog transform."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

from solution_arista_avd.protocols import AvdStructuredConfigFile
from transforms.avd_anta_catalog import AvdAntaCatalogTransform
from transforms.avd_anta_catalog_query import AvdAntaCatalogQuery

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
        "anta_fabrics": {
            "edges": [
                {
                    "node": {
                        "id": FABRIC_ID,
                        "anta_enabled": {"value": anta_enabled},
                        "avd_catalogs_filters": {"value": avd_catalogs_filters},
                    }
                }
            ]
        },
        "target": {"edges": target_edges},
        "DcimDevice": {"edges": [{"node": _device("leaf1", "dev-target", with_sc=target_has_sc)}]},
    }


def _transform(structured_config: dict | None = None) -> AvdAntaCatalogTransform:
    """Build a transform with a mocked client that returns the given structured config."""
    t = AvdAntaCatalogTransform.__new__(AvdAntaCatalogTransform)
    sc_file = AsyncMock()
    sc_file.id = "scf-dev-target"
    sc_file.download_file = AsyncMock(return_value=json.dumps(structured_config or {"hostname": "leaf1"}))
    client = AsyncMock()
    client.filters = AsyncMock(return_value=[sc_file])
    t._init_client = client  # `client` is a read-only property backed by _init_client
    return t


def _catalog_test_names(catalog: str) -> set[str]:
    """Return every ANTA test class name from a rendered YAML catalog."""
    parsed = yaml.safe_load(catalog)
    return {next(iter(test)) if isinstance(test, dict) else test for tests in parsed.values() for test in tests}


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


async def test_enabled_excludes_tests_from_real_pyavd_catalog() -> None:
    excluded_tests = {"VerifyInterfaceDiscards", "VerifyLoggingErrors"}

    unfiltered = await _transform().transform(_data(anta_enabled=True))
    filtered = await _transform().transform(
        _data(
            anta_enabled=True,
            avd_catalogs_filters=sorted(excluded_tests),
        )
    )

    assert excluded_tests <= _catalog_test_names(unfiltered)
    filtered_test_names = _catalog_test_names(filtered)
    assert filtered_test_names
    assert excluded_tests.isdisjoint(filtered_test_names)


def test_query_exposes_fabric_settings_for_proposed_change_impact_tracking() -> None:
    query = (Path(__file__).parents[2] / "transforms" / "avd_anta_catalog.gql").read_text()

    direct_fabric_query = query.split("anta_fabrics: NetworkFabric", maxsplit=1)[1].split(
        "target: DcimDevice", maxsplit=1
    )[0]
    assert "anta_enabled" in direct_fabric_query
    assert "avd_catalogs_filters" in direct_fabric_query


async def test_whole_fabric_configs_use_one_metadata_read_and_preserve_file_identity() -> None:
    transform = _transform()
    data = _data(anta_enabled=True)
    devices = [_device(f"leaf-{index}", f"dev-{index}") for index in range(32)]
    data["DcimDevice"]["edges"] = [
        *({"node": device} for device in devices),
        {"node": _device("other-fabric", "other", fabric_id="unrelated")},
        {"node": _device("no-config", "missing", with_sc=False)},
    ]
    files = [
        SimpleNamespace(
            id=f"scf-dev-{index}",
            download_file=AsyncMock(return_value=json.dumps({"hostname": f"leaf-{index}"})),
        )
        for index in range(32)
    ]
    transform.client.filters.return_value = list(reversed(files))

    configs = await transform._fabric_structured_configs(AvdAntaCatalogQuery.model_validate(data), FABRIC_ID)

    assert configs == {f"leaf-{index}": {"hostname": f"leaf-{index}"} for index in range(32)}
    transform.client.filters.assert_awaited_once_with(
        AvdStructuredConfigFile, ids=sorted(file.id for file in files), prefetch_relationships=False
    )
    transform.client.get.assert_not_awaited()
    for file in files:
        file.download_file.assert_awaited_once()


async def test_catalog_reads_fresh_metadata_and_contents_on_each_execution() -> None:
    transform = _transform()
    files = [
        SimpleNamespace(
            id="scf-dev-target",
            download_file=AsyncMock(return_value=json.dumps({"hostname": "leaf1", "domain_name": domain})),
        )
        for domain in ("before.example", "after.example")
    ]
    transform.client.filters.side_effect = [[file] for file in files]
    data = AvdAntaCatalogQuery.model_validate(_data(anta_enabled=True))

    before = await transform._fabric_structured_configs(data, FABRIC_ID)
    after = await transform._fabric_structured_configs(data, FABRIC_ID)

    assert before["leaf1"]["domain_name"] == "before.example"
    assert after["leaf1"]["domain_name"] == "after.example"
    assert transform.client.filters.await_count == 2
    for file in files:
        file.download_file.assert_awaited_once()


async def test_missing_required_catalog_file_fails_instead_of_omitting_a_sibling() -> None:
    transform = _transform()
    transform.client.filters.return_value = []
    data = AvdAntaCatalogQuery.model_validate(_data(anta_enabled=True))

    with pytest.raises(KeyError, match="scf-dev-target"):
        await transform._fabric_structured_configs(data, FABRIC_ID)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
