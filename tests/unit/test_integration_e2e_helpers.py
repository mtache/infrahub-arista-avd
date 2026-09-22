from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.integration.test_e2e_pipeline import _dci_leaf_candidates, _ensure_dci_pool


def _dci_candidate_edge(*, fabric_id: str, fabric_name: str, device_id: str, device_name: str) -> dict:
    return {
        "node": {
            "id": device_id,
            "name": {"value": device_name},
            "asn": {"node": {"asn": {"value": 65001}}},
            "pod": {
                "node": {
                    "parent": {
                        "node": {
                            "id": fabric_id,
                            "name": {"value": fabric_name},
                        }
                    }
                }
            },
        }
    }


@pytest.mark.asyncio
async def test_dci_leaf_candidates_choose_first_complete_fabric_deterministically() -> None:
    client = SimpleNamespace(
        execute_graphql=AsyncMock(
            return_value={
                "DcimDevice": {
                    "edges": [
                        _dci_candidate_edge(
                            fabric_id="fabric-b",
                            fabric_name="Fabric-B",
                            device_id="leaf-b1",
                            device_name="leaf-b-1",
                        ),
                        _dci_candidate_edge(
                            fabric_id="fabric-a",
                            fabric_name="Fabric-A",
                            device_id="leaf-a2",
                            device_name="leaf-a-2",
                        ),
                        _dci_candidate_edge(
                            fabric_id="fabric-a",
                            fabric_name="Fabric-A",
                            device_id="leaf-a1",
                            device_name="leaf-a-1",
                        ),
                    ]
                }
            }
        )
    )

    candidates = await _dci_leaf_candidates(client, "test-branch")

    assert [candidate["device_name"] for candidate in candidates] == ["leaf-a-1", "leaf-a-2"]


@pytest.mark.asyncio
async def test_ensure_dci_pool_reuses_existing_relationship() -> None:
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(dci_pool=SimpleNamespace(id="existing-dci-pool"))),
        create=AsyncMock(),
    )

    await _ensure_dci_pool(client, "test-branch", "fabric-a")

    client.get.assert_awaited_once_with(
        kind="NetworkFabric",
        id="fabric-a",
        branch="test-branch",
        include=["dci_pool"],
    )
    client.create.assert_not_awaited()
