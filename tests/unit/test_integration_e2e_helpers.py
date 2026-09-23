from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.integration.test_e2e_pipeline import (
    _dci_leaf_candidates,
    _ensure_dci_pool,
    _mark_racks_generation_incomplete,
    _run_generator_for_nodes,
)


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


@pytest.mark.asyncio
async def test_rack_reruns_mark_every_rack_incomplete_before_generation() -> None:
    client = SimpleNamespace(execute_graphql=AsyncMock())

    await _mark_racks_generation_incomplete(client, "test-branch", ["rack-1", "rack-2"])

    assert [call.kwargs["variables"] for call in client.execute_graphql.await_args_list] == [
        {"id": "rack-1"},
        {"id": "rack-2"},
    ]
    assert all(call.kwargs["branch_name"] == "test-branch" for call in client.execute_graphql.await_args_list)


@pytest.mark.asyncio
async def test_generator_nodes_can_run_sequentially() -> None:
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(id="generator-definition")),
        execute_graphql=AsyncMock(),
    )

    await _run_generator_for_nodes(
        client,
        "test-branch",
        "generate-rack",
        ["rack-1", "rack-2"],
        sequential=True,
    )

    client.get.assert_awaited_once_with(
        kind="CoreGeneratorDefinition",
        name__value="generate-rack",
        branch="test-branch",
    )
    assert [call.kwargs["variables"] for call in client.execute_graphql.await_args_list] == [
        {"id": "generator-definition", "nodes": ["rack-1"]},
        {"id": "generator-definition", "nodes": ["rack-2"]},
    ]
