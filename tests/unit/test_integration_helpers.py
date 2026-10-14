"""Offline regressions for bounded integration reads and storage pressure."""

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from infrahub_sdk.exceptions import GraphQLError

if TYPE_CHECKING:
    from pathlib import Path

from tests.integration.helpers import read_with_retry, storage_content, wait_until


@pytest.mark.asyncio
async def test_poll_has_a_deadline_even_when_fetch_never_returns() -> None:
    async def blocked() -> None:
        await asyncio.Event().wait()

    with pytest.raises(AssertionError, match="timed out"):
        await wait_until(blocked, bool, timeout=0.02, interval=1, describe="blocked read")


@pytest.mark.asyncio
async def test_poll_propagates_permanent_graphql_errors() -> None:
    fetch = AsyncMock(side_effect=GraphQLError(errors=[{"message": "At most, one relationship expected"}]))
    with pytest.raises(GraphQLError, match="one relationship"):
        await wait_until(fetch, bool, timeout=1, interval=1, describe="cardinality")
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_transient_read_respects_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    response = httpx.Response(429, headers={"Retry-After": "3"}, request=httpx.Request("GET", "http://test/content"))
    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)
    fetch = AsyncMock(side_effect=[httpx.HTTPStatusError("limited", request=response.request, response=response), "ok"])
    assert await read_with_retry(fetch) == "ok"
    sleep.assert_awaited_once_with(3)


@pytest.mark.asyncio
async def test_storage_cache_deduplicates_and_limits_concurrency() -> None:
    active = peak = 0

    async def download(identifier: str) -> str:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return identifier

    client = Mock()
    client.object_store.get = AsyncMock(side_effect=download)
    identifiers = ["same", "same", "b", "c", "d"]
    assert await asyncio.gather(*(storage_content(client, identifier) for identifier in identifiers)) == identifiers
    assert client.object_store.get.await_count == 4
    assert peak == 2
    assert await storage_content(client, "same") == "same"
    assert client.object_store.get.await_count == 4


def test_failed_container_startup_still_preserves_logs_and_tears_down(tmp_path: Path) -> None:
    from tests.integration.stack import IntegrationStack

    compose = Mock()
    compose.start.side_effect = RuntimeError("startup timeout")
    compose.get_logs.return_value = ("startup logs", "stderr")
    fixture = IntegrationStack.infrahub_app.__wrapped__(IntegrationStack(), compose, tmp_path)
    with pytest.raises(RuntimeError, match="startup timeout"):
        next(fixture)
    compose.stop.assert_called_once()
    assert (tmp_path / "instance.log").read_text() == "startup logs\nstderr"


@pytest.mark.asyncio
async def test_dci_targets_only_the_explicit_fabric_in_deterministic_order() -> None:
    from tests.integration.test_e2e_pipeline import _dci_leaf_candidates

    def device(name: str, fabric: str) -> dict:
        return {
            "node": {
                "id": name,
                "name": {"value": name},
                "asn": {"node": {"asn": {"value": 65101}}},
                "pod": {"node": {"parent": {"node": {"id": fabric, "name": {"value": fabric}}}}},
            }
        }

    client = Mock()
    client.execute_graphql = AsyncMock(
        return_value={
            "DcimDevice": {
                "edges": [
                    device("foreign", "Fabric-L3LS-MultiPod-B"),
                    device("leaf-2", "Fabric-L3LS-MultiPod-A"),
                    device("leaf-1", "Fabric-L3LS-MultiPod-A"),
                ]
            }
        }
    )
    candidates = await _dci_leaf_candidates(client, "dci")
    assert [device["device_name"] for device in candidates] == ["leaf-1", "leaf-2"]


@pytest.mark.asyncio
async def test_dci_never_replaces_an_existing_pool() -> None:
    from tests.integration.test_e2e_pipeline import _ensure_dci_pool

    client = Mock()
    client.get = AsyncMock(return_value=Mock(dci_pool=Mock(id="existing")))
    client.create = AsyncMock()
    with pytest.raises(AssertionError, match="existing DCI pool"):
        await _ensure_dci_pool(client, "dci", "fabric")
    client.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_structured_config_readiness_counts_devices_without_an_artifact() -> None:
    from tests.integration.test_e2e_pipeline import _devices_without_structured_config

    client = Mock()
    client.execute_graphql = AsyncMock(
        return_value={
            "DcimDevice": {
                "edges": [
                    {
                        "node": {
                            "name": {"value": "unready"},
                            "role": {"value": "leaf"},
                            "avd_artifact": {"node": None},
                        }
                    }
                ]
            }
        }
    )
    assert await _devices_without_structured_config(client, "baseline") == {"total": 1, "without": ["unready (leaf)"]}


@pytest.mark.asyncio
async def test_poll_does_not_retry_pending_mutations() -> None:
    response = httpx.Response(429, request=httpx.Request("POST", "http://test/generate"))
    action = AsyncMock(side_effect=httpx.HTTPStatusError("limited", request=response.request, response=response))
    fetch = AsyncMock(return_value=False)
    with pytest.raises(httpx.HTTPStatusError):
        await wait_until(fetch, bool, timeout=1, interval=1, describe="generation", on_pending=action)
    action.assert_awaited_once()
    fetch.assert_awaited_once()


def test_stack_makes_only_public_bind_configs_readable(tmp_path: Path) -> None:
    from tests.integration.stack import IntegrationStack

    for name in ("haproxy.cfg", "prometheus.yml", ".env"):
        config = tmp_path / name
        config.write_text("configuration")
        config.chmod(0o600)
    compose = Mock()
    compose.start.side_effect = RuntimeError("startup timeout")
    compose.get_logs.return_value = ("", "")
    fixture = IntegrationStack.infrahub_app.__wrapped__(IntegrationStack(), compose, tmp_path)
    with pytest.raises(RuntimeError, match="startup timeout"):
        next(fixture)
    assert (tmp_path / "haproxy.cfg").stat().st_mode & 0o777 == 0o644
    assert (tmp_path / "prometheus.yml").stat().st_mode & 0o777 == 0o644
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600
