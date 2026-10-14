"""Shared helpers for the end-to-end pipeline integration test.

These utilities back ``test_e2e_pipeline.py``: a bounded-wait poller with
diagnostic failure messages, the generator/artifact name constants sourced from
``.infrahub.yml``, and small helpers for deriving expected counts from the loaded
seed data (rather than hardcoding brittle literals).
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode
from weakref import WeakKeyDictionary

import httpx
from infrahub_sdk.exceptions import GraphQLError, ServerNotReachableError, ServerNotResponsiveError

from .device_design_parity_query import DeviceDesignParityQuery

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from infrahub_sdk import InfrahubClient

# --- Generator definition names (from .infrahub.yml `generator_definitions`) ---
GENERATOR_FABRIC = "generate-fabric"
GENERATOR_POD = "generate-pod"
GENERATOR_RACK = "generate-rack"
GENERATOR_SERVER_CABLING = "generate-server-cabling"
GENERATOR_AVD_STRUCTURED_CONFIG = "generate-avd-device-structured-config"
GENERATOR_BACKFILL = "backfill-structured-config"

# Ordered topology chain. `infrahubctl generator <name>` with no variables
# iterates every member of the generator's target group, so one call per
# generator processes the whole fabric.
TOPOLOGY_GENERATOR_CHAIN = [
    GENERATOR_FABRIC,
    GENERATOR_POD,
    GENERATOR_RACK,
    GENERATOR_SERVER_CABLING,
]

# --- Artifact instance names (from .infrahub.yml `artifact_definitions` -> `artifact_name`) ---
ARTIFACT_CABLING_PLAN = "Cabling Plan"
ARTIFACT_AVD_EOS_CONFIG = "AVD EOS Configuration"
ARTIFACT_AVD_FABRIC_DOC = "AVD Fabric Documentation"
ARTIFACT_AVD_DEVICE_DOC = "AVD Device Documentation"
ARTIFACT_AVD_ANTA_CATALOG = "AVD ANTA Catalog"
ARTIFACT_CONTAINERLAB_TOPOLOGY = "ContainerLab Topology"

ALL_ARTIFACT_NAMES = [
    ARTIFACT_CABLING_PLAN,
    ARTIFACT_AVD_EOS_CONFIG,
    ARTIFACT_AVD_FABRIC_DOC,
    ARTIFACT_AVD_DEVICE_DOC,
    ARTIFACT_AVD_ANTA_CATALOG,
    ARTIFACT_CONTAINERLAB_TOPOLOGY,
]

# Arista device types + object templates seeded for issue #70, with the interface
# count each object template must expand to (Ethernet ports + 1 Loopback0).
ARISTA_DEVICE_TYPES = ("Arista 7050SX3-48YC8C", "Arista 7050CX3-32C")
ARISTA_TEMPLATE_INTERFACE_COUNTS = {
    "arista-7050cx3-32c-super-spine-switch": 33,
    "arista-7050cx3-32c-spine-switch": 33,  # 32x 100G QSFP + Loopback0
    "arista-7050cx3-32c-multipod-spine-switch": 33,
    "arista-7050sx3-48yc8c-leaf-switch": 57,  # 48x 25G + 8x 100G QSFP + Loopback0
    "arista-7050sx3-48yc8c-l2leaf-switch": 57,
}

# Marker the ANTA transform emits when the fabric has ANTA disabled
# (transforms/avd_anta_catalog.py). Used to assert the catalog is *populated*.
ANTA_DISABLED_MARKER = "# ANTA disabled"

# Default bounded-wait budgets (seconds). Kept generous for CI runners.
SCHEMA_TIMEOUT = 120
OBJECT_LOAD_TIMEOUT = 300
GROUP_TIMEOUT = 60
REPO_SYNC_INTERVAL = 10
REPO_SYNC_RETRIES = 60  # 10s * 60 = 600s
GENERATOR_TIMEOUT = 600
ARTIFACT_TIMEOUT = 600
POLL_INTERVAL = 10


def _summarize(value: Any) -> str:
    """Render an observed value compactly for a diagnostic failure message."""
    if isinstance(value, (list, tuple, set)):
        return repr(value)[:2000]
    if isinstance(value, dict):
        compact = repr(value)
        if len(compact) <= 500:
            return compact
        return f"dict with keys {sorted(value)[:10]}"
    text = repr(value)
    return text if len(text) <= 200 else f"{text[:200]}..."


async def wait_until[T](
    fetch: Callable[[], Awaitable[T]],
    ready: Callable[[T], bool],
    *,
    timeout: int,  # noqa: ASYNC109 (deliberate polling budget, not an asyncio.timeout scope)
    interval: int,
    describe: str,
    on_pending: Callable[[T], Awaitable[None]] | None = None,
) -> T:
    """Poll ``fetch`` until ``ready`` is satisfied or ``timeout`` elapses.

    Returns the observed value once ``ready(value)`` is truthy. On timeout raises
    ``AssertionError`` including the last observed value so the failing stage is
    diagnosable without a rerun (FR-015 / FR-019).
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    try:
        async with asyncio.timeout(timeout):
            while True:
                last = await read_with_retry(fetch)
                if ready(last):
                    return last
                if on_pending is not None:
                    await on_pending(last)
                await asyncio.sleep(min(interval, max(0, deadline - time.monotonic())))
    except TimeoutError as exc:
        msg = f"{describe}: timed out after {timeout}s; last observed: {_summarize(last)}"
        raise AssertionError(msg) from exc


READ_TIMEOUT = 60


async def read_with_retry[T](fetch: Callable[[], Awaitable[T]]) -> T:
    """Bound a read to 60 seconds, retrying only transient transport/HTTP errors.

    This helper must never wrap mutations: a lost response cannot establish
    whether a write was applied. Permanent GraphQL errors propagate immediately.
    """
    read_deadline = asyncio.timeout(READ_TIMEOUT)
    try:
        async with read_deadline:
            return await _retry_read(fetch)
    except TimeoutError as exc:
        if not read_deadline.expired():
            raise
        raise AssertionError(f"integration read timed out after {READ_TIMEOUT}s") from exc


async def _retry_read[T](fetch: Callable[[], Awaitable[T]]) -> T:
    for attempt in range(4):
        delay = 4 * 2**attempt
        try:
            return await fetch()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in {429, 502, 503, 504} or attempt == 3:
                raise
            retry_after = exc.response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    delay = max(delay, (parsedate_to_datetime(retry_after) - datetime.now(UTC)).total_seconds())
        except httpx.TransportError, ServerNotReachableError, ServerNotResponsiveError:
            if attempt == 3:
                raise
        except GraphQLError as exc:
            # Load shedding can be an HTTP 200 GraphQL error envelope.
            # A mixed response containing a permanent error must still fail.
            codes = [(error.get("extensions") or {}).get("code") for error in exc.errors]
            if not codes or any(str(code) not in {"429", "502", "503", "504"} for code in codes) or attempt == 3:
                raise
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


_storage_caches: WeakKeyDictionary = WeakKeyDictionary()


async def storage_content(client: InfrahubClient, storage_id: str, *, file_branch: str | None = None) -> str:
    """Read immutable storage content once per instance, with two concurrent reads."""
    if client not in _storage_caches:
        _storage_caches[client] = ({}, asyncio.Semaphore(2))
    cache, semaphore = _storage_caches[client]

    async def download() -> str:
        async with semaphore:
            if file_branch is None:
                return await read_with_retry(lambda: client.object_store.get(identifier=storage_id))

            async def file_content() -> str:
                response = await client._get(
                    f"{client.address}/api/storage/files/by-storage-id/{storage_id}?{urlencode({'branch': file_branch})}"
                )
                response.raise_for_status()
                return response.text

            return await read_with_retry(file_content)

    if storage_id not in cache:
        cache[storage_id] = asyncio.create_task(download())
    try:
        return await cache[storage_id]
    except BaseException:
        cache.pop(storage_id, None)
        raise


async def create_branch(client: InfrahubClient, name: str, origin: str) -> str:
    """Create a branch from the known stable default-branch state."""
    response = await client.execute_graphql(
        query="""mutation($name: String!, $origin: String!) {
          BranchCreate(data: {name: $name, origin_branch: $origin, sync_with_git: false}) { ok }
        }""",
        variables={"name": name, "origin": origin},
        branch_name=origin,
    )
    assert response["BranchCreate"]["ok"], response
    return name


async def device_design_mismatches(client: InfrahubClient, branch: str) -> list[str]:
    """Compare each pod's and rack's generated devices against its device designs.

    Device designs are the sole source of sizing, so for each container and each
    design role the number of devices produced must equal ``device_quantity``.
    The design ``role`` names a tier, which the generators map onto a device role
    using the fabric's underlay (non-L3LS example fabrics use l2spine / l3spine /
    p / pe / l2leaf), so the same mapping is applied here. The routed leaf tier
    includes ``border_leaf``: seed intent specializes that role without changing
    the rack design's sizing (both roles map to AVD ``l3leaf``).

    The fabric tier is covered separately by ``expected_super_spine_count``.

    Returns a list of human-readable mismatch descriptions; empty means parity.
    """
    from solution_arista_avd.avd import LEAF_ROLE_BY_UNDERLAY, SPINE_ROLE_BY_UNDERLAY

    def device_role(design_role: str, underlay: str | None) -> str:
        if underlay is None:
            return design_role
        if design_role == "spine":
            return SPINE_ROLE_BY_UNDERLAY.get(underlay, "spine")
        if design_role == "leaf":
            return LEAF_ROLE_BY_UNDERLAY.get(underlay, "leaf")
        return design_role

    query = Path(__file__).with_name("device_design_parity.gql").read_text()
    entities: dict[str, list] = defaultdict(list)
    offset = 0
    while True:
        response = await client.execute_graphql(
            query=query, variables={"offset": offset, "limit": 200}, branch_name=branch
        )
        page = DeviceDesignParityQuery.model_validate(response)
        collections = {key: getattr(page, key) for key in type(page).model_fields}
        for key, collection in collections.items():
            entities[key].extend(edge.node for edge in collection.edges if edge.node is not None)
        offset += 200
        if offset >= max(collection.count for collection in collections.values()):
            break
        assert any(collection.edges for collection in collections.values()), "Empty parity page before reported count"

    underlay_by_fabric = {
        fabric.id: fabric.underlay_routing_protocol.value if fabric.underlay_routing_protocol else None
        for fabric in entities["network_fabric"]
    }
    underlay_by_pod = {
        pod.id: underlay_by_fabric.get(pod.parent.node.id) if pod.parent.node else None
        for pod in entities["network_pod"]
    }
    underlay_by_rack = {
        rack.id: underlay_by_pod.get(rack.pod.node.id) if rack.pod.node else None for rack in entities["location_rack"]
    }
    per_rack: dict[str, Counter] = defaultdict(Counter)
    per_pod: dict[str, Counter] = defaultdict(Counter)
    for device in entities["dcim_device"]:
        if device.rack.node:
            per_rack[device.rack.node.id][device.role.value] += 1
        elif device.pod.node:
            per_pod[device.pod.node.id][device.role.value] += 1

    mismatches = []
    for parent_attr, container_kind, designs, containers, actual_counts, underlays in (
        ("pod", "NetworkPod", entities["network_pod_device_design"], entities["network_pod"], per_pod, underlay_by_pod),
        (
            "rack",
            "LocationRack",
            entities["network_rack_device_design"],
            entities["location_rack"],
            per_rack,
            underlay_by_rack,
        ),
    ):
        names = {container.id: container.name.value for container in containers}
        for design in designs:
            parent = getattr(design, parent_attr).node
            assert parent is not None, f"{container_kind} design has no container"
            role = design.role.value
            want = int(design.device_quantity.value)
            mapped = device_role(role, underlays.get(parent.id))
            actual = actual_counts[parent.id]
            roles = ("leaf", "border_leaf") if role == "leaf" and mapped == "leaf" else (mapped,)
            got = sum(actual.get(device_role, 0) for device_role in roles)
            if got != want:
                mismatches.append(
                    f"{container_kind} {names[parent.id]} design_role={role} -> "
                    f"device_roles={roles}: design={want} actual={got}; roles={dict(actual)}"
                )
    return mismatches


async def expected_super_spine_count(client: InfrahubClient, branch: str) -> int:
    """Sum the ``super_spine`` design quantities across all fabrics.

    Derives the expected count from the fabrics' ``device_designs`` rather than
    hardcoding it; a fabric with no ``super_spine`` design contributes nothing.
    """
    designs = await client.all(kind="NetworkFabricDeviceDesign", branch=branch)
    total = 0
    for design in designs:
        if design.role.value != "super_spine":
            continue
        if design.device_quantity.value is not None:
            total += int(design.device_quantity.value)
    return total


# Device roles each example fabric design must generate (used by the
# fabric-scoped deployment validation selected via ``--fabric``).
FABRIC_EXPECTED_ROLES: dict[str, set[str]] = {
    "Fabric-A": {"super_spine", "spine", "leaf"},
    "Fabric-C": {"spine", "leaf"},
    "Fabric-L2LS": {"l2spine", "l2leaf"},
    "Fabric-Campus": {"l3spine", "l2leaf"},
    "Fabric-ISIS-LDP": {"p", "pe"},
}

# Roles that form MLAG pairs (and so must carry an ``mlag_domain``) in the
# standalone-L2LS design — MLAG on both the spine and leaf tier.
L2LS_MLAG_ROLES = {"l2spine", "l2leaf"}


async def fabric_deployment_report(client: InfrahubClient, branch: str, fabric_name: str) -> dict[str, Any] | None:
    """Report a single fabric's generated devices, grouped by role, with MLAG membership.

    Returns ``None`` when the named fabric does not exist (so callers can fail fast
    on an unknown ``--fabric``). Otherwise returns ``{roles, device_count,
    mlag_roles}`` where ``roles`` maps each role to its device names and
    ``mlag_roles`` is the set of roles whose devices carry an ``mlag_domain``.
    """
    fabrics = await client.filters(kind="NetworkFabric", name__value=fabric_name, branch=branch)
    if not fabrics:
        return None

    pods = await client.filters(kind="NetworkPod", parent__ids=[fabrics[0].id], branch=branch)
    pod_ids = [pod.id for pod in pods]

    devices: list[Any] = []
    if pod_ids:
        devices = await client.filters(kind="DcimDevice", pod__ids=pod_ids, branch=branch, include=["mlag_domain"])

    roles: dict[str, list[str]] = {}
    mlag_roles: set[str] = set()
    for device in devices:
        role = device.role.value
        roles.setdefault(role, []).append(device.name.value)
        mlag_domain = getattr(device, "mlag_domain", None)
        if mlag_domain is not None and getattr(mlag_domain, "node", None):
            mlag_roles.add(role)

    return {"roles": roles, "device_count": len(devices), "mlag_roles": mlag_roles}
