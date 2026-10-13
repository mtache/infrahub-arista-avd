"""Shared helpers for the end-to-end pipeline integration test.

These utilities back ``test_e2e_pipeline.py``: a bounded-wait poller with
diagnostic failure messages, the generator/artifact name constants sourced from
``.infrahub.yml``, and small helpers for deriving expected counts from the loaded
seed data (rather than hardcoding brittle literals).
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from ipaddress import ip_interface, ip_network
from typing import TYPE_CHECKING, Any

import httpx
from infrahub_sdk.exceptions import GraphQLError

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
READ_TIMEOUT = 300
MAX_READ_RETRIES = 3
RETRYABLE_READ_STATUS_CODES = frozenset({429, 502, 503, 504})


def _summarize(value: Any) -> str:
    """Render an observed value compactly for a diagnostic failure message."""
    if isinstance(value, (list, tuple, set)):
        if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
            details = "; ".join(value)
            if len(details) > 1500:
                details = f"{details[:1497]}..."
            return f"{type(value).__name__} of length {len(value)}: {details}"
        return f"{type(value).__name__} of length {len(value)}"
    if isinstance(value, dict):
        compact = repr(value)
        if len(compact) <= 500:
            return compact
        return f"dict with keys {sorted(value)[:10]}"
    text = repr(value)
    return text if len(text) <= 200 else f"{text[:200]}..."


def _retry_after_seconds(error: httpx.HTTPStatusError) -> float | None:
    value = error.response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except TypeError, ValueError, OverflowError:
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max((retry_at - datetime.now(UTC)).total_seconds(), 0.0)


async def _read_with_retries[T](fetch: Callable[[], Awaitable[T]], *, deadline: float) -> T:
    """Bound one poll read and retry only transient HTTP statuses."""
    for attempt in range(MAX_READ_RETRIES + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("poll deadline expired before the next read")
        try:
            return await asyncio.wait_for(fetch(), timeout=min(READ_TIMEOUT, remaining))
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in RETRYABLE_READ_STATUS_CODES or attempt == MAX_READ_RETRIES:
                raise
            delay = _retry_after_seconds(exc)
            if delay is None:
                delay = 2**attempt
            remaining = deadline - time.monotonic()
            if delay >= remaining:
                raise TimeoutError(
                    f"retry delay {delay:.3f}s would exceed the remaining poll deadline {remaining:.3f}s"
                ) from exc
            await asyncio.sleep(delay)
    raise AssertionError("unreachable retry state")


async def wait_until[T](
    fetch: Callable[[], Awaitable[T]],
    ready: Callable[[T], bool],
    *,
    timeout: int,  # noqa: ASYNC109 (deliberate polling budget, not an asyncio.timeout scope)
    interval: int,
    describe: str,
) -> T:
    """Poll ``fetch`` until ``ready`` is satisfied or ``timeout`` elapses.

    Returns the observed value once ``ready(value)`` is truthy. On timeout raises
    ``AssertionError`` including the last observed value so the failing stage is
    diagnosable without a rerun (FR-015 / FR-019).
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    while True:
        try:
            last = await _read_with_retries(fetch, deadline=deadline)
        except GraphQLError as exc:
            last = exc
        except TimeoutError as exc:
            last = exc
            if time.monotonic() < deadline:
                raise AssertionError(f"{describe}: read timed out before the poll deadline: {exc}") from exc
        else:
            if ready(last):
                return last
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            msg = f"{describe}: timed out after {timeout}s; last observed: {_summarize(last)}"
            raise AssertionError(msg)
        await asyncio.sleep(min(interval, remaining))


async def wait_until_stable[T](
    fetch: Callable[[], Awaitable[T]],
    ready: Callable[[T], bool],
    *,
    timeout: int,  # noqa: ASYNC109 (deliberate polling budget, not an asyncio.timeout scope)
    interval: int,
    describe: str,
) -> T:
    """Wait for two identical ready observations, with one shared deadline."""
    deadline = time.monotonic() + timeout
    last: Any = None
    previous: T | None = None
    while True:
        try:
            last = await _read_with_retries(fetch, deadline=deadline)
        except GraphQLError as exc:
            last = exc
            previous = None
        except TimeoutError as exc:
            last = exc
            if time.monotonic() < deadline:
                raise AssertionError(f"{describe}: read timed out before the poll deadline: {exc}") from exc
            previous = None
        else:
            if ready(last):
                if previous is not None and last == previous:
                    return last
                previous = last
            else:
                previous = None

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"{describe}: timed out after {timeout}s without two identical ready observations; "
                f"last observed: {_summarize(last)}"
            )
        await asyncio.sleep(min(interval, remaining))


def is_unrouted_l2_mlag_domain(domain: dict, devices_by_id: dict[str, dict], fabrics: dict[str, dict]) -> bool:
    """Identify only two-peer l2spine domains on fabrics with no routed underlay."""
    peer_devices = [devices_by_id.get(peer_id) for peer_id in domain.get("peer_ids", [])]
    if len(peer_devices) != 2 or any(device is None or device["role"] != "l2spine" for device in peer_devices):
        return False
    fabric_ids = {device["fabric_id"] for device in peer_devices}
    if len(fabric_ids) != 1 or None in fabric_ids:
        return False
    fabric = fabrics.get(next(iter(fabric_ids)))
    if fabric is None:
        return False
    return fabric.get("underlay_routing_protocol") in {None, "none"}


def dci_link_matches_endpoints(link: dict, endpoints: list[tuple[str, str]]) -> bool:
    """Match a DCI hostvar link by both devices and their paired interfaces."""
    nodes = link.get("nodes")
    interfaces = link.get("interfaces")
    if not isinstance(nodes, list) or not isinstance(interfaces, list) or len(nodes) != len(interfaces):
        return False
    return len(nodes) == len(endpoints) and set(zip(nodes, interfaces, strict=True)) == set(endpoints)


def dci_allocation_mismatches(ip_addresses: list[str], pool_prefix: str, existing_allocations: list[str]) -> list[str]:
    """Validate the two endpoint IPs allocated to a DCI /31 against its pool."""
    mismatches: list[str] = []
    try:
        pool = ip_network(pool_prefix, strict=False)
        interfaces = [ip_interface(value) for value in ip_addresses]
        existing_networks = [ip_network(value, strict=False) for value in existing_allocations]
    except ValueError as exc:
        return [f"invalid DCI pool or allocation: {exc}"]

    if len(interfaces) != 2:
        return [f"expected two DCI endpoint addresses, observed {len(interfaces)}: {ip_addresses}"]
    networks = {interface.network for interface in interfaces}
    if len(networks) != 1:
        mismatches.append(f"endpoint addresses use different networks: {sorted(map(str, networks))}")
        return mismatches

    allocation = next(iter(networks))
    if allocation.prefixlen != 31:
        mismatches.append(f"expected a /31 allocation, observed {allocation}")
    if len({interface.ip for interface in interfaces}) != 2:
        mismatches.append(f"endpoint addresses are not distinct: {ip_addresses}")
    if any(interface.ip not in pool for interface in interfaces) or not allocation.subnet_of(pool):
        mismatches.append(f"allocation {allocation} is outside DCI pool {pool}")
    overlaps = sorted(str(network) for network in existing_networks if allocation.overlaps(network))
    if overlaps:
        mismatches.append(f"allocation {allocation} overlaps seed DCI allocations: {overlaps}")
    return mismatches


def task_failure_diagnostic(task: Any) -> str | None:
    """Describe failed task state with its target and a bounded error message."""
    state_value = getattr(task, "state", "")
    state = str(getattr(state_value, "value", state_value)).upper()
    if state not in {"FAILED", "CRASHED", "CANCELLED"}:
        return None

    targets = sorted(
        f"{getattr(node, 'kind', 'unknown')}:{getattr(node, 'id', 'unknown')}"
        for node in getattr(task, "related_nodes", [])
    )
    messages = []
    for log in getattr(task, "logs", []):
        severity_value = getattr(log, "severity", "")
        severity = str(getattr(severity_value, "value", severity_value)).upper()
        if severity in {"ERROR", "CRITICAL"}:
            messages.append(str(getattr(log, "message", "")))
    error = " | ".join(messages) or "no error log attached"
    error = re.sub(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,]+", r"\1[REDACTED]", error)
    error = re.sub(r"(?i)(token|password|secret)(\s*[:=]\s*)[^\s,]+", r"\1\2[REDACTED]", error)
    if len(error) > 500:
        error = f"{error[:497]}..."

    branch = getattr(task, "branch", None) or "unknown"
    return (
        f"{state} task {getattr(task, 'id', 'unknown')} "
        f"title={getattr(task, 'title', 'unknown')!r} branch={branch} "
        f"targets={targets or ['unknown']} error={error}"
    )


async def device_design_mismatches(client: InfrahubClient, branch: str) -> list[str]:
    """Compare each pod's and rack's generated devices against its device designs.

    Device designs are the sole source of sizing, so for each container and each
    design role the number of devices produced must equal ``device_quantity``.
    The design ``role`` names a tier, which the generators map onto a device role
    using the fabric's underlay (non-L3LS example fabrics use l2spine / l3spine /
    p / pe / l2leaf), so the same mapping is applied here.

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

    # Underlay per pod (from its fabric) and per rack (from its pod).
    underlay_by_pod: dict[str, str | None] = {}
    pods = await client.all(kind="NetworkPod", branch=branch)
    for pod in pods:
        fabric = await client.get(kind="NetworkFabric", id=pod.parent.id, branch=branch)
        protocol = getattr(fabric, "underlay_routing_protocol", None)
        underlay_by_pod[pod.id] = protocol.value if protocol else None
    racks = await client.all(kind="LocationRack", branch=branch)
    underlay_by_rack = {rack.id: underlay_by_pod.get(rack.pod.id) if rack.pod else None for rack in racks}

    # Devices bucketed by their rack, else by their pod (spines and super-spines).
    per_rack: dict[str, dict[str, int]] = {}
    per_pod: dict[str, dict[str, int]] = {}
    for device in await client.all(kind="DcimDevice", branch=branch, prefetch_relationships=False):
        bucket = per_rack.setdefault(device.rack.id, {}) if device.rack.id else None
        if bucket is None and device.pod.id:
            bucket = per_pod.setdefault(device.pod.id, {})
        if bucket is not None:
            bucket[device.role.value] = bucket.get(device.role.value, 0) + 1

    mismatches: list[str] = []

    async def check(design_kind: str, parent_attr: str, container_kind: str) -> None:
        for design in await client.all(kind=design_kind, branch=branch):
            container_id = getattr(design, parent_attr).id
            role = design.role.value
            want = int(design.device_quantity.value)
            if parent_attr == "rack":
                actual, underlay = per_rack.get(container_id, {}), underlay_by_rack.get(container_id)
            else:
                actual, underlay = per_pod.get(container_id, {}), underlay_by_pod.get(container_id)
            mapped = device_role(role, underlay)
            observed_roles = ("leaf", "border_leaf") if mapped == "leaf" and underlay == "ebgp" else (mapped,)
            observed_counts = {device_role: actual.get(device_role, 0) for device_role in observed_roles}
            got = sum(observed_counts.values())
            if got != want:
                container = await client.get(kind=container_kind, id=container_id, branch=branch)
                mismatches.append(
                    f"{container_kind} {container.name.value} design_role={role} "
                    f"device_roles={observed_counts}: expected={want} observed={got}"
                )

    await check("NetworkPodDeviceDesign", "pod", "NetworkPod")
    await check("NetworkRackDeviceDesign", "rack", "LocationRack")
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
