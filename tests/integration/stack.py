"""Container fixture with cleanup even when startup fails before pytest yields."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from infrahub_sdk.testing.docker import TestInfrahubDockerClient

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from infrahub_testcontainers.container import InfrahubDockerCompose


class IntegrationStack(TestInfrahubDockerClient):
    @pytest.fixture(scope="class")
    def infrahub_app(self, infrahub_compose: InfrahubDockerCompose, tmp_directory: Path) -> Iterator[dict[str, int]]:
        try:
            infrahub_compose.start()
            yield infrahub_compose.get_services_port()
        finally:
            try:
                stdout, stderr = infrahub_compose.get_logs()
                (tmp_directory / "instance.log").write_text(f"{stdout}\n{stderr}")
            except Exception as exc:  # noqa: BLE001 - preserve the original test/startup error
                (tmp_directory / "log-diagnostic-error.txt").write_text(str(exc))
            finally:
                infrahub_compose.stop()
