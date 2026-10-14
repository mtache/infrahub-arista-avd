import shutil
import subprocess  # noqa: S404 - export only committed repository source
import tarfile
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from infrahub_sdk.testing.docker import TestInfrahubDockerClient
from infrahub_sdk.yaml import SchemaFile

CURRENT_DIRECTORY = Path(__file__).parent.resolve()


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the ``--fabric`` selector for fabric-scoped integration validation.

    Example: ``pytest tests/integration -m e2e --fabric Fabric-L2LS`` scopes the
    fabric-deployment validation to ``Fabric-L2LS``. With no ``--fabric`` the
    selector is ``None`` and the fabric-scoped test skips, so the default suite
    behaviour is unchanged.
    """
    parser.addoption(
        "--fabric",
        action="store",
        default=None,
        help="Restrict fabric-scoped integration validation to this fabric (e.g. Fabric-L2LS).",
    )


@pytest.fixture
def target_fabric(request: pytest.FixtureRequest) -> str | None:
    """The fabric name passed via ``--fabric`` (or ``None`` when unset)."""
    return request.config.getoption("--fabric")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip the optional fabric scenario before its expensive fixtures run."""
    if not config.getoption("--fabric"):
        for item in items:
            if item.name == "test_selected_fabric_deployment":
                item.add_marker(pytest.mark.skip(reason="no --fabric selector; pass --fabric <name>"))


@pytest.fixture(scope="class")
def root_directory() -> Path:
    """
    Return the path of the root directory of the repository.
    """
    return CURRENT_DIRECTORY.parent.parent


@pytest.fixture(scope="class")
def schemas_directory(root_directory: Path) -> Path:
    return root_directory / "schemas"


@pytest.fixture(scope="class")
def schemas(schemas_directory: Path) -> list[dict[str, Any]]:
    schema_files = SchemaFile.load_from_disk(paths=[schemas_directory])
    return [item.content for item in schema_files if item.content]


@pytest.fixture(scope="class")
def repository_source(root_directory: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Export the exact committed source; never copy venv, caches or user files."""
    destination = tmp_path_factory.mktemp("repository-source")
    archive = destination / "source.tar"
    with archive.open("wb") as output:
        subprocess.run(  # noqa: S603 - fixed trusted git archive
            [shutil.which("git") or "/usr/bin/git", "archive", "HEAD"], cwd=root_directory, stdout=output, check=True
        )
    with tarfile.open(archive) as source:
        source.extractall(destination, filter="data")
    archive.unlink()
    return destination


@pytest_asyncio.fixture(scope="class", loop_scope="class", autouse=True)
async def integration_evidence(request: pytest.FixtureRequest):
    """Persist temporary-instance logs and task IDs before container teardown."""
    if request.cls is None or not issubclass(request.cls, TestInfrahubDockerClient):
        yield
        return
    request.getfixturevalue("infrahub_app")
    directory = request.getfixturevalue("tmp_directory")
    client = request.getfixturevalue("client")
    yield
    try:
        from .helpers import read_with_retry

        tasks = await read_with_retry(client.task.all)
        (directory / "tasks.json").write_text("\n".join(task.model_dump_json() for task in tasks))
        failed = [task for task in tasks if task.state in {"FAILED", "CRASHED", "CANCELLED"}]
        logs = [await client.task.get(task.id, include_logs=True) for task in failed]
        (directory / "failed-tasks.json").write_text("\n".join(task.model_dump_json() for task in logs))
    except Exception as exc:  # noqa: BLE001 - diagnostic errors must not mask pytest outcomes
        (directory / "task-diagnostic-error.txt").write_text(str(exc))
