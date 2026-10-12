from pathlib import Path
from unittest.mock import MagicMock

import pytest
from invoke import Context, Result
from invoke.exceptions import Exit, UnexpectedExit

import tasks
from solution_arista_avd.seed_data import select_seed_data


@pytest.fixture
def objects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in ("common", "campus", "l2ls"):
        directory = tmp_path / "objects" / name
        directory.mkdir(parents=True)
        (directory / "00_seed.yml").write_text("---\n", encoding="utf-8")
    monkeypatch.setattr(tasks, "MAIN_DIRECTORY_PATH", tmp_path)
    monkeypatch.delenv("INFRAHUB_SEED_FABRICS", raising=False)
    return tmp_path / "objects"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ("campus", "l2ls")),
        ("", ("campus", "l2ls")),
        ("   ", ("campus", "l2ls")),
        ("all", ("campus", "l2ls")),
        ("all, all", ("campus", "l2ls")),
        ("none", ()),
        ("none, none", ()),
        ("l2ls", ("l2ls",)),
        ("l2ls,campus", ("campus", "l2ls")),
        (" campus , l2ls ,campus ", ("campus", "l2ls")),
    ],
)
def test_selection(objects: Path, value: str | None, expected: tuple[str, ...]) -> None:
    result = select_seed_data(objects, value)
    assert result.fabrics == expected
    assert result.directories == tuple(objects / name for name in ("common", *expected))


@pytest.mark.parametrize(
    "value", ["missing", "Campus", "l2ls,", ",l2ls", "l2ls,,campus", "all,l2ls", "none,l2ls", "all,none"]
)
def test_invalid_selection_lists_available_values(objects: Path, value: str) -> None:
    with pytest.raises(ValueError, match="Available values: all, none, campus, l2ls"):
        select_seed_data(objects, value)


def test_all_discovers_new_yaml_directories(objects: Path) -> None:
    for name in ("new-fabric", "empty", ".hidden"):
        (objects / name).mkdir()
    (objects / "new-fabric" / "00_seed.yaml").touch()
    (objects / ".hidden" / "00_seed.yml").touch()
    (objects / "not-a-directory.yml").touch()
    assert select_seed_data(objects).fabrics == ("campus", "l2ls", "new-fabric")


@pytest.mark.parametrize("missing_directory", [False, True])
def test_common_is_required(objects: Path, missing_directory: bool) -> None:
    (objects / "common" / "00_seed.yml").unlink()
    if missing_directory:
        (objects / "common").rmdir()
    with pytest.raises(ValueError, match="Missing Common seed data"):
        select_seed_data(objects)


@pytest.mark.parametrize(("option", "expected"), [(None, ("l2ls",)), ("campus", ("campus",)), ("", ("campus", "l2ls"))])
def test_option_overrides_environment(
    objects: Path, monkeypatch: pytest.MonkeyPatch, option: str | None, expected: tuple[str, ...]
) -> None:
    monkeypatch.setenv("INFRAHUB_SEED_FABRICS", "l2ls")
    assert tasks._seed_selection(option).fabrics == expected


def test_exported_selection_overrides_dotenv(objects: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = objects.parent / ".env"
    env_file.write_text("INFRAHUB_SEED_FABRICS=campus\n", encoding="utf-8")
    monkeypatch.setenv("INFRAHUB_SEED_FABRICS", "l2ls")
    tasks._load_environment(env_file)
    assert tasks._seed_selection().fabrics == ("l2ls",)


def test_loader_order_branch_and_output(objects: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ctx = MagicMock(spec=Context)
    tasks.load_objects.body(ctx, fabrics="l2ls,campus", branch="seed review")
    assert [call.args[0] for call in ctx.run.call_args_list] == [
        f"infrahubctl object load {objects / name} --branch 'seed review'" for name in ("common", "campus", "l2ls")
    ]
    assert all(call.kwargs == {"warn": False} for call in ctx.run.call_args_list)
    output = capsys.readouterr().out
    assert "Seed fabrics: campus, l2ls" in output
    assert "Loading seed data: common" in output


def test_load_reuses_seed_loader(objects: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = MagicMock(spec=Context)
    for name in ("init_semaphore", "load_schema", "load_menu", "sleep", "wait_for_repository_sync"):
        monkeypatch.setattr(tasks, name, MagicMock())
    loader = MagicMock()
    monkeypatch.setattr(tasks, "_load_seed_selection", loader)
    monkeypatch.setenv("INFRAHUB_SEED_FABRICS", "l2ls")
    tasks.load.body(ctx)
    loader.assert_called_once_with(ctx, select_seed_data(objects, "l2ls"))


@pytest.mark.parametrize("task", [tasks.load, tasks.load_objects])
def test_invalid_selection_prevents_all_bootstrap_actions(
    objects: Path, monkeypatch: pytest.MonkeyPatch, task: object
) -> None:
    ctx = MagicMock(spec=Context)
    actions = [MagicMock() for _ in range(3)]
    for name, action in zip(("init_semaphore", "load_schema", "load_menu"), actions, strict=True):
        monkeypatch.setattr(tasks, name, action)
    monkeypatch.setenv("INFRAHUB_SEED_FABRICS", "bad")
    with pytest.raises(Exit, match="Available values"):
        task.body(ctx)
    ctx.run.assert_not_called()
    ctx.cd.assert_not_called()
    for action in actions:
        action.assert_not_called()


def test_loader_stops_after_first_failed_directory(objects: Path) -> None:
    ctx = MagicMock(spec=Context)
    ctx.run.side_effect = [Result(exited=0), UnexpectedExit(Result(exited=1))]
    with pytest.raises(UnexpectedExit):
        tasks.load_objects.body(ctx)
    assert len(ctx.run.call_args_list) == 2
    assert "campus" in ctx.run.call_args_list[-1].args[0]
