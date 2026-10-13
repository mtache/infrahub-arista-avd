"""Resolve the ordered seed directories without contacting Infrahub."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class SeedSelection:
    """Common followed by an explicit, deterministic selection of fabrics."""

    fabrics: tuple[str, ...]
    directories: tuple[Path, ...]


def select_seed_data(objects_directory: Path, fabrics: str | None = None) -> SeedSelection:
    """Validate a selection before any bootstrap action can write remote data.

    Fabric identifiers are directory names. Empty input preserves the original
    all-examples default; ``none`` selects only Common. Resolve each directory
    independently so the SDK's path sorting cannot reorder Common after a fabric.
    """
    common = objects_directory / "common"
    if not common.is_dir() or not _has_seed_files(common):
        raise ValueError(f"Missing Common seed data: {common}")

    available = {
        path.name: path
        for path in objects_directory.iterdir()
        if path.is_dir() and path.name != "common" and not path.name.startswith(".") and _has_seed_files(path)
    }
    choices = ", ".join(("all", "none", *sorted(available)))
    selection = (fabrics or "all").strip() or "all"
    requested = {identifier.strip() for identifier in selection.split(",")}
    if not requested <= (available.keys() | {"all", "none"}) or (requested & {"all", "none"} and len(requested) > 1):
        raise ValueError(f"Invalid seed fabric selection {fabrics!r}. Available values: {choices}")
    if requested == {"all"}:
        selected = tuple(sorted(available))
    elif requested == {"none"}:
        selected = ()
    else:
        selected = tuple(sorted(requested))

    return SeedSelection(fabrics=selected, directories=(common, *(available[name] for name in selected)))


def _has_seed_files(directory: Path) -> bool:
    return any(path.is_file() and path.suffix in {".yml", ".yaml"} for path in directory.iterdir())
