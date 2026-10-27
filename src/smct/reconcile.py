"""Deterministic Bundle writeback with compare-and-swap file replacement."""

import copy
import hashlib
import os
import re
import tempfile
from pathlib import Path

import yaml

from .orchestration import TRIGGERS, digest


def bundle_document(settings, resource_key, *, notebook_paths=None):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", resource_key):
        raise ValueError("Plain Bundle resource key required")
    definition = copy.deepcopy(settings)
    if any(definition.get(k) for k in TRIGGERS):
        raise ValueError(
            "Bundle proposal must have no trigger; activation is a separate reviewed change"
        )
    mappings = notebook_paths or {}
    for task in definition.get("tasks", []):
        nt = task.get("notebook_task")
        if nt and nt["notebook_path"] in mappings:
            nt["notebook_path"] = mappings[nt["notebook_path"]]
    document = {"resources": {"jobs": {resource_key: definition}}}
    content = yaml.safe_dump(document, sort_keys=False)
    return dict(
        content=content,
        definition_digest=digest(settings),
        content_sha256=hashlib.sha256(content.encode()).hexdigest(),
    )


def write_bundle(root, relative_path, proposal, *, expected_sha256=None):
    root = Path(root).resolve()
    raw = Path(relative_path)
    if raw.is_absolute() or raw.suffix not in (".yml", ".yaml") or ".." in raw.parts:
        raise ValueError(
            "Relative YAML resource path without parent traversal required"
        )
    target = (root / raw).resolve()
    if not target.is_relative_to(root) or raw.parts[0] != "resources":
        raise ValueError("Resource must stay inside repository resources/")
    if target.exists():
        current = hashlib.sha256(target.read_bytes()).hexdigest()
        if current == proposal["content_sha256"]:
            return str(target)
        if expected_sha256 != current:
            raise ValueError("Bundle file drift; review before replacement")
    elif expected_sha256 is not None:
        raise ValueError("Expected Bundle file disappeared")
    target.parent.mkdir(parents=True, exist_ok=True)
    # Resolve after mkdir too, so pre-existing symlinks cannot escape the repository.
    if not target.parent.resolve().is_relative_to(root):
        raise ValueError("Resource parent escapes repository")
    fd, temporary = tempfile.mkstemp(prefix=".smct-", dir=target.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(proposal["content"])
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return str(target)


def verify_bundle(path, *, resource_key, expected_definition):
    document = yaml.safe_load(Path(path).read_text())
    actual = document["resources"]["jobs"][resource_key]
    if digest(actual) != digest(expected_definition):
        raise ValueError("Bundle definition differs from validated promotion plan")
    return dict(
        path=str(path),
        resource_key=resource_key,
        definition_digest=digest(actual),
        content_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest(),
    )
