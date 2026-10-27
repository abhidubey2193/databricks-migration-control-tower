"""Refuse literal credentials before creating durable programme backups."""

import re

SENSITIVE = re.compile(
    r"(?:password|passwd|secret|token|credential|api[_-]?key|access[_-]?key|private[_-]?key)",
    re.I,
)


def reject_literal_credentials(value, path="settings"):
    if isinstance(value, dict):
        named_secret = SENSITIVE.search(str(value.get("name", "")))
        for key, child in value.items():
            sensitive = SENSITIVE.search(str(key)) or (
                named_secret and key in ("default", "value")
            )
            if sensitive and child not in (None, "", [], {}):
                if (
                    isinstance(child, str)
                    and child.startswith("{{secrets/")
                    and child.endswith("}}")
                ):
                    continue
                raise ValueError(
                    f"Literal credential-like setting at {path}.{key}; replace it with a secret reference before storing a backup"
                )
            reject_literal_credentials(child, path + "." + str(key))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            reject_literal_credentials(child, f"{path}[{i}]")
