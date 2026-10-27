"""Refresh controller dependencies after Git pulls in long-lived notebook sessions."""

import importlib


def refresh():
    # Reload dependencies before modules that bind their functions with `from ...`.
    # Never call this while a controller action is in progress.
    for name in (
        "validation",
        "assessment",
        "ab",
        "discovery",
        "orchestration",
        "remediation",
        "secrets",
        "program",
        "program_runtime",
        "lease",
        "delta_journal",
        "fleet",
        "reconcile",
        "promotion",
    ):
        importlib.reload(importlib.import_module("smct." + name))
