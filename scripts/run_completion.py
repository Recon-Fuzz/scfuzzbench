#!/usr/bin/env python3
from __future__ import annotations

TERMINAL_RUN_STATUSES = {"completed", "failed"}


def as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def normalize_status(value: object) -> str:
    return str(value or "").strip().lower()


def is_terminal_run_status(value: object) -> bool:
    return normalize_status(value) in TERMINAL_RUN_STATUSES


def is_queue_mode_run(manifest: dict) -> bool:
    return as_bool(manifest.get("queue_mode", False))


def timeout_hours_from_manifest(manifest: dict, default: float = 24.0) -> float:
    try:
        return float(manifest.get("timeout_hours", default))
    except Exception:
        return default


def is_run_complete(
    manifest: dict,
    *,
    run_id: str,
    now_epoch: int,
    grace_seconds: int,
    queue_status: object | None = None,
) -> bool:
    if is_queue_mode_run(manifest):
        return is_terminal_run_status(queue_status)

    try:
        run_start = int(run_id)
    except Exception:
        return False
    timeout_hours = timeout_hours_from_manifest(manifest)
    deadline = run_start + int(timeout_hours * 3600) + int(grace_seconds)
    return now_epoch >= deadline
