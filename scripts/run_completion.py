#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

RUN_MANIFEST_RE = re.compile(r"^runs/([0-9]+)/([0-9a-f]{32})/manifest\.json$")
TERMINAL_STATES = {"succeeded", "failed", "timed_out", "completed"}


def aws_env(profile: str | None) -> dict[str, str]:
    env = os.environ.copy()
    if profile:
        env["AWS_PROFILE"] = profile
    return env


def aws_text(args: list[str], *, profile: str | None) -> str:
    return subprocess.check_output(["aws", *args], text=True, env=aws_env(profile))


def aws_json(args: list[str], *, profile: str | None) -> dict[str, Any]:
    out = aws_text([*args, "--output", "json"], profile=profile)
    return json.loads(out) if out.strip() else {}


def list_keys(bucket: str, prefix: str, *, profile: str | None) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        cmd = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix]
        if token:
            cmd += ["--continuation-token", token]
        data = aws_json(cmd, profile=profile)
        keys.extend([obj["Key"] for obj in data.get("Contents", [])])
        if not data.get("IsTruncated"):
            break
        token = data.get("NextContinuationToken")
        if not token:
            break
    return keys


def s3_json_or_none(bucket: str, key: str, *, profile: str | None) -> dict[str, Any] | None:
    try:
        raw = aws_text(["s3", "cp", f"s3://{bucket}/{key}", "-"], profile=profile)
    except subprocess.CalledProcessError:
        return None
    try:
        value = json.loads(raw)
    except Exception:
        return None
    if not isinstance(value, dict):
        return None
    return value


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


@dataclass(frozen=True)
class Completion:
    run_id: str
    benchmark_uuid: str
    complete: bool
    reason: str
    timeout_hours: float | None
    queue_mode: bool
    status_state: str
    status_terminal: bool

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "run_id": self.run_id,
            "benchmark_uuid": self.benchmark_uuid,
            "complete": self.complete,
            "reason": self.reason,
            "queue_mode": self.queue_mode,
            "status_state": self.status_state,
            "status_terminal": self.status_terminal,
        }
        if self.timeout_hours is not None:
            out["timeout_hours"] = self.timeout_hours
        return out


def check_run_completion(
    *,
    bucket: str,
    run_id: str,
    benchmark_uuid: str,
    grace_seconds: int,
    profile: str | None,
    now: int | None = None,
) -> Completion:
    if now is None:
        now = int(time.time())

    status_key = f"runs/{run_id}/{benchmark_uuid}/status/run.json"
    manifest_key = f"runs/{run_id}/{benchmark_uuid}/manifest.json"
    legacy_manifest_key = f"logs/{run_id}/{benchmark_uuid}/manifest.json"

    status = s3_json_or_none(bucket, status_key, profile=profile)
    manifest = s3_json_or_none(bucket, manifest_key, profile=profile)
    if manifest is None:
        manifest = s3_json_or_none(bucket, legacy_manifest_key, profile=profile)

    timeout_hours: float | None = None
    if manifest is not None:
        timeout_hours = safe_float(manifest.get("timeout_hours", 24), 24.0)

    status_state = ""
    status_terminal = False
    queue_mode = False
    if status is not None:
        status_state = str(status.get("state", "")).strip().lower()
        status_terminal = bool(status.get("terminal", False)) or status_state in TERMINAL_STATES
        mode = str(status.get("mode", "")).strip().lower()
        queue_mode = mode == "s3_queue" or bool(status.get("queue_mode", False))

    if status_terminal:
        return Completion(
            run_id=run_id,
            benchmark_uuid=benchmark_uuid,
            complete=True,
            reason="status_terminal",
            timeout_hours=timeout_hours,
            queue_mode=queue_mode,
            status_state=status_state,
            status_terminal=True,
        )

    # Legacy fallback used when run status is absent or non-terminal.
    try:
        run_start = int(run_id)
    except ValueError:
        return Completion(
            run_id=run_id,
            benchmark_uuid=benchmark_uuid,
            complete=False,
            reason="invalid_run_id",
            timeout_hours=timeout_hours,
            queue_mode=queue_mode,
            status_state=status_state,
            status_terminal=status_terminal,
        )

    if timeout_hours is None:
        return Completion(
            run_id=run_id,
            benchmark_uuid=benchmark_uuid,
            complete=False,
            reason="manifest_missing",
            timeout_hours=None,
            queue_mode=queue_mode,
            status_state=status_state,
            status_terminal=status_terminal,
        )

    deadline = run_start + int(timeout_hours * 3600) + int(grace_seconds)
    complete = now >= deadline
    return Completion(
        run_id=run_id,
        benchmark_uuid=benchmark_uuid,
        complete=complete,
        reason="legacy_deadline_met" if complete else "legacy_deadline_pending",
        timeout_hours=timeout_hours,
        queue_mode=queue_mode,
        status_state=status_state,
        status_terminal=status_terminal,
    )


def discover_complete_runs(
    *,
    bucket: str,
    grace_seconds: int,
    profile: str | None,
    now: int | None = None,
) -> list[Completion]:
    keys = list_keys(bucket, "runs/", profile=profile)
    candidates: list[tuple[str, str]] = []
    for key in keys:
        m = RUN_MANIFEST_RE.match(key)
        if not m:
            continue
        candidates.append((m.group(1), m.group(2)))

    seen: set[tuple[str, str]] = set()
    result: list[Completion] = []
    for run_id, benchmark_uuid in sorted(candidates, reverse=True):
        pair = (run_id, benchmark_uuid)
        if pair in seen:
            continue
        seen.add(pair)
        completion = check_run_completion(
            bucket=bucket,
            run_id=run_id,
            benchmark_uuid=benchmark_uuid,
            grace_seconds=grace_seconds,
            profile=profile,
            now=now,
        )
        if completion.complete:
            result.append(completion)

    result.sort(key=lambda r: (int(r.run_id), r.benchmark_uuid), reverse=True)
    return result


def cmd_check(args: argparse.Namespace) -> int:
    completion = check_run_completion(
        bucket=args.bucket,
        run_id=args.run_id,
        benchmark_uuid=args.benchmark_uuid,
        grace_seconds=args.grace_seconds,
        profile=args.profile,
    )
    if args.output == "json":
        print(json.dumps(completion.as_dict(), separators=(",", ":")))
    else:
        print("complete" if completion.complete else "incomplete")
    if args.exit_nonzero_if_incomplete and not completion.complete:
        return 1
    return 0


def cmd_discover(args: argparse.Namespace) -> int:
    completions = discover_complete_runs(
        bucket=args.bucket,
        grace_seconds=args.grace_seconds,
        profile=args.profile,
    )
    include: list[dict[str, Any]] = []
    for completion in completions:
        entry: dict[str, Any] = {
            "run_id": completion.run_id,
            "benchmark_uuid": completion.benchmark_uuid,
        }
        if completion.timeout_hours is not None:
            entry["timeout_hours"] = completion.timeout_hours
        include.append(entry)
    print(json.dumps({"include": include}, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Queue-aware run completion checks.")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="Check if a run is complete")
    check.add_argument("--bucket", required=True)
    check.add_argument("--run-id", required=True)
    check.add_argument("--benchmark-uuid", required=True)
    check.add_argument("--grace-seconds", type=int, default=3600)
    check.add_argument("--profile", default=None)
    check.add_argument("--output", choices=["json", "plain"], default="json")
    check.add_argument("--exit-nonzero-if-incomplete", action="store_true")
    check.set_defaults(func=cmd_check)

    discover = sub.add_parser("discover", help="Discover complete runs")
    discover.add_argument("--bucket", required=True)
    discover.add_argument("--grace-seconds", type=int, default=3600)
    discover.add_argument("--profile", default=None)
    discover.set_defaults(func=cmd_discover)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
