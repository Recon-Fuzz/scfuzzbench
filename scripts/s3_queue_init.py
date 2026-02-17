#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from typing import Any


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


def s3_exists(bucket: str, key: str, *, profile: str | None) -> bool:
    try:
        subprocess.check_call(
            ["aws", "s3api", "head-object", "--bucket", bucket, "--key", key],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=aws_env(profile),
        )
        return True
    except subprocess.CalledProcessError:
        return False


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


def s3_put_json(bucket: str, key: str, payload: dict[str, Any], *, profile: str | None) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tf:
        tf.write(json.dumps(payload, separators=(",", ":")))
        tf.write("\n")
        temp_path = tf.name
    try:
        subprocess.check_call(
            [
                "aws",
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--content-type",
                "application/json",
                "--body",
                temp_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=aws_env(profile),
        )
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_fragment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def parse_shards(encoded: str) -> list[dict[str, Any]]:
    raw = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
    value = json.loads(raw)
    if not isinstance(value, list):
        raise ValueError("decoded shard payload is not a list")

    shards: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("shard entry must be an object")
        shard_key = str(item.get("shard_key", "")).strip()
        fuzzer_key = str(item.get("fuzzer_key", "")).strip()
        run_index = item.get("run_index")
        if not shard_key or not fuzzer_key:
            raise ValueError("shard entry missing shard_key/fuzzer_key")
        if not isinstance(run_index, int) or run_index < 0:
            raise ValueError(f"invalid run_index for shard {shard_key}")
        if shard_key in seen:
            raise ValueError(f"duplicate shard_key: {shard_key}")
        seen.add(shard_key)
        shards.append(
            {
                "shard_key": shard_key,
                "fuzzer_key": fuzzer_key,
                "run_index": run_index,
            }
        )
    return shards


def load_shard_states(bucket: str, shard_prefix: str, *, profile: str | None) -> list[dict[str, Any]]:
    shard_states: list[dict[str, Any]] = []
    for key in sorted(list_keys(bucket, shard_prefix, profile=profile)):
        if not key.endswith(".json"):
            continue
        shard = s3_json_or_none(bucket, key, profile=profile)
        if shard is None:
            continue
        shard_states.append(shard)
    return shard_states


def count_states(shards: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "queued": 0,
        "running": 0,
        "retrying": 0,
        "succeeded": 0,
        "failed": 0,
        "timed_out": 0,
        "total": len(shards),
    }
    for shard in shards:
        status = str(shard.get("status", "")).strip().lower()
        if status not in counts:
            continue
        counts[status] += 1
    return counts


def update_run_status(
    *,
    bucket: str,
    run_status_key: str,
    shard_prefix: str,
    run_id: str,
    benchmark_uuid: str,
    lock_owner: str,
    max_parallel_instances: int,
    shard_max_attempts: int,
    profile: str | None,
) -> dict[str, Any]:
    now = utc_now_iso()
    existing = s3_json_or_none(bucket, run_status_key, profile=profile) or {}
    shards = load_shard_states(bucket, shard_prefix, profile=profile)
    counts = count_states(shards)

    inflight = counts["queued"] + counts["running"] + counts["retrying"]
    terminal = inflight == 0 and counts["total"] > 0
    if terminal:
        state = "failed" if (counts["failed"] + counts["timed_out"]) > 0 else "succeeded"
    else:
        state = "running"

    payload: dict[str, Any] = {
        "mode": "s3_queue",
        "queue_mode": True,
        "run_id": run_id,
        "benchmark_uuid": benchmark_uuid,
        "state": state,
        "terminal": terminal,
        "counts": counts,
        "requested_shards": counts["total"],
        "max_parallel_instances": max_parallel_instances,
        "shard_max_attempts": shard_max_attempts,
        "lock_owner": lock_owner,
        "updated_at": now,
    }

    created_at = str(existing.get("created_at", "")).strip()
    payload["created_at"] = created_at or now

    if terminal:
        completed_at = str(existing.get("completed_at", "")).strip()
        payload["completed_at"] = completed_at or now
    elif "completed_at" in existing and existing["completed_at"]:
        payload["completed_at"] = existing["completed_at"]

    s3_put_json(bucket, run_status_key, payload, profile=profile)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize S3 queue objects for a benchmark run.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--benchmark-uuid", required=True)
    parser.add_argument("--shards-json-b64", required=True)
    parser.add_argument("--max-parallel-instances", type=int, required=True)
    parser.add_argument("--shard-max-attempts", type=int, default=3)
    parser.add_argument("--lock-owner", required=True)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    shards = parse_shards(args.shards_json_b64)
    if not shards:
        print("No shards were provided", file=sys.stderr)
        return 1

    root_prefix = f"runs/{args.run_id}/{args.benchmark_uuid}"
    shard_prefix = f"{root_prefix}/queue/shards/"
    run_status_key = f"{root_prefix}/status/run.json"
    event_prefix = f"{root_prefix}/status/events/"

    created = 0
    now = utc_now_iso()
    for shard in shards:
        shard_key = shard["shard_key"]
        shard_object_key = f"{shard_prefix}{shard_key}.json"
        if s3_exists(args.bucket, shard_object_key, profile=args.profile):
            continue

        payload = {
            "shard_key": shard_key,
            "fuzzer_key": shard["fuzzer_key"],
            "run_index": shard["run_index"],
            "status": "queued",
            "attempt": 0,
            "max_attempts": int(args.shard_max_attempts),
            "created_at": now,
            "updated_at": now,
            "last_worker_id": "",
            "last_exit_code": None,
            "retry_available_at_epoch": 0,
            "retry_available_at": "",
            "claim_token": "",
        }
        s3_put_json(args.bucket, shard_object_key, payload, profile=args.profile)

        event_key = (
            f"{event_prefix}{int(time.time() * 1000)}-bootstrap-"
            f"{sanitize_fragment(shard_key)}-queued.json"
        )
        event_payload = {
            "event_at": now,
            "event_type": "shard_status",
            "run_id": args.run_id,
            "benchmark_uuid": args.benchmark_uuid,
            "shard_key": shard_key,
            "status": "queued",
            "worker_id": "bootstrap",
            "attempt": 0,
        }
        s3_put_json(args.bucket, event_key, event_payload, profile=args.profile)
        created += 1

    run_status = update_run_status(
        bucket=args.bucket,
        run_status_key=run_status_key,
        shard_prefix=shard_prefix,
        run_id=args.run_id,
        benchmark_uuid=args.benchmark_uuid,
        lock_owner=args.lock_owner,
        max_parallel_instances=int(args.max_parallel_instances),
        shard_max_attempts=int(args.shard_max_attempts),
        profile=args.profile,
    )

    out = {
        "created_shards": created,
        "total_requested_shards": len(shards),
        "run_status": run_status,
    }
    print(json.dumps(out, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
