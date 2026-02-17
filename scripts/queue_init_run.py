#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

TERMINAL_SHARD_STATUSES = {"succeeded", "failed", "timed_out"}
TRACKED_SHARD_STATUSES = {"queued", "running", "retrying", *TERMINAL_SHARD_STATUSES}


class AwsError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def now_epoch() -> int:
    return int(time.time())


def aws(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(["aws", *args], text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise AwsError((proc.stderr or proc.stdout).strip() or f"aws {' '.join(args)} failed")
    return proc


def s3_get_json(bucket: str, key: str) -> tuple[dict | None, str | None]:
    with tempfile.NamedTemporaryFile("w+b") as tmp:
        proc = aws(
            [
                "s3api",
                "get-object",
                "--bucket",
                bucket,
                "--key",
                key,
                tmp.name,
                "--output",
                "json",
            ],
            check=False,
        )
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout).strip()
            if "NoSuchKey" in err or "Not Found" in err or "status code: 404" in err:
                return None, None
            raise AwsError(err or f"Failed to read s3://{bucket}/{key}")

        metadata = json.loads(proc.stdout or "{}")
        etag = str(metadata.get("ETag", "") or "").strip().strip('"')
        try:
            raw = Path(tmp.name).read_text(encoding="utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return None, etag
            return payload, etag or None
        except Exception:
            return None, etag or None


def s3_put_json(bucket: str, key: str, payload: dict) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8") as tmp:
        tmp.write(json.dumps(payload, separators=(",", ":")))
        tmp.flush()
        aws(
            [
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--content-type",
                "application/json",
                "--body",
                tmp.name,
            ]
        )


def safe_token(raw: object, fallback: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]", "-", str(raw or "").strip())
    token = token.strip("-")
    return token or fallback


def write_event(
    *,
    bucket: str,
    events_prefix: str,
    run_id: str,
    benchmark_uuid: str,
    shard_key: str,
    status: str,
    instance_id: str,
    attempt: int,
    message: str = "",
) -> str:
    ts_ms = int(time.time() * 1000)
    safe_instance = safe_token(instance_id, "unknown")
    safe_shard = safe_token(shard_key, "run")
    safe_status = safe_token(status, "event")
    key = f"{events_prefix}/{ts_ms}-{safe_instance}-{safe_shard}-{safe_status}.json"
    payload = {
        "ts_epoch_ms": ts_ms,
        "ts": now_iso(),
        "run_id": run_id,
        "benchmark_uuid": benchmark_uuid,
        "instance_id": instance_id,
        "shard_key": shard_key,
        "status": status,
        "attempt": int(attempt),
    }
    if message:
        payload["message"] = message
    s3_put_json(bucket, key, payload)
    return key


def normalize_shard(value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError("each shard entry must be an object")

    shard_key = str(value.get("shard_key", "")).strip()
    fuzzer_key = str(value.get("fuzzer_key", "")).strip()
    run_index_raw = value.get("run_index", "")

    if not shard_key or not re.match(r"^[a-z0-9][a-z0-9-]{0,63}$", shard_key):
        raise ValueError(f"invalid shard_key: {shard_key!r}")
    if not fuzzer_key or not re.match(r"^[a-z0-9][a-z0-9-]{0,63}$", fuzzer_key):
        raise ValueError(f"invalid fuzzer_key for shard {shard_key}: {fuzzer_key!r}")

    try:
        run_index = int(run_index_raw)
    except Exception as exc:
        raise ValueError(f"invalid run_index for shard {shard_key}: {run_index_raw!r}") from exc
    if run_index < 0:
        raise ValueError(f"run_index must be >= 0 for shard {shard_key}")

    return {
        "shard_key": shard_key,
        "fuzzer_key": fuzzer_key,
        "run_index": run_index,
    }


def shard_counts(shards: list[dict]) -> dict[str, int]:
    counts = {status: 0 for status in TRACKED_SHARD_STATUSES}
    counts["unknown"] = 0
    for shard in shards:
        status = str(shard.get("status", "")).strip().lower()
        if status in counts:
            counts[status] += 1
        else:
            counts["unknown"] += 1
    return counts


def run_status_from_counts(counts: dict[str, int]) -> str:
    active = counts.get("queued", 0) + counts.get("running", 0) + counts.get("retrying", 0)
    if active > 0:
        return "running"
    if counts.get("failed", 0) > 0 or counts.get("timed_out", 0) > 0:
        return "failed"
    return "completed"


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize S3-only queue state for a benchmark run.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--benchmark-uuid", required=True)
    parser.add_argument("--max-parallel-effective", required=True, type=int)
    parser.add_argument("--shard-max-attempts", type=int, default=5)
    parser.add_argument("--shards-json", required=True, type=Path)
    parser.add_argument("--summary-path", required=False, type=Path)
    args = parser.parse_args()

    if args.max_parallel_effective < 1:
        raise SystemExit("--max-parallel-effective must be >= 1")
    if args.shard_max_attempts < 1:
        raise SystemExit("--shard-max-attempts must be >= 1")

    raw_shards = json.loads(args.shards_json.read_text(encoding="utf-8"))
    if not isinstance(raw_shards, list) or not raw_shards:
        raise SystemExit("--shards-json must contain a non-empty JSON array")

    shards = [normalize_shard(entry) for entry in raw_shards]

    run_prefix = f"runs/{args.run_id}/{args.benchmark_uuid}"
    shard_prefix = f"{run_prefix}/queue/shards"
    events_prefix = f"{run_prefix}/status/events"
    run_status_key = f"{run_prefix}/status/run.json"

    preserved_existing = 0
    created_queued = 0
    initialized_shards: list[dict] = []

    for shard in shards:
        shard_key = shard["shard_key"]
        key = f"{shard_prefix}/{shard_key}.json"
        existing, _ = s3_get_json(args.bucket, key)
        if isinstance(existing, dict) and str(existing.get("status", "")).strip().lower() in TRACKED_SHARD_STATUSES:
            initialized_shards.append(existing)
            preserved_existing += 1
            continue

        created_at = now_iso()
        state = {
            "run_id": args.run_id,
            "benchmark_uuid": args.benchmark_uuid,
            "queue_backend": "s3",
            "shard_key": shard_key,
            "fuzzer_key": shard["fuzzer_key"],
            "run_index": shard["run_index"],
            "status": "queued",
            "attempt": 0,
            "max_attempts": args.shard_max_attempts,
            "created_at": created_at,
            "updated_at": created_at,
            "next_attempt_at": created_at,
        }
        s3_put_json(args.bucket, key, state)
        initialized_shards.append(state)
        created_queued += 1
        write_event(
            bucket=args.bucket,
            events_prefix=events_prefix,
            run_id=args.run_id,
            benchmark_uuid=args.benchmark_uuid,
            shard_key=shard_key,
            status="queued",
            instance_id="bootstrap",
            attempt=0,
            message="initialized by queue_init_run.py",
        )

    counts = shard_counts(initialized_shards)
    status = run_status_from_counts(counts)
    status_payload = {
        "run_id": args.run_id,
        "benchmark_uuid": args.benchmark_uuid,
        "queue_backend": "s3",
        "status": status,
        "requested_shards": len(shards),
        "max_parallel_instances": args.max_parallel_effective,
        "shard_max_attempts": args.shard_max_attempts,
        "queued_count": counts.get("queued", 0),
        "running_count": counts.get("running", 0),
        "retrying_count": counts.get("retrying", 0),
        "succeeded_count": counts.get("succeeded", 0),
        "failed_count": counts.get("failed", 0),
        "timed_out_count": counts.get("timed_out", 0),
        "unknown_count": counts.get("unknown", 0),
        "updated_at": now_iso(),
        "updated_epoch": now_epoch(),
    }
    if status in {"completed", "failed"}:
        status_payload["completed_at"] = now_iso()

    s3_put_json(args.bucket, run_status_key, status_payload)
    write_event(
        bucket=args.bucket,
        events_prefix=events_prefix,
        run_id=args.run_id,
        benchmark_uuid=args.benchmark_uuid,
        shard_key="run",
        status=status,
        instance_id="bootstrap",
        attempt=0,
        message="run status initialized",
    )

    summary = {
        "run_id": args.run_id,
        "benchmark_uuid": args.benchmark_uuid,
        "requested_shards": len(shards),
        "created_queued": created_queued,
        "preserved_existing": preserved_existing,
        "run_status": status,
        "counts": counts,
        "run_status_key": run_status_key,
    }

    if args.summary_path is not None:
        args.summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(summary, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
