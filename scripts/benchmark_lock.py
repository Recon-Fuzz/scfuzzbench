#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


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


def emit_output(key: str, value: str) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if out_path:
        with open(out_path, "a", encoding="utf-8") as fh:
            fh.write(f"{key}={value}\n")
    else:
        print(f"{key}={value}")


def safe_token(raw: object, fallback: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]", "-", str(raw or "").strip())
    token = token.strip("-")
    return token or fallback


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


def s3_get_json(bucket: str, key: str) -> dict | None:
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
                return None
            raise AwsError(err or f"Failed to read s3://{bucket}/{key}")
        try:
            return json.loads(Path(tmp.name).read_text(encoding="utf-8"))
        except Exception:
            return None


def s3_delete_object(bucket: str, key: str) -> None:
    aws(["s3api", "delete-object", "--bucket", bucket, "--key", key], check=False)


def s3_list_keys(bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix, "--output", "json"]
        if token:
            args.extend(["--continuation-token", token])
        payload = json.loads(aws(args).stdout or "{}")
        for item in payload.get("Contents", []):
            key = str(item.get("Key", "")).strip()
            if key:
                keys.append(key)
        if not payload.get("IsTruncated"):
            break
        token = payload.get("NextContinuationToken")
        if not token:
            break
    return keys


def lock_root(lock_name: str) -> str:
    return f"runs/control/locks/{lock_name}"


def lock_active_key(lock_name: str) -> str:
    return f"{lock_root(lock_name)}/active.json"


def lock_claim_prefix(lock_name: str) -> str:
    return f"{lock_root(lock_name)}/claims/"


def claim_epoch_from_key(key: str) -> int:
    match = re.search(r"/claims/(\d+)-", key)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def lock_owner(active: dict | None) -> str:
    if not isinstance(active, dict):
        return ""
    return str(active.get("owner_run_id", "") or "").strip()


def lock_expires_epoch(active: dict | None) -> int:
    if not isinstance(active, dict):
        return 0
    try:
        return int(active.get("expires_at_epoch", 0))
    except Exception:
        return 0


def is_lock_active(active: dict | None, now: int) -> bool:
    return bool(lock_owner(active)) and lock_expires_epoch(active) > now


def backoff_seconds(attempt: int, max_backoff_seconds: int) -> int:
    base = min(max_backoff_seconds, max(3, 2 ** min(attempt, 6)))
    return min(max_backoff_seconds, base + random.randint(0, 3))


def create_claim(
    *,
    bucket: str,
    lock_name: str,
    owner_run_id: str,
    workflow_run_id: str,
    lease_seconds: int,
) -> str:
    ts_ms = int(time.time() * 1000)
    owner_token = safe_token(owner_run_id, "run")
    nonce = f"{random.getrandbits(32):08x}"
    key = f"{lock_claim_prefix(lock_name)}{ts_ms}-{owner_token}-{nonce}.json"
    payload = {
        "lock_name": lock_name,
        "owner_run_id": owner_run_id,
        "workflow_run_id": workflow_run_id,
        "lease_seconds": lease_seconds,
        "created_at": now_iso(),
        "created_epoch": now_epoch(),
    }
    s3_put_json(bucket, key, payload)
    return key


def prune_stale_claims(bucket: str, claim_keys: list[str], stale_after_seconds: int) -> list[str]:
    now_ms = int(time.time() * 1000)
    keep: list[str] = []
    for key in sorted(claim_keys):
        claim_ms = claim_epoch_from_key(key)
        if claim_ms <= 0:
            keep.append(key)
            continue
        age_seconds = (now_ms - claim_ms) // 1000
        if age_seconds > stale_after_seconds:
            s3_delete_object(bucket, key)
            continue
        keep.append(key)
    return keep


def acquire_lock(
    *,
    bucket: str,
    lock_name: str,
    owner_run_id: str,
    workflow_run_id: str,
    lease_seconds: int,
    max_backoff_seconds: int,
    wait_timeout_seconds: int,
) -> None:
    if lease_seconds < 300:
        raise ValueError(f"Invalid lease_seconds: {lease_seconds} (expected >= 300)")
    if max_backoff_seconds < 5:
        raise ValueError(f"Invalid max_backoff_seconds: {max_backoff_seconds} (expected >= 5)")
    if wait_timeout_seconds < 0:
        raise ValueError(f"Invalid wait_timeout_seconds: {wait_timeout_seconds} (expected >= 0)")

    deadline = now_epoch() + wait_timeout_seconds if wait_timeout_seconds > 0 else None
    claim_key = create_claim(
        bucket=bucket,
        lock_name=lock_name,
        owner_run_id=owner_run_id,
        workflow_run_id=workflow_run_id,
        lease_seconds=lease_seconds,
    )

    active_key = lock_active_key(lock_name)
    stale_claim_after = max(lease_seconds * 2, 1200)
    attempts = 0

    while True:
        now = now_epoch()
        if deadline is not None and now >= deadline:
            raise TimeoutError(
                f"Timed out waiting for global lock after {wait_timeout_seconds}s. "
                "Set lock acquire timeout to 0 for unbounded wait."
            )

        active = s3_get_json(bucket, active_key)
        if is_lock_active(active, now) and lock_owner(active) != owner_run_id:
            holder = lock_owner(active) or "unknown"
            remaining = lock_expires_epoch(active) - now
            if remaining < 0:
                remaining = 0
            attempts += 1
            delay = backoff_seconds(attempts, max_backoff_seconds)
            print(
                f"Global lock busy (owner_run_id={holder}, remaining={remaining}s); "
                f"retrying in {delay}s (attempt {attempts}).",
                flush=True,
            )
            time.sleep(delay)
            continue

        claims = s3_list_keys(bucket, lock_claim_prefix(lock_name))
        claims = prune_stale_claims(bucket, claims, stale_claim_after)
        if claim_key not in claims:
            claim_key = create_claim(
                bucket=bucket,
                lock_name=lock_name,
                owner_run_id=owner_run_id,
                workflow_run_id=workflow_run_id,
                lease_seconds=lease_seconds,
            )
            claims.append(claim_key)

        claims.sort()
        if not claims or claims[0] != claim_key:
            attempts += 1
            delay = backoff_seconds(attempts, max_backoff_seconds)
            time.sleep(delay)
            continue

        acquired_at = now_iso()
        active_payload = {
            "lock_name": lock_name,
            "owner_run_id": owner_run_id,
            "workflow_run_id": workflow_run_id,
            "claim_key": claim_key,
            "acquired_at": acquired_at,
            "updated_at": acquired_at,
            "expires_at_epoch": now + lease_seconds,
            "lease_seconds": lease_seconds,
        }
        s3_put_json(bucket, active_key, active_payload)
        time.sleep(1)

        confirmed = s3_get_json(bucket, active_key)
        if (
            isinstance(confirmed, dict)
            and lock_owner(confirmed) == owner_run_id
            and str(confirmed.get("claim_key", "")) == claim_key
            and lock_expires_epoch(confirmed) > now_epoch()
        ):
            emit_output("lock_acquired", "true")
            emit_output("lock_lease_seconds", str(lease_seconds))
            emit_output("lock_attempts", str(max(1, attempts + 1)))
            emit_output("lock_object_key", active_key)
            return

        attempts += 1
        delay = backoff_seconds(attempts, max_backoff_seconds)
        time.sleep(delay)


def renew_lock(*, bucket: str, lock_name: str, owner_run_id: str, lease_seconds: int) -> None:
    if lease_seconds < 300:
        raise ValueError(f"Invalid lease_seconds: {lease_seconds} (expected >= 300)")

    active_key = lock_active_key(lock_name)
    active = s3_get_json(bucket, active_key)
    if not isinstance(active, dict):
        raise AwsError("lock active object not found")
    if lock_owner(active) != owner_run_id:
        raise AwsError("lock is held by a different owner")

    now = now_epoch()
    updated = dict(active)
    updated["updated_at"] = now_iso()
    updated["expires_at_epoch"] = now + lease_seconds
    updated["lease_seconds"] = lease_seconds

    s3_put_json(bucket, active_key, updated)
    confirmed = s3_get_json(bucket, active_key)
    if not isinstance(confirmed, dict) or lock_owner(confirmed) != owner_run_id:
        raise AwsError("lock renew verification failed")

    emit_output("lock_renewed", "true")
    emit_output("lock_lease_seconds", str(lease_seconds))


def release_lock(*, bucket: str, lock_name: str, owner_run_id: str) -> None:
    active_key = lock_active_key(lock_name)
    active = s3_get_json(bucket, active_key)
    released = False

    if isinstance(active, dict) and lock_owner(active) == owner_run_id:
        s3_delete_object(bucket, active_key)
        released = True

    owner_token = safe_token(owner_run_id, "run")
    for key in s3_list_keys(bucket, lock_claim_prefix(lock_name)):
        if f"-{owner_token}-" in key:
            s3_delete_object(bucket, key)

    emit_output("lock_released", "true" if released else "false")


def main() -> int:
    parser = argparse.ArgumentParser(description="Global benchmark lock helpers using S3 lease objects.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    acquire = sub.add_parser("acquire")
    acquire.add_argument("--bucket", required=True)
    acquire.add_argument("--lock-name", required=True)
    acquire.add_argument("--owner-run-id", required=True)
    acquire.add_argument("--workflow-run-id", required=True)
    acquire.add_argument("--lease-seconds", type=int, required=True)
    acquire.add_argument("--max-backoff-seconds", type=int, default=120)
    acquire.add_argument("--wait-timeout-seconds", type=int, default=0)

    renew = sub.add_parser("renew")
    renew.add_argument("--bucket", required=True)
    renew.add_argument("--lock-name", required=True)
    renew.add_argument("--owner-run-id", required=True)
    renew.add_argument("--lease-seconds", type=int, required=True)

    release = sub.add_parser("release")
    release.add_argument("--bucket", required=True)
    release.add_argument("--lock-name", required=True)
    release.add_argument("--owner-run-id", required=True)

    args = parser.parse_args()

    if args.cmd == "acquire":
        acquire_lock(
            bucket=args.bucket,
            lock_name=args.lock_name,
            owner_run_id=args.owner_run_id,
            workflow_run_id=args.workflow_run_id,
            lease_seconds=args.lease_seconds,
            max_backoff_seconds=args.max_backoff_seconds,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
        return 0

    if args.cmd == "renew":
        renew_lock(
            bucket=args.bucket,
            lock_name=args.lock_name,
            owner_run_id=args.owner_run_id,
            lease_seconds=args.lease_seconds,
        )
        return 0

    if args.cmd == "release":
        release_lock(
            bucket=args.bucket,
            lock_name=args.lock_name,
            owner_run_id=args.owner_run_id,
        )
        return 0

    raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
