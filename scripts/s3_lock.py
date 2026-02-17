#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any

DEFAULT_LOCK_KEY = "runs/_control/global-lock.json"


def aws_env(profile: str | None) -> dict[str, str]:
    env = os.environ.copy()
    if profile:
        env["AWS_PROFILE"] = profile
    return env


def aws_text(args: list[str], *, profile: str | None) -> str:
    return subprocess.check_output(["aws", *args], text=True, env=aws_env(profile))


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


def s3_delete(bucket: str, key: str, *, profile: str | None) -> None:
    subprocess.check_call(
        ["aws", "s3api", "delete-object", "--bucket", bucket, "--key", key],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=aws_env(profile),
    )


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def iso_utc(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp())


def lock_expired(lock: dict[str, Any] | None, *, now_epoch: int) -> bool:
    if not lock:
        return True
    expires_epoch = parse_epoch(lock.get("expires_at_epoch"))
    if expires_epoch is None:
        expires_epoch = parse_epoch(lock.get("expires_at"))
    if expires_epoch is None:
        return True
    return now_epoch >= expires_epoch


def build_payload(
    *,
    owner: str,
    run_id: str,
    benchmark_uuid: str,
    lease_seconds: int,
    previous_generation: int,
    actor: str,
) -> dict[str, Any]:
    now = utc_now()
    expires = now + dt.timedelta(seconds=max(lease_seconds, 1))
    return {
        "owner": owner,
        "run_id": run_id,
        "benchmark_uuid": benchmark_uuid,
        "lease_seconds": int(lease_seconds),
        "generation": int(previous_generation + 1),
        "token": uuid.uuid4().hex,
        "updated_by": actor,
        "acquired_at": iso_utc(now),
        "acquired_at_epoch": int(now.timestamp()),
        "expires_at": iso_utc(expires),
        "expires_at_epoch": int(expires.timestamp()),
    }


def cmd_read(args: argparse.Namespace) -> int:
    now_epoch = int(time.time())
    lock = s3_json_or_none(args.bucket, args.key, profile=args.profile)
    if not lock:
        print(json.dumps({"exists": False}, separators=(",", ":")))
        return 0
    out = {
        "exists": True,
        "expired": lock_expired(lock, now_epoch=now_epoch),
        "lock": lock,
    }
    print(json.dumps(out, separators=(",", ":")))
    return 0


def cmd_acquire(args: argparse.Namespace) -> int:
    timeout = int(args.acquire_timeout_seconds)
    poll = max(float(args.poll_seconds), 0.25)
    lease = max(int(args.lease_seconds), 1)
    started = time.time()
    actor = args.actor or f"pid-{os.getpid()}"

    while True:
        now_epoch = int(time.time())
        lock = s3_json_or_none(args.bucket, args.key, profile=args.profile)
        expired = lock_expired(lock, now_epoch=now_epoch)

        if lock and not expired:
            current_owner = str(lock.get("owner", ""))
            if current_owner != args.owner:
                if timeout > 0 and (time.time() - started) >= timeout:
                    print(
                        json.dumps(
                            {
                                "acquired": False,
                                "reason": "timeout_waiting_for_lock",
                                "current_owner": current_owner,
                            },
                            separators=(",", ":"),
                        )
                    )
                    return 1
                time.sleep(poll + random.uniform(0.0, poll * 0.25))
                continue

        previous_generation = 0
        if lock and isinstance(lock.get("generation"), (int, float)):
            previous_generation = int(lock["generation"])

        payload = build_payload(
            owner=args.owner,
            run_id=args.run_id,
            benchmark_uuid=args.benchmark_uuid,
            lease_seconds=lease,
            previous_generation=previous_generation,
            actor=actor,
        )
        s3_put_json(args.bucket, args.key, payload, profile=args.profile)

        # Re-read to verify ownership after write.
        time.sleep(0.6)
        confirmed = s3_json_or_none(args.bucket, args.key, profile=args.profile)
        if confirmed and str(confirmed.get("owner", "")) == args.owner and str(confirmed.get("token", "")) == payload["token"]:
            out = {
                "acquired": True,
                "lock": confirmed,
            }
            print(json.dumps(out, separators=(",", ":")))
            return 0

        if timeout > 0 and (time.time() - started) >= timeout:
            print(
                json.dumps(
                    {
                        "acquired": False,
                        "reason": "timeout_race_lost",
                    },
                    separators=(",", ":"),
                )
            )
            return 1

        time.sleep(poll + random.uniform(0.0, poll * 0.25))


def cmd_heartbeat(args: argparse.Namespace) -> int:
    lease = max(int(args.lease_seconds), 1)
    actor = args.actor or f"pid-{os.getpid()}"
    now_epoch = int(time.time())
    lock = s3_json_or_none(args.bucket, args.key, profile=args.profile)
    if not lock:
        print(json.dumps({"ok": False, "reason": "missing"}, separators=(",", ":")))
        return 2

    current_owner = str(lock.get("owner", ""))
    expired = lock_expired(lock, now_epoch=now_epoch)
    if current_owner != args.owner and not expired:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason": "owner_mismatch",
                    "current_owner": current_owner,
                },
                separators=(",", ":"),
            )
        )
        return 2

    if current_owner != args.owner:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason": "expired_or_stolen",
                    "current_owner": current_owner,
                },
                separators=(",", ":"),
            )
        )
        return 2

    previous_generation = 0
    if isinstance(lock.get("generation"), (int, float)):
        previous_generation = int(lock["generation"])

    payload = build_payload(
        owner=args.owner,
        run_id=args.run_id,
        benchmark_uuid=args.benchmark_uuid,
        lease_seconds=lease,
        previous_generation=previous_generation,
        actor=actor,
    )
    s3_put_json(args.bucket, args.key, payload, profile=args.profile)

    confirmed = s3_json_or_none(args.bucket, args.key, profile=args.profile)
    if not confirmed:
        print(json.dumps({"ok": False, "reason": "missing_after_write"}, separators=(",", ":")))
        return 2

    if str(confirmed.get("owner", "")) != args.owner:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason": "owner_mismatch_after_write",
                    "current_owner": str(confirmed.get("owner", "")),
                },
                separators=(",", ":"),
            )
        )
        return 2

    print(json.dumps({"ok": True, "lock": confirmed}, separators=(",", ":")))
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    now_epoch = int(time.time())
    lock = s3_json_or_none(args.bucket, args.key, profile=args.profile)
    if not lock:
        print(json.dumps({"released": False, "reason": "missing"}, separators=(",", ":")))
        return 0

    current_owner = str(lock.get("owner", ""))
    expired = lock_expired(lock, now_epoch=now_epoch)
    if current_owner != args.owner and not expired:
        print(
            json.dumps(
                {
                    "released": False,
                    "reason": "owner_mismatch",
                    "current_owner": current_owner,
                },
                separators=(",", ":"),
            )
        )
        return 1

    s3_delete(args.bucket, args.key, profile=args.profile)
    print(json.dumps({"released": True}, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="S3-backed global run lock helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bucket", required=True)
    common.add_argument("--key", default=DEFAULT_LOCK_KEY)
    common.add_argument("--profile", default=None)

    owner_common = argparse.ArgumentParser(add_help=False)
    owner_common.add_argument("--owner", required=True)
    owner_common.add_argument("--run-id", required=True)
    owner_common.add_argument("--benchmark-uuid", required=True)
    owner_common.add_argument("--lease-seconds", type=int, default=900)
    owner_common.add_argument("--actor", default="")

    acquire = sub.add_parser("acquire", parents=[common, owner_common], help="Acquire lock")
    acquire.add_argument("--acquire-timeout-seconds", type=int, default=0)
    acquire.add_argument("--poll-seconds", type=float, default=5.0)
    acquire.set_defaults(func=cmd_acquire)

    heartbeat = sub.add_parser("heartbeat", parents=[common, owner_common], help="Heartbeat lock")
    heartbeat.set_defaults(func=cmd_heartbeat)

    release = sub.add_parser("release", parents=[common], help="Release lock")
    release.add_argument("--owner", required=True)
    release.set_defaults(func=cmd_release)

    read = sub.add_parser("read", parents=[common], help="Read lock state")
    read.set_defaults(func=cmd_read)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
